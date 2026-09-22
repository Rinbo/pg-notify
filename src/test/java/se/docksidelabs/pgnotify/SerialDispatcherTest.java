package se.docksidelabs.pgnotify;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.Executor;
import org.junit.jupiter.api.Test;

/** Unit tests for the per-channel serialisation, driven by a hand-cranked executor. */
class SerialDispatcherTest {

  /** Collects submitted tasks and runs them only when told, so ordering is fully observable. */
  private static final class ManualExecutor implements Executor {
    final Deque<Runnable> pending = new ArrayDeque<>();

    @Override
    public void execute(Runnable r) {
      pending.add(r);
    }

    void runOne() {
      pending.poll().run();
    }

    void runAll() {
      while (!pending.isEmpty()) {
        runOne();
      }
    }
  }

  private final ManualExecutor executor = new ManualExecutor();
  private final SerialDispatcher dispatcher = new SerialDispatcher(executor);
  private final List<String> log = new ArrayList<>();

  private NotificationHandler recording(String tag) {
    return n -> log.add(tag + ":" + n.channel() + ":" + n.payload());
  }

  private static Notification n(String channel, String payload) {
    return new Notification(channel, payload, 0);
  }

  @Test
  void handsAtMostOneTaskPerChannelToTheExecutor() {
    NotificationHandler h = recording("h");
    dispatcher.dispatch(n("a", "1"), List.of(h));
    dispatcher.dispatch(n("a", "2"), List.of(h));
    dispatcher.dispatch(n("a", "3"), List.of(h));
    dispatcher.dispatch(n("b", "1"), List.of(h));
    dispatcher.dispatch(n("b", "2"), List.of(h));

    assertThat(executor.pending).hasSize(2); // one for a, one for b

    executor.runOne(); // a:1 completes and submits a:2
    assertThat(log).containsExactly("h:a:1");
    assertThat(executor.pending).hasSize(2);

    executor.runAll();
    assertThat(log)
        .containsExactly("h:a:1", "h:b:1", "h:a:2", "h:b:2", "h:a:3")
        .filteredOn(s -> s.startsWith("h:a"))
        .containsExactly("h:a:1", "h:a:2", "h:a:3");
  }

  @Test
  void runsHandlersOfOneNotificationInListOrder() {
    dispatcher.dispatch(n("a", "x"), List.of(recording("first"), recording("second")));
    executor.runAll();

    assertThat(log).containsExactly("first:a:x", "second:a:x");
  }

  @Test
  void isolatesExceptionsAndErrorsFromLaterHandlersAndNotifications() {
    NotificationHandler throwing =
        n -> {
          throw new IllegalStateException("boom " + n.payload());
        };
    NotificationHandler erroring =
        n -> {
          throw new AssertionError("err " + n.payload());
        };
    NotificationHandler ok = recording("ok");

    dispatcher.dispatch(n("a", "1"), List.of(throwing, ok));
    dispatcher.dispatch(n("a", "2"), List.of(erroring, ok));
    dispatcher.dispatch(n("a", "3"), List.of(ok));
    executor.runAll();

    assertThat(log).containsExactly("ok:a:1", "ok:a:2", "ok:a:3");
  }

  @Test
  void letsVirtualMachineErrorsPropagateButKeepsTheChannelMoving() {
    NotificationHandler fatal =
        n -> {
          throw new OutOfMemoryError("simulated");
        };
    dispatcher.dispatch(n("a", "1"), List.of(fatal));
    dispatcher.dispatch(n("a", "2"), List.of(recording("ok")));

    assertThatThrownBy(executor::runOne).isInstanceOf(OutOfMemoryError.class);
    executor.runAll();

    assertThat(log).containsExactly("ok:a:2");
  }
}
