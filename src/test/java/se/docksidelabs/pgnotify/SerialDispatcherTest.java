package se.docksidelabs.pgnotify;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
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

  @Test
  void retiresAChannelQueueOnceItIsDrained() {
    NotificationHandler h = recording("h");
    dispatcher.dispatch(n("a", "1"), List.of(h));
    dispatcher.dispatch(n("a", "2"), List.of(h));
    assertThat(dispatcher.activeChannels()).isEqualTo(1);

    executor.runOne();
    assertThat(dispatcher.activeChannels()).as("still one task queued").isEqualTo(1);
    executor.runAll();
    assertThat(dispatcher.activeChannels()).as("nothing left for the channel").isZero();

    dispatcher.dispatch(n("a", "3"), List.of(h));
    executor.runAll();
    assertThat(log).containsExactly("h:a:1", "h:a:2", "h:a:3");
    assertThat(dispatcher.activeChannels()).isZero();
  }

  @Test
  void awaitIdleReturnsOnceEveryChannelHasDrained() throws Exception {
    dispatcher.dispatch(n("a", "1"), List.of(recording("h")));
    dispatcher.dispatch(n("a", "2"), List.of(recording("h")));
    dispatcher.dispatch(n("b", "1"), List.of(recording("h")));
    assertThat(dispatcher.awaitIdle(Duration.ofMillis(50))).as("work still queued").isFalse();

    Thread cranker = new Thread(executor::runAll, "cranker");
    cranker.start();

    assertThat(dispatcher.awaitIdle(Duration.ofSeconds(5))).isTrue();
    cranker.join(5_000);
    assertThat(log).hasSize(3);
    assertThat(new SerialDispatcher(executor).awaitIdle(Duration.ZERO)).as("never used").isTrue();
  }

  @Test
  void dropsTheChannelQueueWhenTheExecutorRejects() {
    Executor rejecting =
        r -> {
          throw new RejectedExecutionException("full");
        };
    SerialDispatcher d = new SerialDispatcher(rejecting);
    d.dispatch(n("a", "1"), List.of(recording("h")));

    assertThat(d.activeChannels()).isZero();
    assertThat(log).isEmpty();
  }

  @Test
  void keepsTheInterruptAndSkipsRemainingHandlers() {
    NotificationHandler interrupted =
        n -> {
          throw new InterruptedException("shutting down");
        };
    dispatcher.dispatch(n("a", "1"), List.of(interrupted, recording("after")));
    dispatcher.dispatch(n("a", "2"), List.of(recording("next")));

    try {
      executor.runOne();
      assertThat(Thread.currentThread().isInterrupted()).as("flag restored").isTrue();
      assertThat(log).as("later handlers of the same notification skipped").isEmpty();
    } finally {
      assertThat(Thread.interrupted()).isTrue(); // clears the flag for the rest of the test
    }
    executor.runAll();
    assertThat(log).containsExactly("next:a:2");
  }

  /**
   * An executor whose {@code execute} blocks while its queue is full must not deadlock against a
   * handler thread that needs the channel lock to hand in the next task.
   */
  @Test
  void doesNotDeadlockOnAnExecutorThatBlocksInExecute() throws Exception {
    BlockingQueue<Runnable> queue = new ArrayBlockingQueue<>(1);
    Executor blocking =
        r -> {
          try {
            queue.put(r);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RejectedExecutionException(e);
          }
        };
    Thread worker =
        new Thread(
            () -> {
              try {
                while (true) {
                  queue.take().run();
                }
              } catch (InterruptedException e) {
                // done
              }
            },
            "worker");
    worker.setDaemon(true);
    worker.start();
    SerialDispatcher d = new SerialDispatcher(blocking);
    int count = 200;
    CountDownLatch done = new CountDownLatch(count);
    NotificationHandler h = n -> done.countDown();

    Thread producer =
        new Thread(
            () -> {
              for (int i = 0; i < count; i++) {
                d.dispatch(n("a", Integer.toString(i)), List.of(h));
              }
            },
            "producer");
    producer.start();

    assertThat(done.await(10, TimeUnit.SECONDS)).as("all notifications handled").isTrue();
    producer.join(5_000);
    assertThat(producer.isAlive()).as("dispatching thread returned").isFalse();
    worker.interrupt();
  }
}
