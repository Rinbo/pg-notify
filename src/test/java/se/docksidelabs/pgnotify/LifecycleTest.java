package se.docksidelabs.pgnotify;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import se.docksidelabs.pgnotify.PgListener.State;

class LifecycleTest {

  private final Lifecycle lifecycle = new Lifecycle();

  @Test
  void startsNew() {
    assertThat(lifecycle.state()).isEqualTo(State.NEW);
    assertThat(lifecycle.isClosed()).isFalse();
  }

  @Test
  void transitionRequiresTheExpectedSourceState() {
    assertThat(lifecycle.transition(State.LISTENING, State.CONNECTING)).isFalse();
    assertThat(lifecycle.state()).isEqualTo(State.NEW);

    assertThat(lifecycle.transition(State.NEW, State.CONNECTING)).isTrue();
    assertThat(lifecycle.state()).isEqualTo(State.CONNECTING);
  }

  @Test
  void closedIsTerminalAndCloseReportsOnlyTheFirstCall() {
    assertThat(lifecycle.close()).isTrue();
    assertThat(lifecycle.close()).isFalse();
    assertThat(lifecycle.isClosed()).isTrue();

    assertThat(lifecycle.moveTo(State.LISTENING)).isFalse();
    assertThat(lifecycle.transition(State.CLOSED, State.NEW)).isFalse();
    assertThat(lifecycle.state()).isEqualTo(State.CLOSED);
  }

  @Test
  void awaitReturnsWhenTheTargetStateIsReached() throws Exception {
    CompletableFuture<Boolean> waiter =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                return lifecycle.await(State.LISTENING, Duration.ofSeconds(5));
              } catch (InterruptedException e) {
                throw new IllegalStateException(e);
              }
            });

    Thread.sleep(50);
    assertThat(waiter).isNotDone();
    lifecycle.moveTo(State.CONNECTING);
    Thread.sleep(50);
    assertThat(waiter).isNotDone();
    lifecycle.moveTo(State.LISTENING);

    assertThat(waiter.get(5, TimeUnit.SECONDS)).isTrue();
  }

  @Test
  void awaitTimesOut() throws Exception {
    assertThat(lifecycle.await(State.LISTENING, Duration.ofMillis(50))).isFalse();
  }

  @Test
  void awaitReleasesOnCloseWithFalse() throws Exception {
    CompletableFuture<Boolean> waiter =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                return lifecycle.await(State.LISTENING, Duration.ofSeconds(5));
              } catch (InterruptedException e) {
                throw new IllegalStateException(e);
              }
            });
    Thread.sleep(50);

    lifecycle.close();

    assertThat(waiter.get(5, TimeUnit.SECONDS)).isFalse();
  }
}
