package se.docksidelabs.pgnotify;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class HandlerExecutorTest {

  @Test
  void ownedExecutorRunsOnANamedDaemonThreadItRecognises() throws Exception {
    HandlerExecutor executor = HandlerExecutor.owned("test-handler");
    CompletableFuture<String> name = new CompletableFuture<>();
    CompletableFuture<Boolean> daemon = new CompletableFuture<>();
    CompletableFuture<Boolean> recognised = new CompletableFuture<>();

    executor.execute(
        () -> {
          name.complete(Thread.currentThread().getName());
          daemon.complete(Thread.currentThread().isDaemon());
          recognised.complete(executor.onHandlerThread());
        });

    assertThat(name.get(5, TimeUnit.SECONDS)).isEqualTo("test-handler");
    assertThat(daemon.get(5, TimeUnit.SECONDS)).isTrue();
    assertThat(recognised.get(5, TimeUnit.SECONDS)).isTrue();
    assertThat(executor.onHandlerThread()).as("from the test thread").isFalse();
    executor.shutdown(Duration.ofSeconds(1));
  }

  @Test
  void ownedExecutorsDoNotRecogniseEachOthersThreads() throws Exception {
    HandlerExecutor a = HandlerExecutor.owned("a");
    HandlerExecutor b = HandlerExecutor.owned("b");
    CompletableFuture<Boolean> bSeenFromA = new CompletableFuture<>();

    a.execute(() -> bSeenFromA.complete(b.onHandlerThread()));

    assertThat(bSeenFromA.get(5, TimeUnit.SECONDS)).isFalse();
    a.shutdown(Duration.ofSeconds(1));
    b.shutdown(Duration.ofSeconds(1));
  }

  @Test
  void shutdownLetsQueuedWorkFinishWithinTheGracePeriod() throws Exception {
    HandlerExecutor executor = HandlerExecutor.owned("graceful");
    CountDownLatch finished = new CountDownLatch(2);
    Runnable slow =
        () -> {
          try {
            Thread.sleep(100);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          finished.countDown();
        };
    executor.execute(slow);
    executor.execute(slow);

    executor.shutdown(Duration.ofSeconds(5));

    assertThat(finished.getCount()).isZero();
  }

  @Test
  void shutdownInterruptsWorkThatOutlivesTheGracePeriod() throws Exception {
    HandlerExecutor executor = HandlerExecutor.owned("stubborn");
    CompletableFuture<Boolean> interrupted = new CompletableFuture<>();
    executor.execute(
        () -> {
          try {
            Thread.sleep(10_000);
            interrupted.complete(false);
          } catch (InterruptedException e) {
            interrupted.complete(true);
          }
        });

    executor.shutdown(Duration.ofMillis(100));

    assertThat(interrupted.get(5, TimeUnit.SECONDS)).isTrue();
  }

  @Test
  void shutdownFromItsOwnThreadReturnsWithoutWaiting() throws Exception {
    HandlerExecutor executor = HandlerExecutor.owned("self-closing");
    CompletableFuture<Long> elapsedMillis = new CompletableFuture<>();

    executor.execute(
        () -> {
          long start = System.nanoTime();
          executor.shutdown(Duration.ofSeconds(10));
          elapsedMillis.complete((System.nanoTime() - start) / 1_000_000);
        });

    assertThat(elapsedMillis.get(5, TimeUnit.SECONDS)).isLessThan(1_000);
  }

  @Test
  void suppliedExecutorIsNeverShutDown() {
    ExecutorService pool = Executors.newSingleThreadExecutor();
    try {
      HandlerExecutor executor = HandlerExecutor.supplied(pool);

      executor.shutdown(Duration.ofSeconds(1));

      assertThat(pool.isShutdown()).isFalse();
      assertThat(executor.onHandlerThread()).isFalse();
    } finally {
      pool.shutdownNow();
    }
  }
}
