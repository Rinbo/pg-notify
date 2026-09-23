package se.docksidelabs.pgnotify;

import java.time.Duration;

/**
 * Prints the backoff delays a listener thread draws. {@link ReconnectPolicyTest} runs it in
 * separate JVMs: identical output would mean every instance of a fleet reconnects in lockstep.
 */
final class JitterProbe {

  private JitterProbe() {}

  public static void main(String[] args) throws InterruptedException {
    // Built on this thread and used on a fresh one, as PgListener.start() and the listener do.
    ReconnectPolicy policy = new ReconnectPolicy(Duration.ofMillis(500), Duration.ofSeconds(30));
    StringBuilder out = new StringBuilder();
    Thread listenerThread =
        new Thread(
            () -> {
              for (int i = 0; i < 8; i++) {
                out.append(policy.delay(7).toMillis()).append(' ');
              }
            });
    listenerThread.start();
    listenerThread.join();
    System.out.println(out.toString().trim());
  }
}
