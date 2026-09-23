package se.docksidelabs.pgnotify;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class ReconnectPolicyTest {

  private final ReconnectPolicy policy =
      new ReconnectPolicy(Duration.ofMillis(100), Duration.ofSeconds(5), new Random(42));

  @Test
  void capDoublesUntilTheMaximum() {
    assertThat(policy.cap(1)).isEqualTo(100);
    assertThat(policy.cap(2)).isEqualTo(200);
    assertThat(policy.cap(3)).isEqualTo(400);
    assertThat(policy.cap(6)).isEqualTo(3_200);
    assertThat(policy.cap(7)).isEqualTo(5_000);
    assertThat(policy.cap(100)).isEqualTo(5_000);
    assertThat(policy.cap(Integer.MAX_VALUE)).as("no overflow").isEqualTo(5_000);
  }

  @Test
  void separateJvmsDrawDifferentDelays() throws Exception {
    String first = runJitterProbe();
    String second = runJitterProbe();

    assertThat(first).matches("\\d+( \\d+){7}");
    assertThat(second).as("jitter must not repeat across JVMs").isNotEqualTo(first);
  }

  @Test
  void delayIsUniformlyWithinZeroAndTheCap() {
    for (int failures = 1; failures <= 12; failures++) {
      long cap = policy.cap(failures);
      long min = Long.MAX_VALUE;
      long max = Long.MIN_VALUE;
      for (int i = 0; i < 500; i++) {
        long d = policy.delay(failures).toMillis();
        assertThat(d).isBetween(0L, cap);
        min = Math.min(min, d);
        max = Math.max(max, d);
      }
      assertThat(max - min).as("spread for %d failures", failures).isGreaterThan(cap / 2);
    }
  }

  @Test
  void rejectsZeroFailures() {
    assertThatIllegalArgumentException().isThrownBy(() -> policy.delay(0));
  }

  private static String runJitterProbe() throws Exception {
    Process process =
        new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp",
                System.getProperty("java.class.path"),
                JitterProbe.class.getName())
            .redirectErrorStream(true)
            .start();
    String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    assertThat(process.waitFor(30, TimeUnit.SECONDS)).as("probe finished").isTrue();
    assertThat(process.exitValue()).as(output).isZero();
    return output.trim();
  }
}
