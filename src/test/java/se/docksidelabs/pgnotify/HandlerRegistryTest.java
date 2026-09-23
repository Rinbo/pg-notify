package se.docksidelabs.pgnotify;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.Test;

class HandlerRegistryTest {

  private final HandlerRegistry registry = new HandlerRegistry();
  private final NotificationHandler h1 = n -> {};
  private final NotificationHandler h2 = n -> {};

  @Test
  void reportsFirstHandlerOnAChannel() {
    assertThat(registry.add("a", h1)).isTrue();
    assertThat(registry.add("a", h2)).isFalse();
    assertThat(registry.add("b", h1)).isTrue();
    assertThat(registry.channelCount()).isEqualTo(2);
  }

  @Test
  void keepsRegistrationOrderPerChannel() {
    registry.add("a", h2);
    registry.add("a", h1);

    assertThat(registry.handlersFor("a")).containsExactly(h2, h1);
    assertThat(registry.handlersFor("missing")).isEmpty();
  }

  @Test
  void removeReportsWhenAChannelBecomesEmpty() {
    registry.add("a", h1);
    registry.add("a", h2);

    assertThat(registry.remove("a", h1)).isFalse();
    assertThat(registry.remove("a", h1)).as("already removed").isFalse();
    assertThat(registry.remove("a", h2)).isTrue();
    assertThat(registry.channels()).isEmpty();
    assertThat(registry.remove("a", h2)).as("unknown channel").isFalse();
  }

  @Test
  void listsChannelsInFirstRegistrationOrder() {
    registry.add("c", h1);
    registry.add("a", h1);
    registry.add("b", h1);
    registry.add("c", h2);

    assertThat(registry.channels()).containsExactly("c", "a", "b");
  }

  @Test
  void handsOutSnapshots() {
    registry.add("a", h1);
    List<NotificationHandler> snapshot = registry.handlersFor("a");
    registry.add("a", h2);

    assertThat(snapshot).containsExactly(h1);
    assertThatThrownBy(() -> snapshot.add(h2)).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> registry.channels().add("x"))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void validatesChannelNames() {
    assertThatIllegalArgumentException().isThrownBy(() -> registry.add("", h1));
    assertThatIllegalArgumentException().isThrownBy(() -> registry.add("x".repeat(64), h1));
    assertThatThrownBy(() -> registry.add("a", null)).isInstanceOf(NullPointerException.class);
    assertThat(registry.channelCount()).isZero();
  }
}
