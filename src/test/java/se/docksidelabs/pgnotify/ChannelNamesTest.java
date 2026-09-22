package se.docksidelabs.pgnotify;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ChannelNamesTest {

  @ParameterizedTest
  @ValueSource(
      strings = {
        "a",
        "cache_invalidation",
        "MixedCase",
        "with-dash.and:colon",
        "has space",
        "has\"quote",
        "unicode_åäö",
        "1starts_with_digit",
        "semicolon;drop table x",
      })
  void acceptsAnythingPrintableWithinLimit(String name) {
    assertThat(ChannelNames.validate(name)).isSameAs(name);
  }

  @Test
  void acceptsExactly63Bytes() {
    String name = "x".repeat(63);
    assertThat(ChannelNames.validate(name)).isSameAs(name);
  }

  @Test
  void lengthIsMeasuredInUtf8BytesNotChars() {
    String twentyOneChars = "å".repeat(21); // 42 bytes
    assertThat(ChannelNames.validate(twentyOneChars)).isSameAs(twentyOneChars);
    String thirtyTwoChars = "å".repeat(32); // 64 bytes
    assertThatIllegalArgumentException()
        .isThrownBy(() -> ChannelNames.validate(thirtyTwoChars))
        .withMessageContaining("64 bytes");
  }

  @Test
  void quotesAndDoublesEmbeddedQuotes() {
    assertThat(ChannelNames.quote("plain")).isEqualTo("\"plain\"");
    assertThat(ChannelNames.quote("MixedCase")).isEqualTo("\"MixedCase\"");
    assertThat(ChannelNames.quote("a\"b")).isEqualTo("\"a\"\"b\"");
    assertThat(ChannelNames.quote("\"")).isEqualTo("\"\"\"\"");
    assertThat(ChannelNames.quote("x\"; DROP TABLE t; --"))
        .isEqualTo("\"x\"\"; DROP TABLE t; --\"");
  }

  @Test
  void rejects64Bytes() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> ChannelNames.validate("x".repeat(64)))
        .withMessageContaining("64 bytes");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"nul\u0000byte", "new\nline", "tab\tchar", "del\u007fchar", "esc\u001b[0m"})
  void rejectsControlCharacters(String name) {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> ChannelNames.validate(name))
        .withMessageContaining("control");
  }

  @Test
  void rejectsEmpty() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> ChannelNames.validate(""))
        .withMessageContaining("empty");
  }

  @Test
  void rejectsNull() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> ChannelNames.validate(null))
        .withMessageContaining("null");
  }

  @Test
  void rejectsUnpairedSurrogate() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> ChannelNames.validate("bad\uD800end"))
        .withMessageContaining("Unicode");
  }
}
