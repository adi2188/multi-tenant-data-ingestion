package io.atlan.ingestion;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.json.JsonWriteFeature;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.cfg.JsonNodeFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.*;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.regex.Pattern;

final class Json {
  static final ObjectMapper MAPPER =
      JsonMapper.builder()
          .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
          .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
          .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
          // Match stored UTF-8 byte budgets instead of escaping each surrogate.
          .enable(JsonWriteFeature.COMBINE_UNICODE_SURROGATES_IN_UTF8)
          .disable(JsonNodeFeature.STRIP_TRAILING_BIGDECIMAL_ZEROES)
          .build();
  static final Pattern ID = Pattern.compile("[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}");

  static {
    MAPPER
        .getFactory()
        .setStreamReadConstraints(
            StreamReadConstraints.builder()
                .maxNestingDepth(32)
                .maxStringLength(2 * 1024 * 1024)
                .maxNumberLength(1000)
                .build());
  }

  static JsonNode parse(byte[] bytes) {
    try {
      String text =
          StandardCharsets.UTF_8
              .newDecoder()
              .onMalformedInput(CodingErrorAction.REPORT)
              .onUnmappableCharacter(CodingErrorAction.REPORT)
              .decode(ByteBuffer.wrap(bytes))
              .toString();
      JsonNode node = MAPPER.readTree(text);
      if (node == null) throw new IOException("empty JSON");
      // BigDecimal preserves valid JSON numbers beyond double's range. Numeric
      // representability belongs to schema validation of each record, not admission.
      return node;
    } catch (IOException | IllegalArgumentException ex) {
      throw new ApiException(
          400,
          "invalid_json",
          "Expected strict UTF-8 JSON without duplicate keys or non-JSON numeric literals");
    }
  }

  static ObjectNode object() {
    return MAPPER.createObjectNode();
  }

  static JsonNode canonical(JsonNode node) {
    if (node.isObject()) {
      var result = object();
      var names = new TreeSet<String>();
      node.fieldNames().forEachRemaining(names::add);
      for (String name : names) result.set(name, canonical(node.get(name)));
      return result;
    }
    if (node.isArray()) {
      var result = MAPPER.createArrayNode();
      node.forEach(n -> result.add(canonical(n)));
      return result;
    }
    return node;
  }

  static String fingerprint(JsonNode node) {
    try {
      var digest = MessageDigest.getInstance("SHA-256");
      hashNode(digest, node);
      return HexFormat.of().formatHex(digest.digest());
    } catch (NoSuchAlgorithmException ex) {
      throw new AssertionError("JDK must provide SHA-256", ex);
    }
  }

  private static void hashNode(MessageDigest digest, JsonNode node) {
    if (node.isObject()) {
      digest.update((byte) '{');
      digest.update(ByteBuffer.allocate(4).putInt(node.size()).array());
      var keys = new TreeSet<String>();
      node.fieldNames().forEachRemaining(keys::add);
      for (String key : keys) {
        hashString(digest, key);
        hashNode(digest, node.get(key));
      }
    } else if (node.isArray()) {
      digest.update((byte) '[');
      digest.update(ByteBuffer.allocate(4).putInt(node.size()).array());
      node.forEach(value -> hashNode(digest, value));
    } else if (node.isIntegralNumber()) {
      digest.update((byte) 'i');
      hashString(digest, node.bigIntegerValue().toString());
    } else if (node.isFloatingPointNumber()) {
      // JSON reserialization alone turns 1e0 into 1, which would lose token kind.
      digest.update((byte) 'd');
      hashString(digest, decimalIdentity(node.decimalValue()));
    } else if (node.isTextual()) {
      digest.update((byte) 's');
      hashString(digest, node.textValue());
    } else {
      digest.update((byte) (node.isNull() ? 'n' : node.booleanValue() ? 't' : 'f'));
    }
  }

  private static void hashString(MessageDigest digest, String value) {
    if (!validUnicode(value)) {
      // A negative length cannot collide with normal UTF-8. Preserve malformed
      // UTF-16 exactly, even in invalid records and unknown field names.
      digest.update(ByteBuffer.allocate(4).putInt(-value.length() - 1).array());
      for (int i = 0; i < value.length(); i++) {
        digest.update((byte) (value.charAt(i) >>> 8));
        digest.update((byte) value.charAt(i));
      }
      return;
    }
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    digest.update(ByteBuffer.allocate(4).putInt(bytes.length).array());
    digest.update(bytes);
  }

  private static String decimalIdentity(BigDecimal value) {
    try {
      return value.stripTrailingZeros().toString();
    } catch (ArithmeticException overflow) {
      // Trailing-zero removal can underflow BigDecimal's int scale even though
      // the original JSON number parsed successfully. Normalize using a long exponent.
      String digits = value.unscaledValue().abs().toString();
      int end = digits.length();
      while (end > 1 && digits.charAt(end - 1) == '0') end--;
      long exponent = (long) digits.length() - 1 - value.scale();
      return (value.signum() < 0 ? "-" : "")
          + digits.charAt(0)
          + (end > 1 ? "." + digits.substring(1, end) : "")
          + "E+"
          + exponent;
    }
  }

  static boolean validUnicode(String value) {
    for (int i = 0; i < value.length(); i++) {
      char ch = value.charAt(i);
      if (Character.isHighSurrogate(ch)) {
        if (++i == value.length() || !Character.isLowSurrogate(value.charAt(i))) return false;
      } else if (Character.isLowSurrogate(ch)) return false;
    }
    return true;
  }

  static boolean validId(JsonNode n) {
    return n != null && n.isTextual() && ID.matcher(n.textValue()).matches();
  }

  static String id(JsonNode object, String key) {
    if (!validId(object.get(key)))
      throw new ApiException(400, "invalid_identifier", key + " must match " + ID.pattern());
    return object.get(key).textValue();
  }

  static int positiveInt(JsonNode n, int status, String name) {
    if (n == null || !n.isIntegralNumber() || !n.canConvertToInt() || n.intValue() < 1)
      throw new ApiException(
          status, "invalid_version", name + " must be a positive 32-bit integer");
    return n.intValue();
  }

  static void keys(JsonNode n, Set<String> expected, int status) {
    if (!n.isObject())
      throw new ApiException(status, "invalid_structure", "Expected a JSON object");
    var actual = new HashSet<String>();
    n.fieldNames().forEachRemaining(actual::add);
    if (!actual.equals(expected))
      throw new ApiException(
          status, "invalid_structure", "Expected exactly these fields: " + new TreeSet<>(expected));
  }
}
