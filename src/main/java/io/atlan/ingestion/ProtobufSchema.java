package io.atlan.ingestion;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.*;
import com.google.protobuf.DescriptorProtos.*;
import com.google.protobuf.Descriptors.*;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.regex.Pattern;

/** Real protobuf descriptors define types, field numbers and requiredness. */
final class ProtobufSchema {
  private static final Pattern TIMESTAMP =
      Pattern.compile(
          "[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\\.[0-9]{1,9})?(?:Z|[+-][0-9]{2}:[0-9]{2})");
  final Descriptor descriptor;
  final FileDescriptorProto normalizedRoot;
  final ObjectNode registration;
  final String customer, dataset;
  final int version;

  private ProtobufSchema(
      Descriptor descriptor, FileDescriptorProto normalizedRoot, ObjectNode registration) {
    this.descriptor = descriptor;
    this.normalizedRoot = normalizedRoot;
    this.registration = registration;
    customer = registration.path("customer_id").asText();
    dataset = registration.path("dataset_id").asText();
    version = registration.path("version").asInt();
  }

  static ProtobufSchema parse(JsonNode body) {
    Json.keys(
        body,
        Set.of("customer_id", "dataset_id", "version", "message_type", "descriptor_set"),
        422);
    Json.id(body, "customer_id");
    Json.id(body, "dataset_id");
    Json.positiveInt(body.get("version"), 422, "version");
    if (!body.path("message_type").isTextual() || !body.path("descriptor_set").isTextual())
      throw invalid("message_type and descriptor_set must be strings");
    try {
      byte[] bytes = Base64.getDecoder().decode(body.get("descriptor_set").textValue());
      if (bytes.length > 256 * 1024) throw invalid("descriptor_set exceeds 256 KiB");
      var set = FileDescriptorSet.parseFrom(bytes);
      if (!set.getUnknownFields().isEmpty() || set.getFileCount() < 1 || set.getFileCount() > 2)
        throw invalid("Expected one dataset .proto and optional timestamp import");
      FileDescriptorProto root = null;
      var names = new HashSet<String>();
      for (var file : set.getFileList()) {
        if (!file.getUnknownFields().isEmpty() || file.getName().isEmpty())
          throw invalid("Malformed descriptor file entry");
        if (!names.add(file.getName())) throw invalid("Duplicate descriptor file");
        if (file.getName().equals("google/protobuf/timestamp.proto")) {
          // Ignore compiler source locations, but never accept a redefined well-known type.
          if (!file.toBuilder()
              .clearSourceCodeInfo()
              .build()
              .equals(
                  TimestampProto.getDescriptor().toProto().toBuilder()
                      .clearSourceCodeInfo()
                      .build()))
            throw invalid("The Timestamp descriptor must be the standard protobuf definition");
        } else {
          if (root != null) throw invalid("Only one dataset .proto is supported");
          root = file;
        }
      }
      if (root == null
          || root.hasEdition()
          || !(root.getSyntax().isEmpty() || root.getSyntax().equals("proto2")))
        throw invalid("Use proto2 required/optional fields");
      rejectUnknownFields(root);
      if (root.getOptions().hasFeatures()) throw invalid("Editions features are unsupported");
      if (root.getPackage().equals("google.protobuf")
          || root.getPackage().startsWith("google.protobuf."))
        throw invalid("The google.protobuf namespace is reserved");
      if (root.getMessageTypeCount() != 1
          || root.getEnumTypeCount() != 0
          || root.getServiceCount() != 0
          || root.getExtensionCount() != 0
          || root.getPublicDependencyCount() != 0
          || root.getWeakDependencyCount() != 0) throw invalid("Expected exactly one flat message");
      if (root.getDependencyList().stream()
              .anyMatch(n -> !n.equals("google/protobuf/timestamp.proto"))
          || root.getDependencyCount() > 1
          || (set.getFileCount() == 2 && root.getDependencyCount() == 0))
        throw invalid("Only google/protobuf/timestamp.proto may be imported");
      var dependencies =
          root.getDependencyCount() == 0
              ? new FileDescriptor[0]
              : new FileDescriptor[] {TimestampProto.getDescriptor()};
      var file = FileDescriptor.buildFrom(root, dependencies);
      Descriptor message = file.getMessageTypes().getFirst();
      if (!message.getFullName().equals(body.get("message_type").textValue()))
        throw invalid("message_type does not identify the root message");
      var proto = message.toProto();
      if (proto.getOptions().hasFeatures()) throw invalid("Editions features are unsupported");
      if (!proto.getUnknownFields().isEmpty()
          || message.getFields().size() > 100
          || proto.getNestedTypeCount() != 0
          || proto.getEnumTypeCount() != 0
          || proto.getExtensionCount() != 0
          || proto.getExtensionRangeCount() != 0
          || proto.getOneofDeclCount() != 0)
        throw invalid("Use at most 100 flat, singular fields without enums, extensions or oneofs");
      for (var range : proto.getReservedRangeList()) {
        if (!range.getUnknownFields().isEmpty()
            || range.getStart() < 1
            || range.getEnd() <= range.getStart()
            || range.getEnd() > 536870912) throw invalid("Illegal reserved field range");
      }
      for (String reservedName : proto.getReservedNameList()) {
        if (!Json.ID.matcher(reservedName).matches())
          throw invalid("Illegal reserved field name: " + reservedName);
      }
      for (var field : message.getFields()) {
        var fieldProto = field.toProto();
        if (fieldProto.getOptions().hasFeatures() || fieldProto.getProto3Optional())
          throw invalid("Use proto2 presence without Editions features");
        int number = field.getNumber();
        if (!fieldProto.getUnknownFields().isEmpty()
            || fieldProto.hasExtendee()
            || number < 1
            || number > 536870911
            || (number >= 19000 && number <= 19999))
          throw invalid("Illegal protobuf field number: " + number);
        if (proto.getReservedNameList().contains(field.getName())
            || proto.getReservedRangeList().stream()
                .anyMatch(r -> number >= r.getStart() && number < r.getEnd()))
          throw invalid("Field collides with a reserved name or number: " + field.getName());
        if (field.isRepeated()
            || field.hasDefaultValue()
            || !Json.ID.matcher(field.getName()).matches())
          throw invalid("Repeated fields and explicit defaults are unsupported");
        switch (field.getType()) {
          case STRING, INT64, DOUBLE, BOOL -> {}
          case MESSAGE -> {
            if (field.getMessageType()
                != TimestampProto.getDescriptor().findMessageTypeByName("Timestamp"))
              throw invalid("Only the standard Timestamp message type is supported");
          }
          default ->
              throw invalid(
                  "Unsupported protobuf type for "
                      + field.getName()
                      + "; use string, int64, double, bool or Timestamp");
        }
      }
      return new ProtobufSchema(
          message,
          root.toBuilder().clearSourceCodeInfo().build(),
          (ObjectNode) Json.canonical(body));
    } catch (InvalidProtocolBufferException
        | DescriptorValidationException
        | IllegalArgumentException ex) {
      throw invalid("Malformed protobuf descriptor: " + ex.getMessage());
    }
  }

  private static ApiException invalid(String message) {
    return new ApiException(422, "invalid_schema", message);
  }

  private static void rejectUnknownFields(Message message) {
    if (!message.getUnknownFields().isEmpty()) throw invalid("Unknown descriptor wire fields");
    for (var value : message.getAllFields().values()) {
      if (value instanceof Message nested) rejectUnknownFields(nested);
      else if (value instanceof List<?> list)
        for (var item : list) if (item instanceof Message nested) rejectUnknownFields(nested);
    }
  }

  boolean sameSchemaAs(ProtobufSchema other) {
    return customer.equals(other.customer)
        && dataset.equals(other.dataset)
        && version == other.version
        && descriptor.getFullName().equals(other.descriptor.getFullName())
        && normalizedRoot.equals(other.normalizedRoot);
  }

  void checkEvolution(ProtobufSchema previous) {
    if (version <= previous.version)
      throw new ApiException(409, "version_order", "New schema versions must increase");
    if (!descriptor.getFullName().equals(previous.descriptor.getFullName()))
      throw invalid("message_type cannot change");
    var oldProto = previous.descriptor.toProto();
    var nextProto = descriptor.toProto();
    for (String reservedName : oldProto.getReservedNameList()) {
      if (!nextProto.getReservedNameList().contains(reservedName)
          || descriptor.findFieldByName(reservedName) != null)
        throw invalid("Cannot remove or reuse reserved field name: " + reservedName);
    }
    for (var oldRange : oldProto.getReservedRangeList()) {
      boolean covered =
          nextProto.getReservedRangeList().stream()
              .anyMatch(
                  r -> r.getStart() <= oldRange.getStart() && r.getEnd() >= oldRange.getEnd());
      if (!covered
          || descriptor.getFields().stream()
              .anyMatch(
                  f -> f.getNumber() >= oldRange.getStart() && f.getNumber() < oldRange.getEnd()))
        throw invalid("Cannot remove or reuse reserved field range");
    }
    for (var old : previous.descriptor.getFields()) {
      var next = descriptor.findFieldByName(old.getName());
      if (next == null
          || next.getNumber() != old.getNumber()
          || next.getType() != old.getType()
          || next.isRequired() != old.isRequired())
        throw invalid(
            "Cannot remove, renumber, change type or change requiredness of " + old.getName());
    }
    for (var next : descriptor.getFields())
      if (previous.descriptor.findFieldByName(next.getName()) == null && next.isRequired())
        throw invalid("New fields must be optional: " + next.getName());
  }

  byte[] validateAndEncode(JsonNode payload) {
    if (payload == null || !payload.isObject())
      throw validation("invalid_payload", "payload must be a flat object");
    var builder = DynamicMessage.newBuilder(descriptor);
    var names = payload.fieldNames();
    while (names.hasNext()) {
      String name = names.next();
      if (descriptor.findFieldByName(name) == null)
        throw validation("unknown_field", "Undeclared field: " + name);
    }
    for (var field : descriptor.getFields()) {
      var value = payload.get(field.getName());
      if (value == null) {
        if (field.isRequired())
          throw validation("missing_required", "Missing required field: " + field.getName());
        continue;
      }
      if (value.isNull())
        throw validation("null_value", "Explicit null is forbidden: " + field.getName());
      Object scalar;
      switch (field.getType()) {
        case STRING -> {
          if (!value.isTextual()) throw wrong(field);
          if (!Json.validUnicode(value.textValue()))
            throw validation("invalid_unicode", "Unmatched Unicode surrogate: " + field.getName());
          scalar = value.textValue();
        }
        case INT64 -> {
          if (!value.isIntegralNumber() || !value.canConvertToLong()) throw wrong(field);
          scalar = value.longValue();
        }
        case DOUBLE -> {
          if (!value.isNumber() || !Double.isFinite(value.doubleValue())) throw wrong(field);
          scalar = value.doubleValue();
        }
        case BOOL -> {
          if (!value.isBoolean()) throw wrong(field);
          scalar = value.booleanValue();
        }
        case MESSAGE -> {
          if (!value.isTextual() || !TIMESTAMP.matcher(value.textValue()).matches())
            throw wrong(field);
          try {
            var parsed = OffsetDateTime.parse(value.textValue());
            long seconds = parsed.toEpochSecond();
            if (seconds < -62135596800L || seconds > 253402300799L || parsed.getYear() < 1)
              throw wrong(field);
            var timestamp = field.getMessageType();
            scalar =
                DynamicMessage.newBuilder(timestamp)
                    .setField(timestamp.findFieldByName("seconds"), seconds)
                    .setField(timestamp.findFieldByName("nanos"), parsed.getNano())
                    .build();
          } catch (DateTimeParseException ex) {
            throw wrong(field);
          }
        }
        default -> throw new IllegalStateException("Unvalidated schema type");
      }
      builder.setField(field, scalar);
    }
    return builder.build().toByteArray();
  }

  private static ApiException wrong(FieldDescriptor f) {
    return validation("incorrect_type", "Incorrect " + f.getType() + " value: " + f.getName());
  }

  private static ApiException validation(String code, String message) {
    return new ApiException(422, code, message);
  }
}
