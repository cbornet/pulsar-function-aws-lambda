/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.pulsar.functions.awslambda;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.JsonNode;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Date;
import java.util.Map;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaType;

/**
 * The payload class convert from {@link org.apache.pulsar.functions.api.Record} for AWS lambda
 * invoke request.
 */
@NoArgsConstructor
@Data
public class JsonRecord {
  private TypedValue<?> value;
  private String key;
  private String topicName;
  private String partitionId;
  private Integer partitionIndex;
  private Long recordSequence;
  private String destinationTopic;
  private Long eventTime;
  private Map<String, String> properties;
}

@Data
@JsonTypeInfo(
  use = JsonTypeInfo.Id.NAME,
  include = JsonTypeInfo.As.EXISTING_PROPERTY,
  visible = true,
  property = "schemaType"
)
@JsonSubTypes({
  @JsonSubTypes.Type(value = StringTypedValue.class, name = "STRING"),
  @JsonSubTypes.Type(value = ByteTypedValue.class, name = "INT8"),
  @JsonSubTypes.Type(value = ShortTypedValue.class, name = "INT16"),
  @JsonSubTypes.Type(value = IntegerTypedValue.class, name = "INT32"),
  @JsonSubTypes.Type(value = LongTypedValue.class, name = "INT64"),
  @JsonSubTypes.Type(value = FloatTypedValue.class, name = "FLOAT"),
  @JsonSubTypes.Type(value = DoubleTypedValue.class, name = "DOUBLE"),
  @JsonSubTypes.Type(value = BooleanTypedValue.class, name = "BOOLEAN"),
  @JsonSubTypes.Type(value = DateTypedValue.class, name = "DATE"),
  @JsonSubTypes.Type(value = TimeTypedValue.class, name = "TIME"),
  @JsonSubTypes.Type(value = TimestampTypedValue.class, name = "TIMESTAMP"),
  @JsonSubTypes.Type(value = InstantTypedValue.class, name = "INSTANT"),
  @JsonSubTypes.Type(value = LocalDateTypedValue.class, name = "LOCAL_DATE"),
  @JsonSubTypes.Type(value = LocalTimeTypedValue.class, name = "LOCAL_TIME"),
  @JsonSubTypes.Type(value = LocalDateTimeTypedValue.class, name = "LOCAL_DATE_TIME"),
  @JsonSubTypes.Type(value = JsonTypedValue.class, name = "JSON"),
  @JsonSubTypes.Type(value = KeyValueTypedValue.class, name = "KEY_VALUE"),
  @JsonSubTypes.Type(
    value = BytesTypedValue.class,
    names = {"BYTES", "AVRO"}
  ),
})
abstract class TypedValue<T> {
  protected SchemaType schemaType;
  protected byte[] schema;
  protected T value;
}

class StringTypedValue extends TypedValue<String> {}

class ByteTypedValue extends TypedValue<Byte> {}

class ShortTypedValue extends TypedValue<Short> {}

class IntegerTypedValue extends TypedValue<Integer> {}

class LongTypedValue extends TypedValue<Long> {}

class FloatTypedValue extends TypedValue<Float> {}

class DoubleTypedValue extends TypedValue<Double> {}

class BooleanTypedValue extends TypedValue<Boolean> {}

class DateTypedValue extends TypedValue<Date> {}

class TimeTypedValue extends TypedValue<Time> {}

class TimestampTypedValue extends TypedValue<Timestamp> {}

class InstantTypedValue extends TypedValue<Instant> {}

class LocalDateTypedValue extends TypedValue<LocalDate> {}

class LocalTimeTypedValue extends TypedValue<LocalTime> {}

class LocalDateTimeTypedValue extends TypedValue<LocalDateTime> {}

class JsonTypedValue extends TypedValue<JsonNode> {}

class BytesTypedValue extends TypedValue<byte[]> {}

@Data
@EqualsAndHashCode(callSuper = true)
class KeyValueTypedValue extends TypedValue<TypedValue<?>> {
  private KeyValueEncodingType keyValueEncodingType;
  private TypedValue<?> key;
}
