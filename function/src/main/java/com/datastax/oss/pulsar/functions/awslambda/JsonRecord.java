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
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaType;

/**
 * The payload class convert from {@link org.apache.pulsar.functions.api.Record} for AWS lambda
 * invoke request.
 */
@Data
@JsonTypeInfo(
  use = JsonTypeInfo.Id.NAME,
  include = JsonTypeInfo.As.EXISTING_PROPERTY,
  defaultImpl = BytesJsonRecord.class,
  visible = true,
  property = "schemaType"
)
@JsonSubTypes({
  @JsonSubTypes.Type(value = StringJsonRecord.class, name = "STRING"),
  @JsonSubTypes.Type(value = ByteJsonRecord.class, name = "INT8"),
  @JsonSubTypes.Type(value = ShortJsonRecord.class, name = "INT16"),
  @JsonSubTypes.Type(value = IntegerJsonRecord.class, name = "INT32"),
  @JsonSubTypes.Type(value = LongJsonRecord.class, name = "INT64"),
  @JsonSubTypes.Type(value = FloatJsonRecord.class, name = "FLOAT"),
  @JsonSubTypes.Type(value = DoubleJsonRecord.class, name = "DOUBLE"),
  @JsonSubTypes.Type(value = BooleanJsonRecord.class, name = "BOOLEAN"),
  @JsonSubTypes.Type(value = DateJsonRecord.class, name = "DATE"),
  @JsonSubTypes.Type(value = TimeJsonRecord.class, name = "TIME"),
  @JsonSubTypes.Type(value = TimestampJsonRecord.class, name = "TIMESTAMP"),
  @JsonSubTypes.Type(value = InstantJsonRecord.class, name = "INSTANT"),
  @JsonSubTypes.Type(value = LocalDateJsonRecord.class, name = "LOCAL_DATE"),
  @JsonSubTypes.Type(value = LocalTimeJsonRecord.class, name = "LOCAL_TIME"),
  @JsonSubTypes.Type(value = LocalDateTimeJsonRecord.class, name = "LOCAL_DATE_TIME"),
  @JsonSubTypes.Type(value = JsonJsonRecord.class, name = "JSON"),
  @JsonSubTypes.Type(value = KeyValueJsonRecord.class, name = "KEY_VALUE"),
  @JsonSubTypes.Type(
    value = BytesJsonRecord.class,
    names = {"BYTES", "AVRO"}
  ),
})
abstract class JsonRecord<K, V> {
  private V value;
  private K key;
  private SchemaType schemaType;
  private byte[] schema;
  private String topicName;
  private String partitionId;
  private Integer partitionIndex;
  private Long recordSequence;
  private String destinationTopic;
  private Long eventTime;
  private Map<String, String> properties;
}

abstract class StringKeyJsonRecord<V> extends JsonRecord<String, V> {}

class StringJsonRecord extends StringKeyJsonRecord<String> {}

class ByteJsonRecord extends StringKeyJsonRecord<Byte> {}

class ShortJsonRecord extends StringKeyJsonRecord<Short> {}

class IntegerJsonRecord extends StringKeyJsonRecord<Integer> {}

class LongJsonRecord extends StringKeyJsonRecord<Long> {}

class FloatJsonRecord extends StringKeyJsonRecord<Float> {}

class DoubleJsonRecord extends StringKeyJsonRecord<Double> {}

class BooleanJsonRecord extends StringKeyJsonRecord<Boolean> {}

class DateJsonRecord extends StringKeyJsonRecord<Date> {}

class TimeJsonRecord extends StringKeyJsonRecord<Time> {}

class TimestampJsonRecord extends StringKeyJsonRecord<Timestamp> {}

class InstantJsonRecord extends StringKeyJsonRecord<Instant> {}

class LocalDateJsonRecord extends StringKeyJsonRecord<LocalDate> {}

class LocalTimeJsonRecord extends StringKeyJsonRecord<LocalTime> {}

class LocalDateTimeJsonRecord extends StringKeyJsonRecord<LocalDateTime> {}

class JsonJsonRecord extends StringKeyJsonRecord<JsonNode> {}

class BytesJsonRecord extends StringKeyJsonRecord<byte[]> {}

@Data
@EqualsAndHashCode(callSuper = true)
class KeyValueJsonRecord<K extends JsonRecord<?, ?>, V extends JsonRecord<?, ?>>
    extends JsonRecord<K, V> {
  private KeyValueEncodingType keyValueEncodingType;
}
