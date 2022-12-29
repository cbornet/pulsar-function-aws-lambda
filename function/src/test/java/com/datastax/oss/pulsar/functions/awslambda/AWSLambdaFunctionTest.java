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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.amazonaws.services.lambda.AWSLambdaAsync;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.api.schema.KeyValueSchema;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaBuilder;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;
import org.apache.pulsar.client.impl.schema.KeyValueSchemaImpl;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.api.utils.FunctionRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class AWSLambdaFunctionTest {

  private static final ObjectMapper mapper =
      new ObjectMapper().registerModule(new JavaTimeModule());

  AWSLambdaFunction function;
  AWSLambdaAsync client;

  @BeforeMethod
  void setup() {
    function = spy(new AWSLambdaFunction());
    client = mock(AWSLambdaAsync.class);
    doReturn(client).when(function).createAwsClient();
  }

  @Test
  void testAvro() throws Exception {
    RecordSchemaBuilder recordSchemaBuilder = SchemaBuilder.record("record");
    recordSchemaBuilder.field("firstName").type(SchemaType.STRING);

    SchemaInfo schemaInfo = recordSchemaBuilder.build(SchemaType.AVRO);
    GenericSchema<GenericRecord> genericSchema = Schema.generic(schemaInfo);

    GenericRecord genericRecord = genericSchema.newRecordBuilder().set("firstName", "Jane").build();

    setClientHandler(
        input -> {
          try {
            return updateAvro((BytesTypedValue) input, "firstName");
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
    Record<?> outputRecord = processRecord(genericSchema, genericRecord);

    assertEquals(outputRecord.getSchema().getSchemaInfo().getType(), SchemaType.AVRO);
    GenericData.Record read = getRecord(outputRecord.getSchema(), (byte[]) outputRecord.getValue());
    assertEquals(read.getSchema().getFields().size(), 1);
    assertEquals(read.get("newfirstName"), new Utf8("Jane!"));
  }

  @Test
  void testKVAvro() throws Exception {
    RecordSchemaBuilder keySchemaBuilder = SchemaBuilder.record("keyRecord");
    keySchemaBuilder.field("keyField").type(SchemaType.STRING);
    GenericSchema<GenericRecord> keySchema =
        Schema.generic(keySchemaBuilder.build(SchemaType.AVRO));
    GenericRecord keyRecord = keySchema.newRecordBuilder().set("keyField", "keyValue").build();

    RecordSchemaBuilder valueSchemaBuilder = SchemaBuilder.record("valueRecord");
    valueSchemaBuilder.field("valueField").type(SchemaType.STRING);
    GenericSchema<GenericRecord> valueSchema =
        Schema.generic(valueSchemaBuilder.build(SchemaType.AVRO));
    GenericRecord valueRecord =
        valueSchema.newRecordBuilder().set("valueField", "valueValue").build();

    Schema<KeyValue<GenericRecord, GenericRecord>> keyValueSchema =
        KeyValueSchemaImpl.of(keySchema, valueSchema);
    KeyValue<GenericRecord, GenericRecord> keyValue = new KeyValue<>(keyRecord, valueRecord);

    setClientHandler(
        input -> {
          try {
            KeyValueTypedValue kv = (KeyValueTypedValue) input;
            BytesTypedValue keyField = updateAvro((BytesTypedValue) kv.getKey(), "keyField");
            BytesTypedValue valueField = updateAvro((BytesTypedValue) kv.getValue(), "valueField");
            KeyValueTypedValue output = new KeyValueTypedValue();
            output.setKey(keyField);
            output.setValue(valueField);
            output.setSchemaType(SchemaType.KEY_VALUE);
            return output;
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
    Record<?> outputRecord =
        processRecord(
            keyValueSchema,
            AutoConsumeSchema.wrapPrimitiveObject(keyValue, SchemaType.KEY_VALUE, new byte[] {}));

    assertEquals(outputRecord.getSchema().getSchemaInfo().getType(), SchemaType.KEY_VALUE);
    KeyValueSchema<?, ?> kvSchema = (KeyValueSchema<?, ?>) outputRecord.getSchema();
    KeyValue<?, ?> kv = (KeyValue<?, ?>) outputRecord.getValue();

    assertEquals(kvSchema.getKeySchema().getSchemaInfo().getType(), SchemaType.AVRO);
    GenericData.Record readKey = getRecord(kvSchema.getKeySchema(), (byte[]) kv.getKey());
    assertEquals(readKey.getSchema().getFields().size(), 1);
    assertEquals(readKey.get("newkeyField"), new Utf8("keyValue!"));

    assertEquals(kvSchema.getValueSchema().getSchemaInfo().getType(), SchemaType.AVRO);
    GenericData.Record readValue = getRecord(kvSchema.getValueSchema(), (byte[]) kv.getValue());
    assertEquals(readValue.getSchema().getFields().size(), 1);
    assertEquals(readValue.get("newvalueField"), new Utf8("valueValue!"));
  }

  private static BytesTypedValue updateAvro(BytesTypedValue value, String fieldName)
      throws IOException {
    assertEquals(value.getSchemaType(), SchemaType.AVRO);
    org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
    org.apache.avro.Schema avroSchema =
        parser.parse(new String(value.getSchema(), StandardCharsets.UTF_8));
    DatumReader<GenericData.Record> reader = new GenericDatumReader<>(avroSchema);
    Decoder decoder = DecoderFactory.get().binaryDecoder(value.getValue(), null);
    GenericData.Record read = reader.read(null, decoder);

    org.apache.avro.Schema newSchema =
        org.apache.avro.SchemaBuilder.record("new" + read.getSchema().getName())
            .fields()
            .optionalString("new" + fieldName)
            .endRecord();
    GenericData.Record newRecord = new GenericData.Record(newSchema);

    // Make modif
    newRecord.put("new" + fieldName, read.get(fieldName) + "!");

    // Serialize Avro
    GenericDatumWriter<org.apache.avro.generic.GenericRecord> writer =
        new GenericDatumWriter<>(newSchema);
    ByteArrayOutputStream oo = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(oo, null);
    writer.write(newRecord, encoder);

    BytesTypedValue outputValue = new BytesTypedValue();
    outputValue.setValue(oo.toByteArray());
    outputValue.setSchema(newSchema.toString().getBytes(StandardCharsets.UTF_8));
    outputValue.setSchemaType(SchemaType.AVRO);
    return outputValue;
  }

  private static GenericData.Record getRecord(Schema<?> schema, byte[] value) throws IOException {
    DatumReader<GenericData.Record> reader =
        new GenericDatumReader<>(
            (org.apache.avro.Schema)
                schema.getNativeSchema().orElseThrow(() -> new RuntimeException("missing schema")));
    Decoder decoder = DecoderFactory.get().binaryDecoder(value, null);
    return reader.read(null, decoder);
  }

  @DataProvider(name = "primitives")
  public static Object[][] primitives() {
    return new Object[][] {
      {
        SchemaType.BYTES,
        Schema.BYTES,
        new BytesTypedValue(),
        (Function<byte[], byte[]>) b -> new byte[] {(byte) (b[0] + 1)},
        new byte[] {42},
        new byte[] {43}
      },
      {
        SchemaType.STRING,
        Schema.STRING,
        new StringTypedValue(),
        (Function<String, String>) s -> s + "!",
        "test",
        "test!"
      },
      {
        SchemaType.INT8,
        Schema.INT8,
        new ByteTypedValue(),
        (Function<Byte, Byte>) b -> (byte) (b + 1),
        (byte) 42,
        (byte) 43
      },
      {
        SchemaType.INT16,
        Schema.INT16,
        new ShortTypedValue(),
        (Function<Short, Short>) s -> (short) (s + 1),
        (short) 42,
        (short) 43
      },
      {
        SchemaType.INT32,
        Schema.INT32,
        new IntegerTypedValue(),
        (Function<Integer, Integer>) i -> i + 1,
        42,
        43
      },
      {
        SchemaType.INT64,
        Schema.INT64,
        new LongTypedValue(),
        (Function<Long, Long>) l -> l + 1,
        42L,
        43L
      },
      {
        SchemaType.FLOAT,
        Schema.FLOAT,
        new FloatTypedValue(),
        (Function<Float, Float>) f -> f + 1,
        42F,
        43F
      },
      {
        SchemaType.DOUBLE,
        Schema.DOUBLE,
        new DoubleTypedValue(),
        (Function<Double, Double>) d -> d + 1,
        42D,
        43D
      },
      {
        SchemaType.BOOLEAN,
        Schema.BOOL,
        new BooleanTypedValue(),
        (Function<Boolean, Boolean>) b -> !b,
        true,
        false
      },
      {
        SchemaType.DATE,
        Schema.DATE,
        new DateTypedValue(),
        (Function<Date, Date>) d -> new Date(d.getTime() + 100),
        new Date(100),
        new Date(200)
      },
      {
        SchemaType.TIME,
        Schema.TIME,
        new TimeTypedValue(),
        (Function<Time, Time>) t -> new Time(t.getTime() + 100000),
        new Time(100000),
        new Time(200000)
      },
      {
        SchemaType.TIMESTAMP,
        Schema.TIMESTAMP,
        new TimestampTypedValue(),
        (Function<Timestamp, Timestamp>) t -> new Timestamp(t.getTime() + 100000),
        new Timestamp(100000),
        new Timestamp(200000)
      },
      {
        SchemaType.INSTANT,
        Schema.INSTANT,
        new InstantTypedValue(),
        (Function<Instant, Instant>) i -> i.plus(100, ChronoUnit.MILLIS),
        Instant.ofEpochMilli(100),
        Instant.ofEpochMilli(200)
      },
      {
        SchemaType.LOCAL_DATE,
        Schema.LOCAL_DATE,
        new LocalDateTypedValue(),
        (Function<LocalDate, LocalDate>) d -> d.plus(100, ChronoUnit.DAYS),
        LocalDate.ofEpochDay(100),
        LocalDate.ofEpochDay(200)
      },
      {
        SchemaType.LOCAL_TIME,
        Schema.LOCAL_TIME,
        new LocalTimeTypedValue(),
        (Function<LocalTime, LocalTime>) t -> t.plus(100, ChronoUnit.SECONDS),
        LocalTime.ofSecondOfDay(100),
        LocalTime.ofSecondOfDay(200)
      },
      {
        SchemaType.LOCAL_DATE_TIME,
        Schema.LOCAL_DATE_TIME,
        new LocalDateTimeTypedValue(),
        (Function<LocalDateTime, LocalDateTime>) t -> t.plus(100, ChronoUnit.SECONDS),
        LocalDateTime.ofEpochSecond(100, 0, ZoneOffset.UTC),
        LocalDateTime.ofEpochSecond(200, 0, ZoneOffset.UTC)
      },
    };
  }

  @Test(dataProvider = "primitives")
  void testPrimitives(
      SchemaType schemaType,
      Schema<Object> schema,
      TypedValue<Object> outputValue,
      Function<Object, Object> fromStringFunc,
      Object number,
      Object expected)
      throws Exception {
    setClientHandler(
        input -> {
          try {
            assertEquals(input.getSchemaType(), schemaType);
            outputValue.setValue(fromStringFunc.apply(input.getValue()));
            outputValue.setSchemaType(schemaType);
            return outputValue;
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });

    GenericRecord genericRecord =
        AutoConsumeSchema.wrapPrimitiveObject(number, schemaType, new byte[] {});
    Record<?> outputRecord = processRecord(schema, genericRecord);

    assertEquals(outputRecord.getValue(), expected);
    assertEquals(outputRecord.getSchema(), schema);
  }

  @Test
  void testKVPrimitives() throws Exception {
    Schema<KeyValue<String, String>> keyValueSchema =
        KeyValueSchemaImpl.of(Schema.STRING, Schema.STRING);
    KeyValue<String, String> keyValue = new KeyValue<>("key", "value");

    setClientHandler(
        input -> {
          KeyValueTypedValue kv = (KeyValueTypedValue) input;
          assertEquals(kv.getKey().getSchemaType(), SchemaType.STRING);
          assertEquals(kv.getValue().getSchemaType(), SchemaType.STRING);
          String newKey = ((StringTypedValue) kv.getKey()).getValue() + "!";
          String newValue = ((StringTypedValue) kv.getValue()).getValue() + "!";
          KeyValueTypedValue output = new KeyValueTypedValue();
          StringTypedValue keyField = new StringTypedValue();
          keyField.setValue(newKey);
          keyField.setSchemaType(SchemaType.STRING);
          output.setKey(keyField);
          StringTypedValue valueField = new StringTypedValue();
          valueField.setValue(newValue);
          valueField.setSchemaType(SchemaType.STRING);
          output.setValue(valueField);
          output.setSchemaType(SchemaType.KEY_VALUE);
          return output;
        });
    Record<?> outputRecord =
        processRecord(
            keyValueSchema,
            AutoConsumeSchema.wrapPrimitiveObject(keyValue, SchemaType.KEY_VALUE, new byte[] {}));

    assertEquals(outputRecord.getSchema().getSchemaInfo().getType(), SchemaType.KEY_VALUE);
    KeyValueSchema<?, ?> kvSchema = (KeyValueSchema<?, ?>) outputRecord.getSchema();
    KeyValue<?, ?> kv = (KeyValue<?, ?>) outputRecord.getValue();

    assertEquals(kvSchema.getKeyValueEncodingType(), KeyValueEncodingType.INLINE);

    assertEquals(kvSchema.getKeySchema().getSchemaInfo().getType(), SchemaType.STRING);
    assertEquals(kv.getKey(), "key!");

    assertEquals(kvSchema.getValueSchema().getSchemaInfo().getType(), SchemaType.STRING);
    assertEquals(kv.getValue(), "value!");
  }

  @Test
  void testKVPrimitivesSeparated() throws Exception {
    Schema<KeyValue<String, String>> keyValueSchema =
        KeyValueSchemaImpl.of(Schema.STRING, Schema.STRING, KeyValueEncodingType.SEPARATED);
    KeyValue<String, String> keyValue = new KeyValue<>("key", "value");

    doAnswer(
            invocationOnMock -> {
              try {
                InvokeRequest request = invocationOnMock.getArgument(0, InvokeRequest.class);
                JsonRecord jsonRecord =
                    mapper.readValue(request.getPayload().array(), JsonRecord.class);

                assertEquals(jsonRecord.getKeyValueEncodingType(), KeyValueEncodingType.SEPARATED);

                InvokeResult result = new InvokeResult();
                result.setPayload(request.getPayload());
                result.setStatusCode(200);
                return CompletableFuture.completedFuture(result);
              } catch (Exception e) {
                CompletableFuture<Object> failed = new CompletableFuture<>();
                failed.completeExceptionally(e);
                return e;
              }
            })
        .when(client)
        .invokeAsync(any(InvokeRequest.class));
    Record<?> outputRecord =
        processRecord(
            keyValueSchema,
            AutoConsumeSchema.wrapPrimitiveObject(keyValue, SchemaType.KEY_VALUE, new byte[] {}));

    assertEquals(outputRecord.getSchema().getSchemaInfo().getType(), SchemaType.KEY_VALUE);
    KeyValueSchema<?, ?> kvSchema = (KeyValueSchema<?, ?>) outputRecord.getSchema();
    KeyValue<?, ?> kv = (KeyValue<?, ?>) outputRecord.getValue();

    assertEquals(kvSchema.getKeyValueEncodingType(), KeyValueEncodingType.SEPARATED);

    assertEquals(kvSchema.getKeySchema().getSchemaInfo().getType(), SchemaType.STRING);
    assertEquals(kv.getKey(), "key");

    assertEquals(kvSchema.getValueSchema().getSchemaInfo().getType(), SchemaType.STRING);
    assertEquals(kv.getValue(), "value");
  }

  @Test
  void testJson() throws Exception {
    RecordSchemaBuilder recordSchemaBuilder = SchemaBuilder.record("record");
    recordSchemaBuilder.field("firstName").type(SchemaType.STRING);

    SchemaInfo schemaInfo = recordSchemaBuilder.build(SchemaType.JSON);
    GenericSchema<GenericRecord> genericSchema = Schema.generic(schemaInfo);

    GenericRecord genericRecord = genericSchema.newRecordBuilder().set("firstName", "Jane").build();

    setClientHandler(
        input -> {
          assertEquals(input.getSchemaType(), SchemaType.JSON);
          org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
          org.apache.avro.Schema avroSchema =
              parser.parse(new String(input.getSchema(), StandardCharsets.UTF_8));
          assertNotNull(avroSchema.getField("firstName"));

          org.apache.avro.Schema newSchema =
              org.apache.avro.SchemaBuilder.record("newRecord")
                  .fields()
                  .optionalString("newFirstName")
                  .endRecord();

          JsonTypedValue jsonTypedValue = (JsonTypedValue) input;

          JsonTypedValue output = new JsonTypedValue();
          ObjectMapper objectMapper = new ObjectMapper();
          ObjectNode jsonNode = objectMapper.createObjectNode();

          jsonNode.put("newFirstName", jsonTypedValue.getValue().get("firstName").asText() + "!");

          output.setSchemaType(SchemaType.JSON);
          output.setSchema(newSchema.toString().getBytes(StandardCharsets.UTF_8));
          output.setValue(jsonNode);
          return output;
        });
    Record<?> outputRecord = processRecord(genericSchema, genericRecord);

    assertEquals(outputRecord.getSchema().getSchemaInfo().getType(), SchemaType.JSON);
    JsonNode value = (JsonNode) outputRecord.getValue();
    assertEquals(value.get("newFirstName").asText(), "Jane!");
  }

  @Test
  void testRecordAttributes() throws Exception {
    doAnswer(
            invocationOnMock -> {
              try {
                InvokeRequest request = invocationOnMock.getArgument(0, InvokeRequest.class);
                JsonRecord jsonRecord =
                    mapper.readValue(request.getPayload().array(), JsonRecord.class);

                assertEquals(jsonRecord.getTopicName(), "my-topic");
                assertEquals(jsonRecord.getKey(), "my-key");
                assertEquals(jsonRecord.getDestinationTopic(), "my-destination-topic");
                assertEquals(jsonRecord.getEventTime().longValue(), 100L);
                assertEquals(jsonRecord.getProperties().size(), 1);
                assertEquals(jsonRecord.getProperties().get("my-prop"), "my-prop-value");
                assertEquals(jsonRecord.getPartitionId(), "my-partition-id");
                assertEquals(jsonRecord.getPartitionIndex().intValue(), 100);
                assertEquals(jsonRecord.getRecordSequence().longValue(), 100L);

                Map<String, String> properties = new HashMap<>();
                properties.put("new-prop", "new-prop-value");
                JsonRecord outputRecord = new JsonRecord();
                outputRecord.setKey("new-key");
                outputRecord.setDestinationTopic("new-destination-topic");
                outputRecord.setEventTime(200L);
                outputRecord.setProperties(properties);
                outputRecord.setTopicName("new-topic");
                outputRecord.setRecordSequence(200L);
                outputRecord.setPartitionId("new-partition-id");
                outputRecord.setPartitionIndex(200);

                StringTypedValue value = new StringTypedValue();
                value.setSchemaType(SchemaType.STRING);
                value.setValue("new-value");

                outputRecord.setValue(value);

                byte[] output = mapper.writeValueAsBytes(outputRecord);

                InvokeResult result = new InvokeResult();
                result.setPayload(ByteBuffer.wrap(output));
                result.setStatusCode(200);
                return CompletableFuture.completedFuture(result);
              } catch (Exception e) {
                CompletableFuture<Object> failed = new CompletableFuture<>();
                failed.completeExceptionally(e);
                return e;
              }
            })
        .when(client)
        .invokeAsync(any(InvokeRequest.class));

    GenericRecord genericRecord =
        AutoConsumeSchema.wrapPrimitiveObject(42, SchemaType.INT32, new byte[] {});
    Map<String, String> properties = new HashMap<>();
    properties.put("my-prop", "my-prop-value");
    TestRecord<Object> record =
        TestRecord.builder()
            .value(genericRecord)
            .schema(Schema.INT32)
            .destinationTopic("my-destination-topic")
            .eventTime(100L)
            .properties(properties)
            .key("my-key")
            .topicName("my-topic")
            .partitionId("my-partition-id")
            .partitionIndex(100)
            .recordSequence(100L)
            .build();
    Map<String, Object> config = new HashMap<>();

    TestContext context = new TestContext(record, config);
    function.initialize(context);
    Record<?> outputRecord = function.process(genericRecord, context);

    assertEquals(outputRecord.getKey().orElse(null), "new-key");
    assertEquals(outputRecord.getDestinationTopic().orElse(null), "new-destination-topic");
    assertEquals(outputRecord.getEventTime().orElse(0L).longValue(), 200L);
    assertEquals(outputRecord.getProperties().size(), 1);
    assertEquals(outputRecord.getProperties().get("new-prop"), "new-prop-value");
    assertEquals(outputRecord.getTopicName().orElse(null), "new-topic");
    assertEquals(outputRecord.getPartitionId().orElse(null), "new-partition-id");
    assertEquals(outputRecord.getPartitionIndex().orElse(0).intValue(), 200);
    assertEquals(outputRecord.getRecordSequence().orElse(0L).intValue(), 200L);
    assertEquals(outputRecord.getValue(), "new-value");
    assertEquals(outputRecord.getSchema(), Schema.STRING);
  }

  private Record<?> processRecord(Schema<?> schema, GenericRecord genericRecord) throws Exception {
    TestRecord<Object> record = TestRecord.builder().value(genericRecord).schema(schema).build();
    Map<String, Object> config = new HashMap<>();

    TestContext context = new TestContext(record, config);
    function.initialize(context);
    return function.process(genericRecord, context);
  }

  private void setClientHandler(Function<TypedValue<?>, TypedValue<?>> handler) {
    doAnswer(
            invocationOnMock -> {
              try {
                InvokeRequest request = invocationOnMock.getArgument(0, InvokeRequest.class);
                JsonRecord jsonRecord =
                    mapper.readValue(request.getPayload().array(), JsonRecord.class);

                TypedValue<?> outputValue = handler.apply(jsonRecord.getValue());
                JsonRecord outputJsonRecord = new JsonRecord();
                outputJsonRecord.setValue(outputValue);

                byte[] reponseBody = mapper.writeValueAsBytes(outputJsonRecord);
                InvokeResult result = new InvokeResult();
                result.setPayload(ByteBuffer.wrap(reponseBody));
                result.setStatusCode(200);
                return CompletableFuture.completedFuture(result);
              } catch (Exception e) {
                CompletableFuture<Object> failed = new CompletableFuture<>();
                failed.completeExceptionally(e);
                return e;
              }
            })
        .when(client)
        .invokeAsync(any(InvokeRequest.class));
  }

  @Builder
  @RequiredArgsConstructor
  @AllArgsConstructor
  public static class TestRecord<T> implements Record<T> {
    private final Schema<?> schema;
    private final T value;
    private final String key;
    private String topicName;
    private String destinationTopic;
    private Long eventTime;
    Map<String, String> properties;
    private String partitionId;

    private Integer partitionIndex;

    private Long recordSequence;

    @Override
    public Optional<String> getKey() {
      return Optional.ofNullable(key);
    }

    @Override
    public Schema<T> getSchema() {
      return (Schema<T>) schema;
    }

    @Override
    public T getValue() {
      return value;
    }

    @Override
    public Optional<String> getTopicName() {
      return Optional.ofNullable(topicName);
    }

    @Override
    public Optional<String> getDestinationTopic() {
      return Optional.ofNullable(destinationTopic);
    }

    @Override
    public Optional<Long> getEventTime() {
      return Optional.ofNullable(eventTime);
    }

    @Override
    public Map<String, String> getProperties() {
      return properties;
    }

    @Override
    public Optional<String> getPartitionId() {
      return Optional.ofNullable(partitionId);
    }

    @Override
    public Optional<Integer> getPartitionIndex() {
      return Optional.ofNullable(partitionIndex);
    }

    @Override
    public Optional<Long> getRecordSequence() {
      return Optional.ofNullable(recordSequence);
    }
  }

  public static class TestContext implements Context {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestContext.class);

    private final Record<?> record;
    private final Map<String, Object> userConfig;

    public TestContext(Record<?> record, Map<String, Object> userConfig) {
      this.record = record;
      this.userConfig = userConfig;
    }

    @Override
    public Collection<String> getInputTopics() {
      return null;
    }

    @Override
    public String getOutputTopic() {
      return null;
    }

    @Override
    public Record<?> getCurrentRecord() {
      return record;
    }

    @Override
    public String getOutputSchemaType() {
      return null;
    }

    @Override
    public String getFunctionName() {
      return null;
    }

    @Override
    public String getFunctionId() {
      return null;
    }

    @Override
    public String getFunctionVersion() {
      return null;
    }

    @Override
    public Map<String, Object> getUserConfigMap() {
      return userConfig;
    }

    @Override
    public Optional<Object> getUserConfigValue(String key) {
      return Optional.empty();
    }

    @Override
    public Object getUserConfigValueOrDefault(String key, Object defaultValue) {
      return null;
    }

    @Override
    public PulsarAdmin getPulsarAdmin() {
      return null;
    }

    @Override
    public <O> CompletableFuture<Void> publish(
        String topicName, O object, String schemaOrSerdeClassName) {
      return null;
    }

    @Override
    public <O> CompletableFuture<Void> publish(String topicName, O object) {
      return null;
    }

    @Override
    public <O> TypedMessageBuilder<O> newOutputMessage(String topicName, Schema<O> schema) {
      return null;
    }

    @Override
    public <O> ConsumerBuilder<O> newConsumerBuilder(Schema<O> schema) {
      return null;
    }

    @Override
    public <X> FunctionRecord.FunctionRecordBuilder<X> newOutputRecordBuilder(Schema<X> schema) {
      return FunctionRecord.from(this, schema);
    }

    @Override
    public String getTenant() {
      return null;
    }

    @Override
    public String getNamespace() {
      return null;
    }

    @Override
    public int getInstanceId() {
      return 0;
    }

    @Override
    public int getNumInstances() {
      return 0;
    }

    @Override
    public Logger getLogger() {
      return LOGGER;
    }

    @Override
    public String getSecret(String secretName) {
      return null;
    }

    @Override
    public void putState(String key, ByteBuffer value) {}

    @Override
    public CompletableFuture<Void> putStateAsync(String key, ByteBuffer value) {
      return null;
    }

    @Override
    public ByteBuffer getState(String key) {
      return null;
    }

    @Override
    public CompletableFuture<ByteBuffer> getStateAsync(String key) {
      return null;
    }

    @Override
    public void deleteState(String key) {}

    @Override
    public CompletableFuture<Void> deleteStateAsync(String key) {
      return null;
    }

    @Override
    public void incrCounter(String key, long amount) {}

    @Override
    public CompletableFuture<Void> incrCounterAsync(String key, long amount) {
      return null;
    }

    @Override
    public long getCounter(String key) {
      return 0;
    }

    @Override
    public CompletableFuture<Long> getCounterAsync(String key) {
      return null;
    }

    @Override
    public void recordMetric(String metricName, double value) {}
  }
}
