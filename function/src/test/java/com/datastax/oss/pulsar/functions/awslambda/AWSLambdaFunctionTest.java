package com.datastax.oss.pulsar.functions.awslambda;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import com.amazonaws.services.lambda.AWSLambdaAsync;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
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
import org.apache.avro.data.Json;
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
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaBuilder;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;
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

    private static ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());

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

        setClientHandler(input -> {
                try {
                    byte[] schema = input.getSchema();
                    byte[] value = (byte[]) input.getValue();
                    assertEquals(input.getSchemaType(), SchemaType.AVRO);
                    org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
                    org.apache.avro.Schema avroSchema = parser.parse(new String(schema, StandardCharsets.UTF_8));
                    DatumReader<GenericData.Record> reader = new GenericDatumReader<>(avroSchema);
                    Decoder decoder = DecoderFactory.get().binaryDecoder(value, null);
                    GenericData.Record read = reader.read(null, decoder);

                    org.apache.avro.Schema newSchema =
                        org.apache.avro.SchemaBuilder.record("myrecord").fields().optionalString("newFirstName")
                            .endRecord();
                    GenericData.Record newRecord = new GenericData.Record(newSchema);

                    // Make modif
                    newRecord.put("newFirstName", read.get("firstName") + "!");

                    // Serialize Avro
                    GenericDatumWriter<org.apache.avro.generic.GenericRecord> writer = new GenericDatumWriter<>(newSchema);
                    ByteArrayOutputStream oo = new ByteArrayOutputStream();
                    BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(oo, null);
                    writer.write(newRecord, encoder);

                    BytesTypedValue outputValue = new BytesTypedValue();
                    outputValue.setValue(oo.toByteArray());
                    outputValue.setSchema(newSchema.toString().getBytes(StandardCharsets.UTF_8));
                    outputValue.setSchemaType(SchemaType.AVRO);
                    return outputValue;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        Record<?> outputRecord = processRecord(genericSchema, genericRecord);

        assertEquals(outputRecord.getSchema().getSchemaInfo().getType(), SchemaType.AVRO);
        GenericData.Record read = getRecord(outputRecord.getSchema(), (byte[]) outputRecord.getValue());
        assertEquals(read.get("newFirstName"), new Utf8("Jane!"));
    }

    private static GenericData.Record getRecord(Schema<?> schema, byte[] value) throws IOException {
        DatumReader<GenericData.Record> reader =
            new GenericDatumReader<>((org.apache.avro.Schema) schema.getNativeSchema().orElseThrow(() -> new RuntimeException("missing schema")));
        Decoder decoder = DecoderFactory.get().binaryDecoder(value, null);
        return reader.read(null, decoder);
    }

    @DataProvider(name = "primitives")
    public static Object[][] primitives() {
        return new Object[][] {
//            {SchemaType.BYTES, Schema.BYTES, new BytesTypedValue(), (Function<byte[], byte[]>) b -> {
//                byte[] out = new byte[]{(byte) (b[0] + 1)};
//                return out;
//            }, new byte[]{42}, new byte[]{43}},
//            {SchemaType.STRING, Schema.STRING, new StringTypedValue(), (Function<String, String>) s -> s + "!", "test", "test!"},
//            {SchemaType.INT8, Schema.INT8, new ByteTypedValue(), (Function<Byte, Byte>) b -> (byte) (b + 1), (byte) 42, (byte) 43},
//            {SchemaType.INT16, Schema.INT16, new ShortTypedValue(), (Function<Short, Short>) s -> (short) (s + 1), (short) 42, (short) 43},
//            {SchemaType.INT32, Schema.INT32, new IntegerTypedValue(), (Function<Integer, Integer>) i -> i + 1, 42, 43},
//            {SchemaType.INT64, Schema.INT64, new LongTypedValue(), (Function<Long, Long>) l -> l + 1, 42L, 43L},
//            {SchemaType.FLOAT, Schema.FLOAT, new FloatTypedValue(), (Function<Float, Float>) f -> f + 1, 42F, 43F},
            {SchemaType.DOUBLE, Schema.DOUBLE, new DoubleTypedValue(), (Function<Double, Double>) d -> d + 1, 42D, 43D},
//            {SchemaType.BOOLEAN, Schema.BOOL, new BooleanTypedValue(), (Function<Boolean, Boolean>) b -> !b, true, false},
//            {SchemaType.DATE, Schema.DATE, new DateTypedValue(), (Function<Date, Date>) d -> new Date(d.getTime() + 100), new Date(100), new Date(200)},
//            {SchemaType.TIME, Schema.TIME, new TimeTypedValue(), (Function<Time, Time>) t -> new Time(t.getTime() + 100000), new Time(100000), new Time(200000)},
//            {SchemaType.TIMESTAMP, Schema.TIMESTAMP, new TimestampTypedValue(), (Function<Timestamp, Timestamp>) t -> new Timestamp(t.getTime() + 100000), new Timestamp(100000), new Timestamp(200000)},
//            {SchemaType.INSTANT, Schema.INSTANT, new InstantTypedValue(), (Function<Instant, Instant>) i -> i.plus(100, ChronoUnit.MILLIS), Instant.ofEpochMilli(100), Instant.ofEpochMilli(200)},
//            {SchemaType.LOCAL_DATE, Schema.LOCAL_DATE, new LocalDateTypedValue(), (Function<LocalDate, LocalDate>) d -> d.plus(100, ChronoUnit.DAYS), LocalDate.ofEpochDay(100), LocalDate.ofEpochDay(200)},
//            {SchemaType.LOCAL_TIME, Schema.LOCAL_TIME, new LocalTimeTypedValue(), (Function<LocalTime, LocalTime>) t -> t.plus(100, ChronoUnit.SECONDS), LocalTime.ofSecondOfDay(100), LocalTime.ofSecondOfDay(200)},
//            {SchemaType.LOCAL_DATE_TIME, Schema.LOCAL_DATE_TIME, new LocalDateTimeTypedValue(), (Function<LocalDateTime, LocalDateTime>) t -> t.plus(100, ChronoUnit.SECONDS), LocalDateTime.ofEpochSecond(100, 0,
//                ZoneOffset.UTC), LocalDateTime.ofEpochSecond(200, 0, ZoneOffset.UTC)},
        };
    }

    @Test(dataProvider = "primitives")
    void testPrimitives(SchemaType schemaType, Schema<Object> schema, TypedValue<Object> outputValue, Function<Object, Object> fromStringFunc, Object number, Object expected) throws Exception {
        setClientHandler(input -> {
            try {
                assertEquals(input.getSchemaType(), schemaType);
                outputValue.setValue(fromStringFunc.apply(input.getValue()));
                outputValue.setSchemaType(schemaType);
                return outputValue;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        GenericRecord genericRecord = AutoConsumeSchema.wrapPrimitiveObject(number, schemaType, new byte[]{});
        Record<?> outputRecord = processRecord(schema, genericRecord);

        assertEquals(outputRecord.getValue(), expected);
        assertEquals(outputRecord.getSchema(), schema);
    }

    @Test
    void testBoolean() throws Exception {
        setClientHandler(input -> {
            Boolean value = (Boolean) input.getValue();
            assertEquals(input.getSchemaType(), SchemaType.BOOLEAN);
            BooleanTypedValue outputValue = new BooleanTypedValue();
            outputValue.setValue(!value);
            outputValue.setSchemaType(SchemaType.BOOLEAN);
            return outputValue;
        });

        GenericRecord genericRecord = AutoConsumeSchema.wrapPrimitiveObject(true, SchemaType.BOOLEAN, new byte[]{});
        Record<?> outputRecord = processRecord(Schema.BOOL, genericRecord);

        assertEquals(outputRecord.getValue(), false);
        assertEquals(outputRecord.getSchema(), Schema.BOOL);
    }

    private Record<?> processRecord(Schema<?> schema, GenericRecord genericRecord)
        throws Exception {
        TestRecord<Object> record = TestRecord.builder().value(genericRecord).schema(schema).build();
        Map<String, Object> config = new HashMap<>();

        TestContext context = new TestContext(record, config);
        function.initialize(context);
        return function.process(genericRecord, context);
    }

    private void setClientHandler(Function<TypedValue<?>, TypedValue<?>> handler) {
        doAnswer(invocationOnMock -> {
            try {
                InvokeRequest request = invocationOnMock.getArgument(0, InvokeRequest.class);
                JsonRecord jsonRecord = mapper.readValue(request.getPayload().array(), JsonRecord.class);

                TypedValue<?> outputValue = handler.apply(jsonRecord.getValue());
                JsonRecord outputJsonRecord = new JsonRecord();
                outputJsonRecord.setValue(outputValue);

                LambdaResponse lambdaResponse = new LambdaResponse();
                lambdaResponse.setStatusCode(200);
                lambdaResponse.setBody(outputJsonRecord);
                byte[] reponseBody = mapper.writeValueAsBytes(lambdaResponse);
                InvokeResult result = new InvokeResult();
                result.setPayload(ByteBuffer.wrap(reponseBody));
                result.setStatusCode(200);
                return CompletableFuture.completedFuture(result);
            } catch (Exception e) {
                CompletableFuture<Object> failed = new CompletableFuture<>();
                failed.completeExceptionally(e);
                return e;
            }
        }).when(client).invokeAsync(any(InvokeRequest.class));
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
        public <O> CompletableFuture<Void> publish(String topicName, O object, String schemaOrSerdeClassName) {
            return null;
        }

        @Override
        public <O> CompletableFuture<Void> publish(String topicName, O object) {
            return null;
        }

        @Override
        public <O> TypedMessageBuilder<O> newOutputMessage(String topicName, Schema<O> schema)
            throws PulsarClientException {
            return null;
        }

        @Override
        public <O> ConsumerBuilder<O> newConsumerBuilder(Schema<O> schema) throws PulsarClientException {
            return null;
        }

        @Override
        public <X> FunctionRecord.FunctionRecordBuilder<X> newOutputRecordBuilder(Schema<X> schema) {
            return FunctionRecord.from(
                this, schema

            );
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
        public void putState(String key, ByteBuffer value) {

        }

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
        public void deleteState(String key) {

        }

        @Override
        public CompletableFuture<Void> deleteStateAsync(String key) {
            return null;
        }

        @Override
        public void incrCounter(String key, long amount) {

        }

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
        public void recordMetric(String metricName, double value) {

        }
    }

}