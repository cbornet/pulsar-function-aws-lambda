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

import com.amazonaws.arn.Arn;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.lambda.AWSLambdaAsync;
import com.amazonaws.services.lambda.AWSLambdaAsyncClient;
import com.amazonaws.services.lambda.AWSLambdaAsyncClientBuilder;
import com.amazonaws.services.lambda.model.InvocationType;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;
import com.amazonaws.services.lambda.model.RequestTooLargeException;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
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
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.KeyValueSchema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.api.utils.FunctionRecord;
import org.apache.pulsar.io.aws.AbstractAwsConnector;
import org.apache.pulsar.io.aws.AwsCredentialProviderPlugin;
import org.slf4j.Logger;

public class AWSLambdaFunction extends AbstractAwsConnector
    implements Function<GenericObject, Record<GenericObject>> {

  private static final long DEFAULT_INVOKE_TIMEOUT_MS = 5 * 60 * 1000L;
  private static final int MAX_PAYLOAD_SIZE_BYTES = (6 * 1024 * 1024);

  private static final ObjectMapper MAPPER =
      new ObjectMapper()
          .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
          .configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true)
          .setSerializationInclusion(JsonInclude.Include.NON_NULL)
          .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
          .configure(SerializationFeature.FAIL_ON_UNWRAPPED_TYPE_IDENTIFIERS, false)
          .registerModule(new JavaTimeModule());

  private AWSLambdaAsync client;
  private AWSLambdaFunctionConfig config;

  private Logger logger;
  private List<String> excludedFields;

  @Override
  public void initialize(Context context) throws Exception {
    logger = context.getLogger();
    config =
        MAPPER.readValue(
            MAPPER.writeValueAsString(context.getUserConfigMap()), AWSLambdaFunctionConfig.class);
    excludedFields =
        Arrays.stream(config.getExcludedFields().split(","))
            .map(String::trim)
            .collect(Collectors.toList());
    client = createAwsClient();
  }

  @Override
  public Record<GenericObject> process(GenericObject input, Context context) throws Exception {
    try {
      Record<GenericObject> record = (Record<GenericObject>) context.getCurrentRecord();
      InvokeResult result = invoke(record);

      if (result != null
          && result.getStatusCode() < 300
          && result.getStatusCode() >= 200
          && result.getFunctionError() == null) {
        ByteBuffer payload = result.getPayload();
        byte[] arr = new byte[payload.remaining()];
        payload.get(arr);

        if (logger.isDebugEnabled()) {
          logger.debug(
              "lambda function {} invocation successful with message {}",
              config.getLambdaFunctionName(),
              record);
        }
        JsonRecord<?, ?> jsonRecord = MAPPER.readValue(arr, JsonRecord.class);

        if (jsonRecord.getValue() == null) {
          return null;
        }

        Schema outputSchema;
        Object outputValue;
        if (SchemaType.KEY_VALUE.equals(jsonRecord.getSchemaType())) {
          KeyValueJsonRecord<?, ?> keyValue = (KeyValueJsonRecord<?, ?>) jsonRecord;
          Schema<?> keyOutputSchema = getSchema(keyValue.getKey());
          Schema<?> valueOutputSchema = getSchema(keyValue.getValue());
          if (keyValue.getKeyValueEncodingType() != null) {
            outputSchema =
                Schema.KeyValue(
                    keyOutputSchema, valueOutputSchema, keyValue.getKeyValueEncodingType());
          } else {
            outputSchema = Schema.KeyValue(keyOutputSchema, valueOutputSchema);
          }
          outputValue =
              new KeyValue<>(keyValue.getKey().getValue(), keyValue.getValue().getValue());
        } else {
          outputSchema = getSchema(jsonRecord);
          outputValue = jsonRecord.getValue();
        }

        FunctionRecord.FunctionRecordBuilder<GenericObject> builder =
            context.newOutputRecordBuilder(outputSchema).value(outputValue);

        if (jsonRecord instanceof StringKeyJsonRecord) {
          builder.key((String) jsonRecord.getKey());
        }

        if (jsonRecord.getDestinationTopic() != null) {
          builder.destinationTopic(jsonRecord.getDestinationTopic());
        }

        if (jsonRecord.getEventTime() != null) {
          builder.eventTime(jsonRecord.getEventTime());
        }

        if (jsonRecord.getProperties() != null) {
          builder.properties(jsonRecord.getProperties());
        }

        if (jsonRecord.getTopicName() != null) {
          builder.topicName(jsonRecord.getTopicName());
        }

        if (jsonRecord.getPartitionId() != null) {
          builder.partitionId(jsonRecord.getPartitionId());
        }

        if (jsonRecord.getPartitionIndex() != null) {
          builder.partitionIndex(jsonRecord.getPartitionIndex());
        }

        if (jsonRecord.getRecordSequence() != null) {
          builder.recordSequence(jsonRecord.getRecordSequence());
        }

        return builder.build();
      } else {
        logger.error(
            "failed to send message to AWS Lambda function {}.", config.getLambdaFunctionName());
        throw new IOException(
            "failed to send message to AWS Lambda function " + config.getLambdaFunctionName());
      }
    } catch (Exception e) {
      logger.error(
          "failed to process AWS Lambda function {} with error {}.",
          config.getLambdaFunctionName(),
          e);
      throw e;
    }
  }

  public InvokeResult invoke(Record<GenericObject> record)
      throws IOException, ExecutionException, InterruptedException, TimeoutException {

    byte[] payload = convertToLambdaPayload(record);

    InvokeRequest request =
        new InvokeRequest()
            .withInvocationType(InvocationType.RequestResponse)
            .withFunctionName(config.getLambdaFunctionName())
            .withPayload(ByteBuffer.wrap(payload));

    Future<InvokeResult> futureResult = client.invokeAsync(request);
    try {
      return futureResult.get(DEFAULT_INVOKE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    } catch (RequestTooLargeException e) {
      if (payload.length > MAX_PAYLOAD_SIZE_BYTES) {
        logger.error(
            "record payload size {} exceeds the max payload "
                + "size for synchronous lambda function invocation.",
            payload.length);
      }
      throw e;
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      logger.error(e.getLocalizedMessage(), e);
      throw e;
    }
  }

  public byte[] convertToLambdaPayload(Record<GenericObject> record) throws IOException {
    JsonRecord<?, ?> payload =
        getJsonRecord(
            record.getSchema(),
            record.getValue().getNativeObject(),
            record.getKey().orElse(null),
            "");
    if (!excludedFields.contains("topicName")) {
      record.getTopicName().ifPresent(payload::setTopicName);
    }
    if (!excludedFields.contains("destinationTopic")) {
      record.getDestinationTopic().ifPresent(payload::setDestinationTopic);
    }
    if (!excludedFields.contains("eventTime")) {
      record.getEventTime().ifPresent(payload::setEventTime);
    }
    if (!excludedFields.contains("partitionId")) {
      record.getPartitionId().ifPresent(payload::setPartitionId);
    }
    if (!excludedFields.contains("partitionIndex")) {
      record.getPartitionIndex().ifPresent(payload::setPartitionIndex);
    }
    if (!excludedFields.contains("recordSequence")) {
      record.getRecordSequence().ifPresent(payload::setRecordSequence);
    }
    if (!excludedFields.contains("properties") && record.getProperties() != null) {
      payload.setProperties(new HashMap<>(record.getProperties()));
    }

    return MAPPER.writeValueAsBytes(payload);
  }

  private JsonRecord<?, ?> getJsonRecord(Schema<?> schema, Object value, String key, String prefix)
      throws IOException {
    JsonRecord<?, ?> jsonRecord;
    switch (schema.getSchemaInfo().getType()) {
      case STRING:
        jsonRecord = new StringJsonRecord();
        ((StringJsonRecord) jsonRecord).setValue((String) value);
        break;
      case INT8:
        jsonRecord = new ByteJsonRecord();
        ((ByteJsonRecord) jsonRecord).setValue((Byte) value);
        break;
      case INT16:
        jsonRecord = new ShortJsonRecord();
        ((ShortJsonRecord) jsonRecord).setValue((Short) value);
        break;
      case INT32:
        jsonRecord = new IntegerJsonRecord();
        ((IntegerJsonRecord) jsonRecord).setValue((Integer) value);
        break;
      case INT64:
        jsonRecord = new LongJsonRecord();
        ((LongJsonRecord) jsonRecord).setValue((Long) value);
        break;
      case FLOAT:
        jsonRecord = new FloatJsonRecord();
        ((FloatJsonRecord) jsonRecord).setValue((Float) value);
        break;
      case DOUBLE:
        jsonRecord = new DoubleJsonRecord();
        ((DoubleJsonRecord) jsonRecord).setValue((Double) value);
        break;
      case BOOLEAN:
        jsonRecord = new BooleanJsonRecord();
        ((BooleanJsonRecord) jsonRecord).setValue((Boolean) value);
        break;
      case DATE:
        jsonRecord = new DateJsonRecord();
        ((DateJsonRecord) jsonRecord).setValue((Date) value);
        break;
      case TIME:
        jsonRecord = new TimeJsonRecord();
        ((TimeJsonRecord) jsonRecord).setValue((Time) value);
        break;
      case TIMESTAMP:
        jsonRecord = new TimestampJsonRecord();
        ((TimestampJsonRecord) jsonRecord).setValue((Timestamp) value);
        break;
      case INSTANT:
        jsonRecord = new InstantJsonRecord();
        ((InstantJsonRecord) jsonRecord).setValue((Instant) value);
        break;
      case LOCAL_DATE:
        jsonRecord = new LocalDateJsonRecord();
        ((LocalDateJsonRecord) jsonRecord).setValue((LocalDate) value);
        break;
      case LOCAL_TIME:
        jsonRecord = new LocalTimeJsonRecord();
        ((LocalTimeJsonRecord) jsonRecord).setValue((LocalTime) value);
        break;
      case LOCAL_DATE_TIME:
        jsonRecord = new LocalDateTimeJsonRecord();
        ((LocalDateTimeJsonRecord) jsonRecord).setValue((LocalDateTime) value);
        break;
      case BYTES:
        jsonRecord = new BytesJsonRecord();
        ((BytesJsonRecord) jsonRecord).setValue(((byte[]) value));
        break;
      case JSON:
        jsonRecord = new JsonJsonRecord();
        ((JsonJsonRecord) jsonRecord).setValue(((JsonNode) value));

        if (!excludedFields.contains(prefix + "schema")) {
          jsonRecord.setSchema(schema.getSchemaInfo().getSchema());
        }
        break;
      case AVRO:
        jsonRecord = new BytesJsonRecord();
        org.apache.avro.generic.GenericRecord avroRecord =
            (org.apache.avro.generic.GenericRecord) value;
        ((BytesJsonRecord) jsonRecord).setValue(serializeAvro(avroRecord));

        if (!excludedFields.contains(prefix + "schema")) {
          jsonRecord.setSchema(schema.getSchemaInfo().getSchema());
        }
        break;
      case KEY_VALUE:
        KeyValue<?, ?> keyValue = (KeyValue<?, ?>) value;
        KeyValueSchema<?, ?> keyValueSchema = (KeyValueSchema<?, ?>) schema;
        Object kvKey =
            keyValueSchema.getKeySchema().getSchemaInfo().getType().isStruct()
                ? ((GenericObject) keyValue.getKey()).getNativeObject()
                : keyValue.getKey();
        JsonRecord<?, ?> keyJsonRecord =
            getJsonRecord(keyValueSchema.getKeySchema(), kvKey, null, "key.");
        Object kvValue =
            keyValueSchema.getValueSchema().getSchemaInfo().getType().isStruct()
                ? ((GenericObject) keyValue.getValue()).getNativeObject()
                : keyValue.getValue();
        JsonRecord<?, ?> valueJsonRecord =
            getJsonRecord(keyValueSchema.getValueSchema(), kvValue, null, "value.");
        KeyValueJsonRecord<JsonRecord<?, ?>, JsonRecord<?, ?>> keyValueJsonRecord =
            new KeyValueJsonRecord<>();
        keyValueJsonRecord.setValue(valueJsonRecord);
        if (!excludedFields.contains("key")) {
          keyValueJsonRecord.setKey(keyJsonRecord);
        }
        if (!excludedFields.contains("keyValueEncodingType")
            && !KeyValueEncodingType.INLINE.equals(keyValueSchema.getKeyValueEncodingType())) {
          keyValueJsonRecord.setKeyValueEncodingType(keyValueSchema.getKeyValueEncodingType());
        }
        jsonRecord = keyValueJsonRecord;
        break;
      default:
        throw new IllegalStateException("Unexpected value: " + schema.getSchemaInfo().getType());
    }
    if (excludedFields.contains(prefix + "value")) {
      jsonRecord.setValue(null);
    }
    if (!excludedFields.contains(prefix + "key") && jsonRecord instanceof StringKeyJsonRecord) {
      ((StringKeyJsonRecord<?>) jsonRecord).setKey(key);
    }
    if (!excludedFields.contains(prefix + "schemaType")) {
      jsonRecord.setSchemaType(schema.getSchemaInfo().getType());
    }
    return jsonRecord;
  }

  private static byte[] serializeAvro(org.apache.avro.generic.GenericRecord record)
      throws IOException {
    GenericDatumWriter<org.apache.avro.generic.GenericRecord> writer =
        new GenericDatumWriter<>(record.getSchema());
    ByteArrayOutputStream oo = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(oo, null);
    writer.write(record, encoder);
    return oo.toByteArray();
  }

  private Schema<?> getSchema(JsonRecord<?, ?> jsonRecord) throws IOException {
    if (jsonRecord.getSchemaType() == null) {
      throw new IOException("Missing Lambda response schema type");
    }
    switch (jsonRecord.getSchemaType()) {
      case AVRO:
        org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
        org.apache.avro.Schema schema =
            parser.parse(new String(jsonRecord.getSchema(), StandardCharsets.UTF_8));
        return Schema.NATIVE_AVRO(schema);
      case JSON:
        return new JsonNodeSchema(jsonRecord.getSchema());
      case INT8:
        return Schema.INT8;
      case INT16:
        return Schema.INT16;
      case INT32:
        return Schema.INT32;
      case INT64:
        return Schema.INT64;
      case FLOAT:
        return Schema.FLOAT;
      case DOUBLE:
        return Schema.DOUBLE;
      case INSTANT:
        return Schema.INSTANT;
      case TIMESTAMP:
        return Schema.TIMESTAMP;
      case DATE:
        return Schema.DATE;
      case TIME:
        return Schema.TIME;
      case LOCAL_DATE:
        return Schema.LOCAL_DATE;
      case BOOLEAN:
        return Schema.BOOL;
      case LOCAL_TIME:
        return Schema.LOCAL_TIME;
      case LOCAL_DATE_TIME:
        return Schema.LOCAL_DATE_TIME;
      case STRING:
        return Schema.STRING;
      case BYTES:
        return Schema.BYTES;
      default:
        throw new IOException(
            String.format(
                "Function output schema type %s not supported", jsonRecord.getSchemaType()));
    }
  }

  AWSLambdaAsync createAwsClient() {
    AwsCredentialProviderPlugin credentialsProvider =
        createCredentialProvider(
            config.getAwsCredentialPluginName(), config.getAwsCredentialPluginParam());

    AWSLambdaAsyncClientBuilder builder = AWSLambdaAsyncClient.asyncBuilder();

    String region = config.getAwsRegion();
    if (region.isEmpty()) {
      try {
        Arn arn = Arn.fromString(config.getLambdaFunctionName());
        region = arn.getRegion();
      } catch (IllegalArgumentException e) {
        if (logger.isDebugEnabled()) {
          logger.debug("Region not provided and lambdaFunctionName cannot be parsed to an ARN", e);
        }
      }
    }

    if (!config.getAwsEndpoint().isEmpty()) {
      builder.setEndpointConfiguration(
          new AwsClientBuilder.EndpointConfiguration(config.getAwsEndpoint(), region));
    } else if (!region.isEmpty()) {
      builder.setRegion(region);
    }
    builder.setCredentials(credentialsProvider.getCredentialProvider());
    return builder.build();
  }
}
