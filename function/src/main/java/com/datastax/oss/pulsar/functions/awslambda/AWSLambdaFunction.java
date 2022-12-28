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
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
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
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.KeyValueSchema;
import org.apache.pulsar.common.schema.KeyValue;
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
  private static final int MAX_SYNC_PAYLOAD_SIZE_BYTES = (6 * 1024 * 1024);
  private static final int MAX_ASYNC_PAYLOAD_SIZE_BYTES = (256 * 1024);

  private static final ObjectMapper MAPPER =
      new ObjectMapper()
          .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
          .configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true)
          .setSerializationInclusion(JsonInclude.Include.NON_NULL)
          .registerModule(new JavaTimeModule())
          .registerModule(new Jdk8Module());

  private AWSLambdaAsync client;
  private AWSLambdaFunctionConfig config;

  private Logger logger;

  @Override
  public void initialize(Context context) throws Exception {
    logger = context.getLogger();
    config =
        MAPPER.readValue(
            MAPPER.writeValueAsString(context.getUserConfigMap()), AWSLambdaFunctionConfig.class);
    client = createAwsClient();
  }

  @Override
  public Record<GenericObject> process(GenericObject input, Context context) throws Exception {
    try {
      Record<GenericObject> record = (Record<GenericObject>) context.getCurrentRecord();
      InvokeResult result = invoke(record);

      if (result != null && result.getStatusCode() < 300 && result.getStatusCode() >= 200) {
        ByteBuffer payload = result.getPayload();
        byte[] arr = new byte[payload.remaining()];
        payload.get(arr);

        if (logger.isDebugEnabled()) {
          if (config.isSynchronousInvocation()) {
            logger.debug(
                "lambda function {} invocation successful with message {} " + "and response {}",
                config.getLambdaFunctionName(),
                record,
                result);
          } else {
            logger.debug(
                "lambda function {} invocation successful with message {}",
                config.getLambdaFunctionName(),
                record);
          }
        }
        JsonRecord jsonRecord = MAPPER.readValue(arr, JsonRecord.class);

        if (jsonRecord.getValue() == null) {
          throw new IllegalStateException("Missing Lambda response value");
        }

        Schema outputSchema;
        Object outputValue;
        if (SchemaType.KEY_VALUE.equals(jsonRecord.getValue().getSchemaType())) {
          KeyValueTypedValue keyValue = (KeyValueTypedValue) jsonRecord.getValue();
          Schema<?> keyOutputSchema = getSchema(keyValue.getKey());
          Schema<?> valueOutputSchema = getSchema(keyValue.getValue());
          if (jsonRecord.getKeyValueEncodingType() != null) {
            outputSchema =
                Schema.KeyValue(
                    keyOutputSchema, valueOutputSchema, jsonRecord.getKeyValueEncodingType());
          } else {
            outputSchema = Schema.KeyValue(keyOutputSchema, valueOutputSchema);
          }
          outputValue =
              new KeyValue<>(keyValue.getKey().getValue(), keyValue.getValue().getValue());
        } else {
          outputSchema = getSchema(jsonRecord.getValue());
          outputValue = jsonRecord.getValue().getValue();
        }

        FunctionRecord.FunctionRecordBuilder<GenericObject> builder =
            context.newOutputRecordBuilder(outputSchema).value(outputValue);

        if (jsonRecord.getKey() != null) {
          builder.key(jsonRecord.getKey());
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
    InvocationType type =
        config.isSynchronousInvocation() ? InvocationType.RequestResponse : InvocationType.Event;

    byte[] payload = convertToLambdaPayload(record);

    InvokeRequest request =
        new InvokeRequest()
            .withInvocationType(type)
            .withFunctionName(config.getLambdaFunctionName())
            .withPayload(ByteBuffer.wrap(payload));

    Future<InvokeResult> futureResult = client.invokeAsync(request);
    try {
      return futureResult.get(DEFAULT_INVOKE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    } catch (RequestTooLargeException e) {
      if (config.isSynchronousInvocation() && payload.length > MAX_SYNC_PAYLOAD_SIZE_BYTES) {
        logger.error(
            "record payload size {} exceeds the max payload "
                + "size for synchronous lambda function invocation.",
            payload.length);
      } else if (!config.isSynchronousInvocation()
          && payload.length > MAX_ASYNC_PAYLOAD_SIZE_BYTES) {
        logger.error(
            "record payload size {} exceeds the max payload "
                + "size for asynchronous lambda function invocation.",
            payload.length);
      }
      throw e;
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      logger.error(e.getLocalizedMessage(), e);
      throw e;
    }
  }

  public byte[] convertToLambdaPayload(Record<GenericObject> record) throws IOException {
    JsonRecord payload = new JsonRecord();

    payload.setValue(getTypedValue(record.getSchema(), record.getValue().getNativeObject()));
    record.getTopicName().ifPresent(payload::setTopicName);
    record.getKey().ifPresent(payload::setKey);
    record.getDestinationTopic().ifPresent(payload::setDestinationTopic);
    record.getEventTime().ifPresent(payload::setEventTime);
    record.getPartitionId().ifPresent(payload::setPartitionId);
    record.getPartitionIndex().ifPresent(payload::setPartitionIndex);
    record.getRecordSequence().ifPresent(payload::setRecordSequence);
    record.getTopicName().ifPresent(payload::setTopicName);

    if (record.getProperties() != null) {
      payload.setProperties(new HashMap<>(record.getProperties()));
    }

    if (record.getSchema() instanceof KeyValueSchema) {
      Schema<?> schema = record.getSchema();
      payload.setKeyValueEncodingType(((KeyValueSchema<?, ?>) schema).getKeyValueEncodingType());
    }

    return MAPPER.writeValueAsBytes(payload);
  }

  private TypedValue<?> getTypedValue(Schema<?> schema, Object value) throws IOException {
    TypedValue<?> typedValue;
    switch (schema.getSchemaInfo().getType()) {
      case STRING:
        typedValue = new StringTypedValue();
        ((StringTypedValue) typedValue).setValue((String) value);
        break;
      case INT8:
        typedValue = new ByteTypedValue();
        ((ByteTypedValue) typedValue).setValue((Byte) value);
        break;
      case INT16:
        typedValue = new ShortTypedValue();
        ((ShortTypedValue) typedValue).setValue((Short) value);
        break;
      case INT32:
        typedValue = new IntegerTypedValue();
        ((IntegerTypedValue) typedValue).setValue((Integer) value);
        break;
      case INT64:
        typedValue = new LongTypedValue();
        ((LongTypedValue) typedValue).setValue((Long) value);
        break;
      case FLOAT:
        typedValue = new FloatTypedValue();
        ((FloatTypedValue) typedValue).setValue((Float) value);
        break;
      case DOUBLE:
        typedValue = new DoubleTypedValue();
        ((DoubleTypedValue) typedValue).setValue((Double) value);
        break;
      case BOOLEAN:
        typedValue = new BooleanTypedValue();
        ((BooleanTypedValue) typedValue).setValue((Boolean) value);
        break;
      case DATE:
        typedValue = new DateTypedValue();
        ((DateTypedValue) typedValue).setValue((Date) value);
        break;
      case TIME:
        typedValue = new TimeTypedValue();
        ((TimeTypedValue) typedValue).setValue((Time) value);
        break;
      case TIMESTAMP:
        typedValue = new TimestampTypedValue();
        ((TimestampTypedValue) typedValue).setValue((Timestamp) value);
        break;
      case INSTANT:
        typedValue = new InstantTypedValue();
        ((InstantTypedValue) typedValue).setValue((Instant) value);
        break;
      case LOCAL_DATE:
        typedValue = new LocalDateTypedValue();
        ((LocalDateTypedValue) typedValue).setValue((LocalDate) value);
        break;
      case LOCAL_TIME:
        typedValue = new LocalTimeTypedValue();
        ((LocalTimeTypedValue) typedValue).setValue((LocalTime) value);
        break;
      case LOCAL_DATE_TIME:
        typedValue = new LocalDateTimeTypedValue();
        ((LocalDateTimeTypedValue) typedValue).setValue((LocalDateTime) value);
        break;
      case BYTES:
        typedValue = new BytesTypedValue();
        ((BytesTypedValue) typedValue).setValue(((byte[]) value));
        break;
      case JSON:
        typedValue = new JsonTypedValue();
        ((JsonTypedValue) typedValue).setValue(((JsonNode) value));
        typedValue.setSchema(schema.getSchemaInfo().getSchema());
        break;
      case AVRO:
        typedValue = new BytesTypedValue();
        org.apache.avro.generic.GenericRecord avroRecord =
            (org.apache.avro.generic.GenericRecord) value;
        ((BytesTypedValue) typedValue).setValue(serializeAvro(avroRecord));
        typedValue.setSchema(schema.getSchemaInfo().getSchema());
        break;
      case KEY_VALUE:
        typedValue = new KeyValueTypedValue();
        KeyValue<?, ?> keyValue = (KeyValue<?, ?>) value;
        KeyValueSchema<?, ?> keyValueSchema = (KeyValueSchema<?, ?>) schema;
        Object key =
            keyValueSchema.getKeySchema().getSchemaInfo().getType().isStruct()
                ? ((GenericObject) keyValue.getKey()).getNativeObject()
                : keyValue.getKey();
        TypedValue<?> keyTypedValue = getTypedValue(keyValueSchema.getKeySchema(), key);
        Object kvValue =
            keyValueSchema.getValueSchema().getSchemaInfo().getType().isStruct()
                ? ((GenericObject) keyValue.getValue()).getNativeObject()
                : keyValue.getValue();
        TypedValue<?> valueTypedValue = getTypedValue(keyValueSchema.getValueSchema(), kvValue);
        ((KeyValueTypedValue) typedValue).setKey(keyTypedValue);
        ((KeyValueTypedValue) typedValue).setValue(valueTypedValue);
        break;
      default:
        throw new IllegalStateException("Unexpected value: " + schema.getSchemaInfo().getType());
    }
    typedValue.setSchemaType(schema.getSchemaInfo().getType());
    return typedValue;
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

  private Schema<?> getSchema(TypedValue<?> typedValue) throws IOException {
    if (typedValue.getSchemaType() == null) {
      throw new IOException("Missing Lambda response schema type");
    }
    switch (typedValue.getSchemaType()) {
      case AVRO:
        org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
        org.apache.avro.Schema schema =
            parser.parse(new String(typedValue.getSchema(), StandardCharsets.UTF_8));
        return Schema.NATIVE_AVRO(schema);
      case JSON:
        return new JsonNodeSchema(typedValue.getSchema());
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
      case PROTOBUF:
      case BYTES:
        return Schema.BYTES;
      default:
        throw new IOException(
            String.format(
                "Function output schema type %s not supported", typedValue.getSchemaType()));
    }
  }

  AWSLambdaAsync createAwsClient() {
    AwsCredentialProviderPlugin credentialsProvider =
        createCredentialProvider(
            config.getAwsCredentialPluginName(), config.getAwsCredentialPluginParam());

    AWSLambdaAsyncClientBuilder builder = AWSLambdaAsyncClient.asyncBuilder();

    if (!config.getAwsEndpoint().isEmpty()) {
      builder.setEndpointConfiguration(
          new AwsClientBuilder.EndpointConfiguration(
              config.getAwsEndpoint(), config.getAwsRegion()));
    } else if (!config.getAwsRegion().isEmpty()) {
      builder.setRegion(config.getAwsRegion());
    }
    builder.setCredentials(credentialsProvider.getCredentialProvider());
    return builder.build();
  }
}
