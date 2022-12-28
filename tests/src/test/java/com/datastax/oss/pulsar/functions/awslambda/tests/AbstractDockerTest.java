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
package com.datastax.oss.pulsar.functions.awslambda.tests;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.Value;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.policies.data.FunctionStatus;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaType;
import org.testcontainers.containers.Network;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public abstract class AbstractDockerTest {

  private final String image;
  private Network network;
  private PulsarContainer pulsarContainer;
  private AwsLambdaLocalStackContainer localStackContainer;
  private PulsarAdmin admin;
  private PulsarClient client;

  AbstractDockerTest(String image) {
    this.image = image;
  }

  @BeforeClass
  public void setup() throws Exception {
    network = Network.newNetwork();
    localStackContainer = new AwsLambdaLocalStackContainer(network);
    localStackContainer.start();
    pulsarContainer = new PulsarContainer(network, image);
    // start Pulsar and wait for it to be ready to accept requests
    pulsarContainer.start();
    admin =
        PulsarAdmin.builder()
            .serviceHttpUrl(
                "http://localhost:" + pulsarContainer.getPulsarContainer().getMappedPort(8080))
            .build();
    client =
        PulsarClient.builder()
            .serviceUrl(
                "pulsar://localhost:" + pulsarContainer.getPulsarContainer().getMappedPort(6650))
            .build();
  }

  @AfterClass
  public void teardown() {
    if (client != null) {
      client.closeAsync();
    }
    if (admin != null) {
      admin.close();
    }
    if (pulsarContainer != null) {
      pulsarContainer.close();
    }
    if (localStackContainer != null) {
      localStackContainer.close();
    }
    if (network != null) {
      network.close();
    }
  }

  @Test
  public void testPrimitive() throws Exception {
    GenericRecord value = testEchoFunction(Schema.INT32, 42, "test-key");
    assertEquals(value.getSchemaType(), SchemaType.INT32);
    assertEquals(value.getNativeObject(), 42);
  }

  @Test
  public void testKVPrimitiveInline() throws Exception {
    GenericRecord value =
        testEchoFunction(
            Schema.KeyValue(Schema.STRING, Schema.STRING, KeyValueEncodingType.INLINE),
            new KeyValue<>("a", "b"),
            "test-key");
    assertEquals(value.getSchemaType(), SchemaType.KEY_VALUE);
    KeyValue<String, String> keyValue = (KeyValue<String, String>) value.getNativeObject();
    assertEquals(keyValue.getKey(), "a");
    assertEquals(keyValue.getValue(), "b");
  }

  @Test
  public void testKVPrimitiveSeparated() throws Exception {
    GenericRecord value =
        testEchoFunction(
            Schema.KeyValue(Schema.STRING, Schema.STRING, KeyValueEncodingType.SEPARATED),
            new KeyValue<>("a", "b"));
    assertEquals(value.getSchemaType(), SchemaType.KEY_VALUE);
    KeyValue<String, String> keyValue = (KeyValue<String, String>) value.getNativeObject();
    assertEquals(keyValue.getKey(), "a");
    assertEquals(keyValue.getValue(), "b");
  }

  @Test
  public void testAvro() throws Exception {
    GenericRecord value = testEchoFunction(Schema.AVRO(Pojo1.class), new Pojo1("a", "b"));
    assertEquals(value.getSchemaType(), SchemaType.AVRO);
    assertTrue(value.getNativeObject() instanceof org.apache.avro.generic.GenericRecord);
    assertEquals(value.getFields().size(), 2);
    assertEquals(value.getField("a"), "a");
    assertEquals(value.getField("b"), "b");
  }

  @Test
  public void testKVAvro() throws Exception {
    GenericRecord value =
        testEchoFunction(
            Schema.KeyValue(Schema.AVRO(Pojo1.class), Schema.AVRO(Pojo2.class)),
            new KeyValue<>(new Pojo1("a", "b"), new Pojo2("c", "d")));
    assertEquals(value.getSchemaType(), SchemaType.KEY_VALUE);
    KeyValue<GenericRecord, GenericRecord> keyValue =
        (KeyValue<GenericRecord, GenericRecord>) value.getNativeObject();

    GenericRecord key = keyValue.getKey();
    assertEquals(key.getSchemaType(), SchemaType.AVRO);
    assertTrue(key.getNativeObject() instanceof org.apache.avro.generic.GenericRecord);
    assertEquals(key.getFields().size(), 2);
    assertEquals(key.getField("a"), "a");
    assertEquals(key.getField("b"), "b");

    GenericRecord val = keyValue.getValue();
    assertEquals(val.getSchemaType(), SchemaType.AVRO);
    assertTrue(key.getNativeObject() instanceof org.apache.avro.generic.GenericRecord);
    assertEquals(val.getFields().size(), 2);
    assertEquals(val.getField("c"), "c");
    assertEquals(val.getField("d"), "d");
  }

  @Test
  public void testJson() throws Exception {
    GenericRecord value = testEchoFunction(Schema.JSON(Pojo1.class), new Pojo1("a", "b"));
    assertEquals(value.getSchemaType(), SchemaType.JSON);
    assertTrue(
        value.getNativeObject()
            instanceof org.apache.pulsar.shade.com.fasterxml.jackson.databind.JsonNode);
    assertEquals(value.getFields().size(), 2);
    assertEquals(value.getField("a"), "a");
    assertEquals(value.getField("b"), "b");
  }

  private <T> GenericRecord testEchoFunction(Schema<T> schema, T value)
      throws PulsarAdminException, InterruptedException, PulsarClientException {
    return testEchoFunction(schema, value, null);
  }

  private <T> GenericRecord testEchoFunction(Schema<T> schema, T value, String key)
      throws PulsarAdminException, InterruptedException, PulsarClientException {
    String userConfig =
        ("{'awsEndpoint': 'http://localstack:4566', 'awsRegion': 'eu-central-1', 'lambdaFunctionName': 'echo', 'awsCredentialPluginParam': '{\\'accessKey\\':\\'myKey\\',\\'secretKey\\':\\'my-Secret\\'}'}")
            .replace("'", "\"");
    String functionId = UUID.randomUUID().toString();
    String inputTopic = "input-" + functionId;
    String outputTopic = "output-" + functionId;
    String functionName = "function-" + functionId;

    admin
        .topics()
        .createSubscription(
            inputTopic, String.format("public/default/%s", functionName), MessageId.latest);

    FunctionConfig functionConfig =
        FunctionConfig.builder()
            .tenant("public")
            .namespace("default")
            .name(functionName)
            .inputs(Collections.singletonList(inputTopic))
            .output(outputTopic)
            .jar("builtin://aws-lambda")
            .runtime(FunctionConfig.Runtime.JAVA)
            .userConfig(
                new Gson().fromJson(userConfig, new TypeToken<Map<String, Object>>() {}.getType()))
            .build();

    admin.functions().createFunction(functionConfig, null);

    FunctionStatus functionStatus = null;
    for (int i = 0; i < 300; i++) {
      functionStatus = admin.functions().getFunctionStatus("public", "default", functionName);
      if (functionStatus.getNumRunning() == 1) {
        break;
      }
      Thread.sleep(100);
    }

    if (functionStatus.getNumRunning() != 1) {
      fail("Function didn't start in time");
    }

    Consumer<GenericRecord> consumer =
        client
            .newConsumer(Schema.AUTO_CONSUME())
            .topic(outputTopic)
            .subscriptionName(UUID.randomUUID().toString())
            .subscribe();

    Producer producer = client.newProducer(schema).topic(inputTopic).create();

    TypedMessageBuilder producerMessage =
        producer.newMessage().value(value).property("prop-key", "prop-value");
    if (key != null) {
      producerMessage.key(key);
    }
    producerMessage.send();

    Message<GenericRecord> message = consumer.receive(30, TimeUnit.SECONDS);
    assertNotNull(message);
    if (key != null) {
      assertEquals(message.getKey(), key);
    }
    assertEquals(message.getProperty("prop-key"), "prop-value");

    GenericRecord messageValue = message.getValue();
    assertNotNull(messageValue);
    return messageValue;
  }

  @Value
  private static class Pojo1 {
    String a;
    String b;
  }

  @Value
  private static class Pojo2 {
    String c;
    String d;
  }
}
