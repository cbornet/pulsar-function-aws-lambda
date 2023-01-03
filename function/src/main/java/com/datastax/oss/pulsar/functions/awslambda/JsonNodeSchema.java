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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

public class JsonNodeSchema implements Schema<JsonNode> {
  private static final org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();

  private static final ObjectMapper mapper = new ObjectMapper();
  private final org.apache.avro.Schema nativeSchema;

  private final SchemaInfo schemaInfo;

  public JsonNodeSchema(byte[] schema) {
    nativeSchema = parser.parse(new String(schema, StandardCharsets.UTF_8));
    schemaInfo = SchemaInfo.builder().schema(schema).name("").type(SchemaType.JSON).build();
  }

  @Override
  public byte[] encode(JsonNode message) {
    try {
      return mapper.writeValueAsBytes(message);
    } catch (JsonProcessingException e) {
      throw new SchemaSerializationException(e);
    }
  }

  @Override
  public JsonNode decode(byte[] bytes) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SchemaInfo getSchemaInfo() {
    return schemaInfo;
  }

  @Override
  public Optional<Object> getNativeSchema() {
    return Optional.of(this.nativeSchema);
  }

  @Override
  public Schema<JsonNode> clone() {
    return new JsonNodeSchema(schemaInfo.getSchema());
  }
}
