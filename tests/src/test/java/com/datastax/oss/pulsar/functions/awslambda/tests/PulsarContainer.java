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
/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.pulsar.functions.awslambda.tests;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

public class PulsarContainer implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(PulsarContainer.class);

  protected static final String PULSAR_FUNCTION_NAR = "/pulsar-function-aws-lambda.nar";

  private GenericContainer<?> pulsarContainer;
  private final Network network;
  private final String image;

  public PulsarContainer(Network network, String image) {
    this.network = network;
    this.image = image;
  }

  public void start() {
    pulsarContainer =
        new org.testcontainers.containers.PulsarContainer(
                DockerImageName.parse(image).asCompatibleSubstituteFor("apachepulsar/pulsar"))
            .withNetwork(network)
            .withNetworkAliases("pulsar")
            .withFunctionsWorker()
            .withClasspathResourceMapping(
                PULSAR_FUNCTION_NAR, "/pulsar/functions" + PULSAR_FUNCTION_NAR, BindMode.READ_ONLY)
            .withLogConsumer(
                (f) -> {
                  String text = f.getUtf8String().trim();
                  log.info(text);
                });
    pulsarContainer.start();
  }

  @Override
  public void close() {
    if (pulsarContainer != null) {
      pulsarContainer.stop();
    }
  }

  public GenericContainer<?> getPulsarContainer() {
    return pulsarContainer;
  }
}
