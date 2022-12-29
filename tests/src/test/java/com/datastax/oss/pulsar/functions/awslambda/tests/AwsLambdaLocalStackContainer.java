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

import static org.testcontainers.containers.localstack.LocalStackContainer.Service.IAM;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.LAMBDA;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

public class AwsLambdaLocalStackContainer implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(AwsLambdaLocalStackContainer.class);

  private LocalStackContainer localstackContainer;
  private final Network network;

  public AwsLambdaLocalStackContainer(Network network) {
    this.network = network;
  }

  public void start() {
    localstackContainer =
        new LocalStackContainer(DockerImageName.parse("localstack/localstack:1.3.1"))
            .withNetwork(network)
            .withNetworkAliases("localstack")
            .withServices(LAMBDA, IAM)
            .withEnv("DEFAULT_REGION", "eu-central-1")
            .withClasspathResourceMapping(
                "function.zip", "/opt/code/localstack/function.zip", BindMode.READ_ONLY)
            .withClasspathResourceMapping(
                "init_localstack.sh",
                "/docker-entrypoint-initaws.d/init_localstack.sh",
                BindMode.READ_ONLY)
            .waitingFor(Wait.forLogMessage(".*Initialized lambda function\n", 1))
            .withLogConsumer(
                (f) -> {
                  String text = f.getUtf8String().trim();
                  log.info(text);
                });
    localstackContainer.start();
  }

  @Override
  public void close() {
    if (localstackContainer != null) {
      localstackContainer.stop();
    }
  }

  public LocalStackContainer getLocalstackContainer() {
    return localstackContainer;
  }
}
