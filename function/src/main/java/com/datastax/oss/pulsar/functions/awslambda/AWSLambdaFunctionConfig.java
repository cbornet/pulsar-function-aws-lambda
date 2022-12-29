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
package com.datastax.oss.pulsar.functions.awslambda; /*
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

import java.io.Serializable;
import lombok.Data;
import org.apache.pulsar.io.core.annotations.FieldDoc;

/** The configuration class for {@link AWSLambdaFunction}. */
@Data
public class AWSLambdaFunctionConfig implements Serializable {
  private static final long serialVersionUID = 1L;

  @FieldDoc(
    defaultValue = "",
    help =
        "AWS Lambda end-point url. It can be found "
            + "at https://docs.aws.amazon.com/general/latest/gr/rande.html"
  )
  private String awsEndpoint = "";

  @FieldDoc(
    required = true,
    defaultValue = "",
    help = "Appropriate aws region. E.g. us-west-1, us-west-2"
  )
  private String awsRegion;

  @FieldDoc(
    required = true,
    defaultValue = "",
    help = "The lambda function name should be invoked."
  )
  private String lambdaFunctionName;

  @FieldDoc(
    defaultValue = "",
    help =
        "Fully-Qualified class name of implementation of AwsCredentialProviderPlugin."
            + " It is a factory class which creates an AWSCredentialsProvider that will be used by aws client."
            + " If it is empty then aws client will create a default AWSCredentialsProvider which accepts json"
            + " of credentials in `awsCredentialPluginParam`"
  )
  private String awsCredentialPluginName = "";

  @FieldDoc(
    defaultValue = "",
    sensitive = true,
    help = "json-parameters to initialize `AwsCredentialsProviderPlugin`"
  )
  private String awsCredentialPluginParam = "";

  @FieldDoc(
    required = true,
    defaultValue = "true",
    help = "Invoke a lambda function synchronously, false to invoke asynchronously."
  )
  private boolean synchronousInvocation = true;
}
