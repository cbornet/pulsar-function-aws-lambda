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
        "The AWS Lambda endpoint URL. If left blank, the default AWS Lambda endpoint URL will be used."
  )
  private String awsEndpoint = "";

  @FieldDoc(
    required = true,
    defaultValue = "",
    help =
        "The AWS region (eg. us-west-1, us-west-2). If left blank, the region will be parsed from the lambdaFunctionName if it's in the full ARN format."
  )
  private String awsRegion;

  @FieldDoc(
    required = true,
    defaultValue = "",
    help =
        "The name of the Lambda function, version, or alias.\n"
            + "*Name formats*\n"
            + "Function name - my-function (name-only), my-function:v1 (with alias).\n"
            + "Function ARN - arn:aws:lambda:us-west-2:123456789012:function:my-function.\n"
            + "Partial ARN - 123456789012:function:my-function."
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
    defaultValue = "",
    help =
        "Comma separated list of fields to exclude from the request done to the Lambda service. Nested fields are referenced using a point separator. Eg: value.key.value.schema"
  )
  private String excludedFields = "";
}
