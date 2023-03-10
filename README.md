# Pulsar Function AWS Lambda

The Pulsar Function AWS Lambda is a Pulsar Function that delegates execution to an AWS Lambda Function.
The intent is to provide an easy way to integrate with the AWS ecosystem to process the messages.
It also provides a way to implement functions in languages not supported by Pulsar Functions but supported by AWS Lambda.

The function requires Pulsar 2.11+ or Luna Streaming 2.10+ to run.

The function doesn't support PROTOBUF and PROTOBUF_NATIVE messages at the moment.

## Configuration

The Function must be configured with the following parameters:

| Name                       | Type   | Required | Default           | Description                                                                                                                                                                                                                                                                                                                     |
|----------------------------|--------|----------|-------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `awsEndpoint`              | String | false    | "" (empty string) | The AWS Lambda endpoint URL. If left blank, the default AWS Lambda endpoint URL will be used.                                                                                                                                                                                                                                   |
| `awsRegion`                | String | false    | "" (empty string) | The AWS region (eg. `us-west-1`, `us-west-2`). If left blank, the region will be parsed from the `lambdaFunctionName` if it's in the full ARN format.                                                                                                                                                                           |
| `awsCredentialPluginName`  | String | false    | "" (empty string) | Fully-qualified class name of implementation of `AwsCredentialProviderPlugin`.                                                                                                                                                                                                                                                  |
| `awsCredentialPluginParam` | String | true     | "" (empty string) | JSON parameter to initialize `AwsCredentialsProviderPlugin`.                                                                                                                                                                                                                                                                    |
| `lambdaFunctionName`       | String | true     | "" (empty string) | The name of the Lambda function, version, or alias. <br>**Name formats**<ul><li>**Function name** - `my-function` (name-only), `my-function:v1` (with alias).</li><li>**Function ARN** - `arn:aws:lambda:us-west-2:123456789012:function:my-function`.</li><li>**Partial ARN** - `123456789012:function:my-function`.</li></ul> |
| `excludedFields`           | String | false    | "" (empty string) | Comma separated list of fields to exclude from the request done to the Lambda service. Nested fields are referenced using a point separator. Eg: `topicName, key.value.schema`                                                                                                                                                  |

## Deployment

See [the Pulsar docs](https://pulsar.apache.org/fr/docs/functions-deploy) for more details on how to deploy a Function.

### Deploy as a non built-in Function

* Create a Pulsar Function AWS LAMBDA providing the path to the Pulsar Transformations NAR.
```shell
pulsar-admin functions create \
--jar pulsar-function-aws-lambda-1.0.0.nar \
--name my-function \
--inputs my-input-topic \
--output my-output-topic \
--user-config '{"awsEndpoint": "https://lambda.eu-central-1.amazonaws.com", "awsRegion": "eu-central-1", "lambdaFunctionName": "my-function", "awsCredentialPluginParam": "{\"accessKey\":\"myKey\",\"secretKey\":\"my-Secret\"}"}'
```

### Deploy as a built-in Function

* Put the Pulsar Transformations NAR in the `functions` directory of the Pulsar Function worker (or broker).
```shell
cp pulsar-function-aws-lambda-1.0.0.nar $PULSAR_HOME/functions/pulsar-function-aws-lambda.nar
```
* Restart the function worker (or broker) instance or reload all the built-in functions:
```shell
pulsar-admin functions reload
```
* Create a Transformation Function with the admin CLI. The built-in function type is `transforms`.
```shell
pulsar-admin functions create \
--function-type awslambda \
--name my-function \
--inputs my-input-topic \
--output my-output-topic \
--user-config '{"awsEndpoint": "https://lambda.eu-central-1.amazonaws.com", "awsRegion": "eu-central-1", "lambdaFunctionName": "my-function", "awsCredentialPluginParam": "{\"accessKey\":\"myKey\",\"secretKey\":\"my-Secret\"}"}'
```

## Developing the AWS Lambda function

For each message consumed from Pulsar, the Lambda function will be invoked with a JSON payload that has a structure like:
```json
{
  "value" : <value>,
  "schemaType" : <SchemaType>, // eg. STRING, INT32, AVRO, ...,
  "schema" : <schema>,
  "key" : "my-key",
  "topicName" : "my-topic",
  "partitionId" : "my-partition-id",
  "partitionIndex" : 100,
  "recordSequence" : 100,
  "destinationTopic" : "my-destination-topic",
  "eventTime" : 100,
  "properties" : {
    "my-prop" : "my-prop-value"
  }
}
```

Fields that have a `null` value may be omitted from the JSON payload.
The Lambda function shall return a JSON with the same structure.
The returned JSON will be used to send the response message to Pulsar.
Non-null values of the response will override the default values of the response message.
If `value` is `null` in the response, no message will be sent to the destination topic.

### Values and schema

The value field depends on the schema type.

#### Primitive types

For primitive types, the value is the JSON representation of the Java type.
Here are some example Lambda payloads for the primitive schema types:

```json
{"schemaType":"BYTES","value":"Kg=="}
{"schemaType":"STRING","value":"test"}
{"schemaType":"INT8","value":42}
{"schemaType":"INT16","value":42}
{"schemaType":"INT32","value":42}
{"schemaType":"INT64","value":42}
{"schemaType":"FLOAT","value":42.0}
{"schemaType":"DOUBLE","value":42.0}
{"schemaType":"BOOLEAN","value":true}
{"schemaType":"DATE","value":"1970-01-01T00:00:00.100+00:00"}
{"schemaType":"TIME","value":"01:01:40"}
{"schemaType":"TIMESTAMP","value":"1970-01-01T00:01:40.000+00:00"}
{"schemaType":"INSTANT","value":"1970-01-01T00:00:00.100Z"}
{"schemaType":"LOCAL_DATE","value":"1970-04-11"}
{"schemaType":"LOCAL_TIME","value":"00:01:40"}
{"schemaType":"LOCAL_DATE_TIME","value":"1970-01-01T00:01:40"}
```

#### AVRO type

For AVRO schema types, the AVRO value is serialized to bytes and then sent as a Base64 string in the JSON payload.
The AVRO schema is also sent in the payload as a Base64 binary string.
Here is an example of a Lambda payload with an AVRO value

```json
{
  "schemaType": "AVRO",
  "schema": "eyJ0eXBlIjoicmVjb3JkIiwibmFtZSI6InJlY29yZCIsImZpZWxkcyI6W3sibmFtZSI6ImZpcnN0TmFtZSIsInR5cGUiOiJzdHJpbmcifV19", 
  "value": "CEphbmU="
}
```

#### JSON type

For JSON schema types, the JSON value is embedded in the JSON payload.
The native AVRO schema is also sent in the payload as a Base64 binary string.

```json
{
  "schemaType": "JSON",
  "schema": "eyJ0eXBlIjoicmVjb3JkIiwibmFtZSI6InJlY29yZCIsImZpZWxkcyI6W3sibmFtZSI6ImZpcnN0TmFtZSIsInR5cGUiOiJzdHJpbmcifV19",
  "value": {
    "firstName": "Jane"
  }
}
```

#### KeyValue type

For KeyValue types, the key and value are structures with the fields `value`, `schemaType` and `schema`.
The encoding type (`INLINE` or `SEPARATED`) is passed in the `keyValueEncodingType` field. 
A null `keyValueEncodingType` corresponds to an `INLINE` encoding.

```json
{
  "schemaType": "KEY_VALUE",
  "key": {
    "schemaType":"STRING",
    "value":"my-key"
  },
  "value": {
    "schemaType": "INT32",
    "value": 42
  },
  "keyValueEncodingType":"SEPARATED"
}
```