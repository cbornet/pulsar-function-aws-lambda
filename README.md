# Pulsar Function AWS Lambda

The Pulsar Function AWS Lambda is a Pulsar Function that delegates execution to an AWS Lambda Function.
The intent is to provide an easy way to integrate with the AWS ecosystem to process the messages.
It also provides a way to implement functions in languages not supported by Pulsar Functions but supported by AWS Lambda.

The function requires Pulsar 2.11+ or Luna Streaming 2.10+ to run.
The function doesn't support PROTOBUF and PROTOBUF_NATIVE messages at the moment.

## Configuration

The Function must be configured with the following parameters:

| Name | Type|Required | Default | Description
|------|----------|----------|---------|-------------|
| `awsEndpoint` |String| false | " " (empty string) | AWS Lambda end-point URL. It can be found at [here](https://docs.aws.amazon.com/general/latest/gr/lambda-service.html). |
| `awsRegion` | String| true | " " (empty string) | Supported AWS region. For example, us-west-1, us-west-2. |
| `awsCredentialPluginName` | String|false | " " (empty string) | Fully-qualified class name of implementation of `AwsCredentialProviderPlugin`. |
| `awsCredentialPluginParam` | String|true | " " (empty string) | JSON parameter to initialize `AwsCredentialsProviderPlugin`. |
| `lambdaFunctionName` | String|true | " " (empty string) | The Lambda function that should be invoked by the messages. |
| `synchronousInvocation` | Boolean|true | true | `true` means invoking a Lambda function synchronously. <br>`false` means invoking a Lambda function asynchronously. |


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
  "value" : {
    "schemaType" : <SchemaType>, // eg. STRING, INT32, AVRO, ...
    "schema" : <schema>,
    "value" : <value object>
  }
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
The Lambda function shall return a JSON with the same structure. The returned JSON will be used to send the response message to Pulsar. Non-null values of the response will override the default values of the response message. The `value` field is required. 

### Value object

The value object structure depends on the schema type.

#### Primitive types

For primitive types, the value is the JSON representation of the Java type.
Here are some example Lambda payloads for the primitive schema types:

```json
{"value":{"schemaType":"BYTES","value":"Kg=="}}
{"value":{"schemaType":"STRING","value":"test"}}
{"value":{"schemaType":"INT8","value":42}}
{"value":{"schemaType":"INT16","value":42}}
{"value":{"schemaType":"INT32","value":42}}
{"value":{"schemaType":"INT64","value":42}}
{"value":{"schemaType":"FLOAT","value":42.0}}
{"value":{"schemaType":"DOUBLE","value":42.0}}
{"value":{"schemaType":"BOOLEAN","value":true}}
{"value":{"schemaType":"DATE","value":"1970-01-01T00:00:00.100+00:00"}}
{"value":{"schemaType":"TIME","value":"01:01:40"}}
{"value":{"schemaType":"TIMESTAMP","value":"1970-01-01T00:01:40.000+00:00"}}
{"value":{"schemaType":"INSTANT","value":"1970-01-01T00:00:00.100Z"}}
{"value":{"schemaType":"LOCAL_DATE","value":"1970-04-11"}}
{"value":{"schemaType":"LOCAL_TIME","value":"00:01:40"}}
{"value":{"schemaType":"LOCAL_DATE_TIME","value":"1970-01-01T00:01:40"}}
```

#### AVRO type

For AVRO schema types, the AVRO value is serialized to bytes and then sent as a Base64 string in the JSON payload.
The AVRO schema is also sent in the payload as a Base64 binary string.
Here is an example of a Lambda payload with an AVRO value

```json
{
  "value": {
    "schemaType": "AVRO",
    "schema": "eyJ0eXBlIjoicmVjb3JkIiwibmFtZSI6InJlY29yZCIsImZpZWxkcyI6W3sibmFtZSI6ImZpcnN0TmFtZSIsInR5cGUiOiJzdHJpbmcifV19", 
    "value": "CEphbmU="
  }
}
```

#### JSON type

For JSON schema types, the JSON value is embedded in the JSON payload.
The native AVRO schema is also sent in the payload as a Base64 binary string.

```json
{
  "value": {
    "schemaType": "JSON",
    "schema": "eyJ0eXBlIjoicmVjb3JkIiwibmFtZSI6InJlY29yZCIsImZpZWxkcyI6W3sibmFtZSI6ImZpcnN0TmFtZSIsInR5cGUiOiJzdHJpbmcifV19",
    "value": {
      "firstName": "Jane"
    }
  }
}
```

#### KeyValue type

For KeyValue types, the key and value are themselves Value objects with their own schema type.
The encoding type (`INLINE` or `SEPARATED`) is passed in the `keyValueEncodingType` field. A null `keyValueEncodingType` corresponds to an `INLINE` encoding.

```json
{
  "value": {
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
}
```