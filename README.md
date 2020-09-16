# nva-search-api
A common search API for NVA across resources


Structure wanted in response from the search API


```JSON
{
  "publishedDate":"2020-08-26",
  "owner":"na@unit.no",
  "contributor":[
    {
      "name":"Doe, John"
    },
    {
      "name":"Doe, Jane"
    },
    {
      "name":"Andrè, Noen"
    }
  ],
  "createdDate":"2020-08-20T11:58:40.390961Z",
  "modifiedDate":"2020-08-26T12:58:40.390961Z",
  "id":"http://localhost/id",
  "type":"sampleResourceType",
  "title":"This Is A Sample Title"
}
```


### Signing request to AWS ElasicSearch
AWS ElasticSearch does not have an AWS Client for Http access to the elasticsearch endpoint. 
AWS recommendation is to restrict the access to the elasticsearch service. 
Fine-grained access control can be to limit access to the elasticsearch service requests must be signed as described in [AWS documentation on access to ElasticSearch](https://docs.aws.amazon.com/elasticsearch-service/latest/developerguide/fgac.html)

Sample source code for signing an http request using Apache HTTP client is available on git, but is not released as a java library.
[Sample source code from AWS for signing requests ](https://github.com/awslabs/aws-request-signing-apache-interceptor)
