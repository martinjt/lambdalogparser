# AWS Log Parser (Load Balancers) For Lambda

This solution is a dotnet core (netcoreapp1.1) project that can be deployed into AWS Lambda and be triggered whenever a load balancer drops an access log into an S3 bucket.

This library does the following:

1. Receive a trigger from S3
1. Download the files that triggered the event
1. Optionally decompress the file (Only applicable for Application Load Balancers)
1. Process the file (line by line) through Grok.NET
1. Process the entries to remove unneeded information
1. Batch submit the entries an ElasticSearch cluster

*Note: Currently this is very centred around the AWS ElasticSearch as a Service instance, there will need to be small tweaks (specifically where you get a IElasticClient instance) if you want to use it for a standard cluster*

# Build instructions

To build this, you will need the correct tooling installed for dotnet core.  Then:

```powershell
dotnet restore
dotnet build
dotnet publish
```

The last part will publish all the dll's required to the `<solution>\src\AWSLambdaLogParser\bin\netcoreapp1.1\publish`

Zip the entire contents of that directory (NOT the directory itself), then upload that to your function.

# AWS Configuration

You will need the following EnvironmentVariables

* `ES_ACCESS_KEY` - this is the IAM access key that has access to push to ElasticSearch
* `ES_ACCESS_SECRET` - this is secret for the above key
* `ES_CLUSTER` - The http endpoint for the ElasticSearch cluster
* `ES_REGION` - The region your ElasticSearch cluster lives in
* `ES_INDEXPREFIX` - The prefix for logs from this trigger

*Small note on log prefix: the standard index name format is logstash-<prfix>-dd.mm.yyyy the prefix allows you differentiate between logs from different regions.  Kibana will put them all together at the end.*

The Lambda function will need access to the S3 bucket where the log files are pushed to, but nothing else.