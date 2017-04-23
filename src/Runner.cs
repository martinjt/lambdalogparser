using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Amazon;
using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;
using Amazon.S3;
using Amazon.S3.Model;
using GrokDotNet;
using Nest;
using Elasticsearch.Net;
using Elasticsearch.Net.Aws;
using Amazon.Runtime;

[assembly: LambdaSerializerAttribute(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]
namespace AWSLambdaLogParser
{
    public class Runner
    {
        // ------- Configuation START -----------
        const int ESPushPageSize = 10000;

        static HashSet<string> removeFields = new HashSet<string>() {
                "message", "urihost", "version", "port", 
                "httpversion", "backendport", "backendip", 
                "rawrequest", "user_agent", "inboundport", 
                "request", "received_bytes", "params", "clientport", "@version"
            };

        // ------- Configuation END -----------

        private string AWSAccessKey = Environment.GetEnvironmentVariable("ES_ACCESS_KEY");
        private string AWSSecret = Environment.GetEnvironmentVariable("ES_ACCESS_SECRET");
        private string ESCluster = Environment.GetEnvironmentVariable("ES_CLUSTER");
        private string ESClusterRegion = Environment.GetEnvironmentVariable("ES_REGION");

        public const string grokString = "%{TIMESTAMP_ISO8601:timestamp} %{NOTSPACE:elb} %{IP:clientip}:%{INT:clientport:int} (?:(%{IP:backendip}:?:%{INT:backendport:int})|-) %{NUMBER:request_processing_time:float} %{NUMBER:backend_processing_time:float} %{NUMBER:response_processing_time:float} (?:-|%{INT:elb_status_code:int}) (?:-|%{INT:backend_status_code:int}) %{INT:received_bytes:int} %{INT:sent_bytes:int} \"%{ELB_REQUEST_LINE}\" \"(?:-|%{DATA:user_agent})\" (?:-|%{NOTSPACE:ssl_cipher}) (?:-|%{NOTSPACE:ssl_protocol})";

        public const string alb_grokString = "%{NOTSPACE:conn_type} %{TIMESTAMP_ISO8601:timestamp} %{NOTSPACE:elb} %{IP:clientip}:%{INT:clientport:int} (?:(%{IP:backendip}:?:%{INT:backendport:int})|-) %{NUMBER:request_processing_time:float} %{NUMBER:backend_processing_time:float} %{NUMBER:response_processing_time:float} (?:-|%{INT:elb_status_code:int}) (?:-|%{INT:backend_status_code:int}) %{INT:received_bytes:int} %{INT:sent_bytes:int} \"%{ELB_REQUEST_LINE}\" \"(?:-|%{DATA:user_agent})\" (?:-|%{NOTSPACE:ssl_cipher}) (?:-|%{NOTSPACE:ssl_protocol}) %{NOTSPACE:target_group} \"Root=%{NOTSPACE:trace_id}\"";
        public const string domainGrokString = "%{DATA:domain}:%{DATA:inboundport}";
        private IElasticClient _esClient;

        /// <summary>
        /// Main Entry method, called by Lambda
        /// </summary>
        /// <param name="s3event">This contains the full information on what triggered the event</param>
        /// <returns></returns>
        [LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]
        public async Task Handler(S3Event s3event)
        {
            LambdaLogger.Log($"TriggerStart - {s3event.Records.Count()} events found");
            var indexPrefix = Environment.GetEnvironmentVariable("ES_INDEXPREFIX");
            var grok = new Grok(grokString);
            var alb_grok = new Grok(alb_grokString);
            var domainGrok = new Grok(domainGrokString);
            foreach (var s3 in s3event.Records)
            {
                var real_grok = s3.S3.Object.Key.EndsWith(".gz") ? alb_grok : grok;

                await ProcessFileFromS3(s3.AwsRegion, null,s3.S3.Bucket.Name, s3.S3.Object.Key, real_grok, domainGrok, indexPrefix);
            }
            LambdaLogger.Log("TriggerFinish");
        }

        /// <summary>
        /// Processes a file from S3, this is a reusable method that could be used to process old files too.
        /// </summary>
        /// <param name="region"></param>
        /// <param name="credentials"></param>
        /// <param name="bucket"></param>
        /// <param name="key"></param>
        /// <param name="grok"></param>
        /// <param name="domainGrok"></param>
        /// <param name="indexPrefix"></param>
        /// <returns></returns>
        public async Task ProcessFileFromS3(string region, AWSCredentials credentials, string bucket, string key, Grok grok, Grok domainGrok, string indexPrefix)
        {
            LambdaLogger.Log($"StartFile - {key}");
            var request = new GetObjectRequest
            {
                BucketName = bucket,
                Key = key
            };
            using (var client = credentials == null ? 
                new AmazonS3Client(RegionEndpoint.GetBySystemName(region)) : 
                new AmazonS3Client(credentials, RegionEndpoint.GetBySystemName(region))) // This is to allow for external testing, as credentials are normally implied
            using (var response = await client.GetObjectAsync(request))
            using (var decompressedStream = key.EndsWith(".gz") ? new MultiMemberGzipStream(response.ResponseStream) : null)
            {
                LambdaLogger.Log($"Size - {response.ContentLength} bytes");
                await ProcessStream(decompressedStream ?? response.ResponseStream, grok, domainGrok, bucket, indexPrefix);
            }
        }

        /// <summary>
        /// Process a given stream into elastic search
        /// </summary>
        /// <param name="stream"></param>
        /// <param name="grok"></param>
        /// <param name="domainGrok"></param>
        /// <param name="bucketName"></param>
        /// <param name="indexPrefix"></param>
        /// <returns></returns>
        public async Task ProcessStream(Stream stream, Grok grok, Grok domainGrok, string bucketName, string indexPrefix)
        {
            using (var reader = new StreamReader(stream))
            {
                var credentials = new BasicAWSCredentials(AWSAccessKey, AWSSecret);

                var hits = new List<List<Tuple<string, object>>>();
                var i = 0;
                while (!reader.EndOfStream)
                {
                    i++;
                    var line = await reader.ReadLineAsync();
                    var result = grok.ParseLine(line);
                    var uriHost = result.Captures.FirstOrDefault(c => c.Item1 == "urihost");
                    if (uriHost != null)
                    {
                        var domainResult = domainGrok.ParseLine(uriHost.Item2.ToString());
                        result.Captures.AddRange(
                            domainResult
                                .Captures
                                .Select(c =>
                                    new Tuple<string, object>(c.Item1, c.Item2.ToString().ToLower())));
                    }
                    var parsedResults = PrepareEntries(result);
                    parsedResults.Add(new Tuple<string, object>("logbucket", bucketName));
                    hits.Add(parsedResults);
                    if (hits.Count() > ESPushPageSize)
                    {
                        await PushToESAsync(credentials, indexPrefix, hits.ToList());
                        hits = new List<List<Tuple<string, object>>>();
                    }
                }
                if (hits.Count() > 0)
                    await PushToESAsync(credentials, indexPrefix, hits);

                LambdaLogger.Log($"EndFile - Total Entries in File {i}");
            }
        }

        /// <summary>
        /// Prepares a file line entry, removing fields, and setting up other things.
        /// </summary>
        /// <param name="response"></param>
        /// <returns></returns>
        private List<Tuple<string, object>> PrepareEntries(GrokParseResponse response)
        {
            var returnList = new List<Tuple<string, object>>();
            foreach (var value in response.Captures)
            {
                if (value.Item1 == "timestamp")
                {
                     returnList.Add(new Tuple<string, object>("@timestamp", value.Item2));
                     continue;
                }

                if (!removeFields.Contains(value.Item1))
                {
                    returnList.Add(value);
                }
            }
            return returnList;
        }

        /// <summary>
        /// Push the entries to elastic search
        /// </summary>
        /// <param name="credentials"></param>
        /// <param name="indexPrefix"></param>
        /// <param name="list"></param>
        /// <returns></returns>
        private async Task PushToESAsync(BasicAWSCredentials credentials, string indexPrefix, List<List<Tuple<string, object>>> list)
        {
            var bulkRequest = new BulkDescriptor();
            var count = 0;
            foreach (var item in list)
            {
                try {
                    var timestamp = item.FirstOrDefault(i => i.Item1 == "@timestamp");
                    var dateString = DateTime.Parse(timestamp.Item2.ToString()).ToString("yyyy.MM.dd");
                    if (dateString == null)
                        continue;
                    count++;
                    bulkRequest.Index<object>(i => i
                        .Index($"logstash-{indexPrefix}-{dateString}")
                        .Type("elb-access-log")
                        .Document(item.ToDictionary(k => k.Item1, k => k.Item2))
                    );
                }
                catch (Exception ex)
                {
                    LambdaLogger.Log($"ESError-PrepareBatch - Exception adding item to batch: \r\n {ex}");
                    throw;
                }
            }
            LambdaLogger.Log($"ESInfo - Sending File to ES with {count} entries");
            if (count != list.Count())
                LambdaLogger.Log($"ESError-Prepare - There were {list.Count()-count} errored items that were ignored");

            try
            {
                var client = GetClient(credentials);
                var response = await client.BulkAsync(bulkRequest);
                LambdaLogger.Log($"ESInfo - Sent File to ES with {response.Items.Count()} entries");
                if (response.Errors)
                {
                    foreach (var item in response.ItemsWithErrors)
                    {
                        LambdaLogger.Log("ESError-Response - " + item.Error.Reason);
                    }
                    var serverError = response.ServerError?.Error?.Reason;
                    if (serverError != null)
                    {
                        LambdaLogger.Log("ESError-ServerError - " + serverError);
                    }
                    throw new Exception("Error from Elastic Search");
                }
            }
            catch (Exception ex)
            {
                LambdaLogger.Log($"ESError-Send - \r\n {ex}");
                throw;
            }
        }

        private IElasticClient GetClient(BasicAWSCredentials credentials)
        {
            if (_esClient == null)
            {
                var esCredentials = new AwsCredentials
                {
                    AccessKey = credentials.GetCredentials().AccessKey,
                    SecretKey = credentials.GetCredentials().SecretKey
                };
                var httpConnection = new AwsHttpConnection(ESClusterRegion,
                    new StaticCredentialsProvider(esCredentials));

                var pool = new SingleNodeConnectionPool(new Uri(ESCluster));
                var config = new ConnectionSettings(pool, httpConnection);
                _esClient = new ElasticClient(config);
            }
            return _esClient;
        }
    }
}