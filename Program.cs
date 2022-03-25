using System;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kafka.Public;
using Kafka.Public.Loggers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace KafkaDemo
{
    static class Program
    {
        static async Task Main(string[] args)
        {
            await CreateHostBuilder(args).Build().RunAsync();
        }

        //The Hosted Services are registered in the dependency injection at startup and started automatically
        private static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureServices(
                     (context, collection) =>
                     {
                         collection.AddHostedService<KafkaConsumerHostedService>();
                         collection.AddHostedService<KafkaProducerHostedService>();
                     }
                 );
    }

    public class KafkaConsumerHostedService: IHostedService
    {
        private readonly ILogger<KafkaConsumerHostedService> _logger;
        private readonly ClusterClient _cluster;

        public KafkaConsumerHostedService(ILogger<KafkaConsumerHostedService> logger)
        {
            _logger = logger;
            _cluster = new ClusterClient(
                new Configuration
                {
                    Seeds = "b-2.kafkademocluster.pissei.c8.kafka.eu-west-1.amazonaws.com:9092,b-1.kafkademocluster.pissei.c8.kafka.eu-west-1.amazonaws.com:9092"
                },
                new ConsoleLogger()
            );
        }

        // Triggered when the application host is ready to start the service.
        public Task StartAsync(CancellationToken cancellationToken)
        {
            _cluster.ConsumeFromLatest("test");
            _cluster.MessageReceived += record => { _logger.LogInformation($"Received: {Encoding.UTF8.GetString(record.Value as byte[])}"); };

            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _cluster?.Dispose();
            return Task.CompletedTask;
        }
    }

    public class KafkaProducerHostedService: IHostedService
    {
        private readonly ILogger<KafkaProducerHostedService> _logger;
        private readonly IProducer<Null, string> _producer;
        private readonly string _topicName;

        public KafkaProducerHostedService(ILogger<KafkaProducerHostedService> logger)
        {
            try
            {
                _logger = logger;
                _topicName = "test";
                string bootstrapServers = "b-2.kafkademocluster.pissei.c8.kafka.eu-west-1.amazonaws.com:9092,b-1.kafkademocluster.pissei.c8.kafka.eu-west-1.amazonaws.com:9092";
                var config = new ProducerConfig
                             {
                                 SecurityProtocol = SecurityProtocol.Plaintext,
                                 BootstrapServers = bootstrapServers,
                                 RequestTimeoutMs = 2000,
                                 MessageTimeoutMs = 5000,
                                 
                                 // retry settings:
                                 // Receive acknowledgement from all sync replicas
                                 Acks = Acks.All,
                                 // Number of times to retry before giving up
                                 MessageSendMaxRetries = 3,
                                 // Duration to retry before next attempt
                                 RetryBackoffMs = 1000,
                                 // Set to true if you don't want to reorder messages on retry
                                 EnableIdempotence = true
                             };
                _producer = new ProducerBuilder<Null, string>(config).Build();
                
        

                using var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build();

                adminClient.CreateTopicsAsync(
                    new[]
                    {
                        new TopicSpecification
                        {
                            Name = _topicName,
                            ReplicationFactor = 1,
                            NumPartitions = 1
                        }
                    }
                );

            }
            catch (CreateTopicsException e)
            {
                Console.WriteLine($"An error occured creating topic {e.Results[0].Topic}: {e.Results[0].Error.Reason}");
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            try
            {
                for (var i = 0; i < 100; ++i)
                {
                    var order = new OrderRequest
                                {
                                    CustomerId = i,
                                    ProductId = i,
                                    OrderId = i,
                                    Quantity = 1,
                                    Status = "New"
                                };
                    string message = JsonSerializer.Serialize(order);
                    _logger.LogInformation(message);
                    // ProduceAsync creates a topic if not exists
                    await _producer.ProduceAsync(
                        _topicName,
                        new Message<Null, string>
                        {
                            Value = message
                        },
                        cancellationToken
                    );
                }

                _producer.Flush(TimeSpan.FromSeconds(10));
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _producer?.Dispose();
            return Task.CompletedTask;
        }
    }
}