using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Shared.Bus.CustomSerializers;

namespace Order.API.Services;

public class ServiceBus(IConfiguration configuration, ILogger<ServiceBus> logger) : IServiceBus
{
    public async Task CreateTopicsOrQueuesAsync(List<string> topicOrQueueNameList)
    {
        using var adminClient = new AdminClientBuilder(new AdminClientConfig()
        {
            BootstrapServers = configuration.GetSection("ServiceBusSettings:Kafka:BootstrapServers").Value
        }).Build();

        foreach (var topicName in topicOrQueueNameList)
        {
            var topicSpecification = new TopicSpecification
            {
                Name = topicName,
                NumPartitions = 6,
                ReplicationFactor = 1,
                Configs = new Dictionary<string, string>
                    {
                        { "retention.ms", TimeSpan.FromDays(30).TotalMilliseconds.ToString() },
                        { "min.insync.replicas", "3" }
                    }
            };

            try
            {
                await adminClient.CreateTopicsAsync([topicSpecification]);
                logger.LogInformation($"Topic '{topicName}' created successfully.");
            }
            catch (CreateTopicsException e)
            {
                logger.LogWarning(e.Message);
            }
        }
    }

    private readonly ProducerConfig _producerConfig = new()
    {
        BootstrapServers = configuration.GetSection("ServiceBusSettings:Kafka:BootstrapServers").Value,
        Acks = Acks.All,
        MessageSendMaxRetries = 10,
        RetryBackoffMaxMs = 2000,
        RetryBackoffMs = 100,

        // Producer veya Consumer tarafından ulaşılmaya çalışılan isimde herhangi bir topic yoksa otomatik oluşturulur ve default değeri true dur.
        // AllowAutoCreateTopics = false, 
    };

    public async Task<bool> PublishAsync<Tkey, Tval>(Tkey key, Tval value, string topicOrQueueName)
    {
        try
        {
            using var producer = new ProducerBuilder<Tkey, Tval>(_producerConfig)
                                                            .SetKeySerializer(new CustomKeySerializer<Tkey>())
                                                            .SetValueSerializer(new CustomValueSerializer<Tval>())
                                                            .Build();

            var message = new Message<Tkey, Tval>()
            {
                Key = key,
                Value = value
            };

            var result = await producer.ProduceAsync(topicOrQueueName, message);

            return result.Status == PersistenceStatus.Persisted;
        }
        catch (Exception ex)
        {
            await Console.Out.WriteLineAsync(ex.Message);
            return false;
        }
    }
}
