using Confluent.Kafka;

namespace Stock.API.Services;

public class ServiceBus(IConfiguration configuration) : IServiceBus
{
    public ConsumerConfig GetConsumerConfig(string groupId)
    {
        return new ConsumerConfig()
        {
            BootstrapServers = configuration.GetSection("ServiceBusSettings:Kafka:BootstrapServers").Value,
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };
    }
}
