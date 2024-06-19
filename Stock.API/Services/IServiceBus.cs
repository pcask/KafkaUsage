using Confluent.Kafka;

namespace Stock.API.Services;

public interface IServiceBus
{
    ConsumerConfig GetConsumerConfig(string groupId);
}