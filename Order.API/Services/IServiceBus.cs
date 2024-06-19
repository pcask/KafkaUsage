namespace Order.API.Services;

public interface IServiceBus
{
    Task<bool> PublishAsync<Tkey, Tval>(Tkey key, Tval value, string topicOrQueueName);
    Task CreateTopicsOrQueuesAsync(List<string> topicOrQueueNameList);
}
