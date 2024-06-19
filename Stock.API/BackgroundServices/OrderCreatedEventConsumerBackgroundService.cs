using Confluent.Kafka;
using Shared.Bus.Constants;
using Shared.Bus.CustomDeserializers;
using Shared.Bus.Events;
using Stock.API.Services;

namespace Stock.API.BackgroundServices;

public class OrderCreatedEventConsumerBackgroundService(IServiceBus serviceBus, ILogger<OrderCreatedEventConsumerBackgroundService> logger) : BackgroundService
{
    private IConsumer<string?, OrderCreatedEvent>? _consumer;
    public override Task StartAsync(CancellationToken cancellationToken)
    {
        _consumer = new ConsumerBuilder<string?, OrderCreatedEvent>(serviceBus.GetConsumerConfig(ServiceBusConstants.OrderCreatedEventConsumerGroupId))
            .SetValueDeserializer(new CustomValueDeserializer<OrderCreatedEvent>())
            .Build();

        return base.StartAsync(cancellationToken);
    }
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _consumer!.Subscribe(ServiceBusConstants.OrderCreatedEventTopicName);

        while (!stoppingToken.IsCancellationRequested) // Uygulama kapama isteği geldiği anda döngüden çıkılacaktır. 
        {
            var consumeResult = _consumer.Consume(5000);

            if (consumeResult != null)
            {
                try
                {
                    var orderCreatedEvent = consumeResult.Message.Value;

                    // Db operations;
                    // Reduce the amount of products in stock.

                    logger.LogInformation($"CurrentOffset = {consumeResult.Offset} \n" +
                        $"Received message \n" +
                        $"Key = {consumeResult.Message.Key} \n" +
                        $"OrderCode = {orderCreatedEvent.OrderCode} \n" +
                        $"UserId = {orderCreatedEvent.UserId} \n" +
                        $"TotalPrice = {orderCreatedEvent.TotalPrice}");

                    _consumer.Commit(consumeResult);
                }
                catch (Exception ex)
                {
                    logger.LogError(ex.Message);
                    throw;
                }
            }
        }
    }
}
