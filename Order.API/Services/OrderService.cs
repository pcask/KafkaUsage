using Order.API.Dtos;
using Shared.Bus.Constants;
using Shared.Bus.Events;

namespace Order.API.Services;

public class OrderService(IServiceBus serviceBus)
{
    public async Task<bool> Create(OrderCreateRequestDto orderCreateRequestDto)
    {
        // Save the order to Db

        var orderCode = Guid.NewGuid().ToString(); // read from Db
        var orderCreatedEvent = new OrderCreatedEvent(orderCode, orderCreateRequestDto.UserId, orderCreateRequestDto.TotalPrice);

        return await serviceBus.PublishAsync(orderCode, orderCreatedEvent, ServiceBusConstants.OrderCreatedEventTopicName);
    }
}
