using Order.API.Services;
using Shared.Bus.Constants;

namespace Order.API.Extensions;

public static class WebAppExtensions
{
    public static async Task CreateTopicsOrQueuesAsync(this WebApplication app)
    {
        using var scope = app.Services.CreateScope();
        var sb = scope.ServiceProvider.GetRequiredService<IServiceBus>();

        await sb.CreateTopicsOrQueuesAsync([ServiceBusConstants.OrderCreatedEventTopicName]);
    }
}
