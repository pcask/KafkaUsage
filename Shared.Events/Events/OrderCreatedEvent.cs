namespace Shared.Bus.Events;

public record OrderCreatedEvent(string OrderCode, string UserId, decimal TotalPrice);