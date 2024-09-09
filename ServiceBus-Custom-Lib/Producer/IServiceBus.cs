namespace ServiceBus_Custom_Lib.Producer;

public interface IServiceBus
{
    Task PublishAsync<T>(T message, string topic, string? key = null) where T : class;
    Task PublishAsync<T>(IEnumerable<T> messages, string topic) where T : class;
}