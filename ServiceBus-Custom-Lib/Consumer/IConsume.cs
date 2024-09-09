namespace ServiceBus_Custom_Lib.Consumer;

public interface IConsume<T>
{
    Task HandleAsync(T message);
}