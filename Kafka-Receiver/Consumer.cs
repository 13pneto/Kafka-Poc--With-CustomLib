using Serilog;
using ServiceBus_Custom_Lib.Consumer;

namespace Kafka_Receiver;

public class Consumer : IConsume<OrderCreatedEvent>
{
    private static object obj = new object();
    private static int counter = 0;

    public async Task HandleAsync(OrderCreatedEvent message)
    {
        lock (obj)
        {
            Console.WriteLine($"{counter} -  Starting Delay");
            Interlocked.Increment(ref counter);
        }

        // await Task.Delay(3000);
        // throw new Exception("Error in consumer test");
        Log.Information(
            $"Received order created event: {message.Uuid} - Thread: {Thread.CurrentThread.ManagedThreadId}");
        // await Task.Delay(3000);
        // Data.ReceivedDateHistory.Enqueue(DateTimeOffset.Now);
        // return Task.CompletedTask;
    }
}

public class OrderCreatedEvent
{
    public Guid Uuid { get; set; } = Guid.NewGuid();
    public string Description { get; set; } = DateTimeOffset.Now.ToString("F");
    public DateTimeOffset Date { get; set; } = DateTimeOffset.Now;
}