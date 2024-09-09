// See https://aka.ms/new-console-template for more information

using System.Diagnostics;
using Confluent.Kafka;
using Serilog;
using ServiceBus_Custom_Lib.Producer;

var quantity = 50000;
var topicName = "topic_8";

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Debug()
    .WriteTo.Console()
    .WriteTo.Seq("http://localhost:5341")
    .Enrich.WithProperty("Application", "Producer")
    .CreateLogger();


IServiceBus serviceBus = new ServiceBus(new ProducerConfig()
{
    BootstrapServers = "pkc-56d1g.eastus.azure.confluent.cloud:9092",
    SaslUsername = "OR666J4Z3XWDSSCV",
    SaslPassword = "s+YBx2NJ9aKXT3SNSeOHD1WcnRnFnfoY9lZRuKzTz/JVBwDW9v+pNn7+84ktK2mm",
    SecurityProtocol = SecurityProtocol.SaslSsl,
    SaslMechanism = SaslMechanism.Plain,
    Acks = Acks.Leader,
});

var tasks = new List<Task>();

var sw = Stopwatch.StartNew();
Console.WriteLine("Starting loop");

for (var i = 0; i < quantity; i++)
{
    // orders.Add(new OrderCratedEvent());
    var task = serviceBus.PublishAsync(new OrderCratedEvent(), topicName);
    
    tasks.Add(task);
}



Console.WriteLine("Finished loop");
await Task.WhenAll(tasks);
sw.Stop();
Console.WriteLine($"Elapsed: {sw.Elapsed}");

// Console.WriteLine("Publishing");
// await serviceBus.PublishAsync(orders as IEnumerable<OrderCratedEvent>, topicName);
// Console.WriteLine("Finished publishing");

class OrderCratedEvent
{
    public Guid Uuid { get; set; } = Guid.NewGuid();
    public string Description { get; set; } = DateTimeOffset.Now.ToString("F");
    public DateTimeOffset Date { get; set; } = DateTimeOffset.Now;
}