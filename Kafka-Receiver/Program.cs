// See https://aka.ms/new-console-template for more information

using System.Reflection;
using Confluent.Kafka;
using Kafka_Receiver;
using Microsoft.Extensions.DependencyInjection;
using Serilog;
using ServiceBus_Custom_Lib.Consumer;
using ServiceBus_Custom_Lib.Producer;

const string topicName = "topic_8";

var serviceCollection = new ServiceCollection();
ConfigureServices(serviceCollection);

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Debug()
    .WriteTo.Console()
    .WriteTo.Seq("http://localhost:5341")
    .Enrich.WithProperty("Application", "Receiver")
    .CreateLogger();

serviceCollection.AddSingleton<IServiceBus>(_ => new ServiceBus(new ProducerConfig
{
    BootstrapServers = "pkc-56d1g.eastus.azure.confluent.cloud:9092",
    SaslUsername = "OR666J4Z3XWDSSCV",
    SaslPassword = "s+YBx2NJ9aKXT3SNSeOHD1WcnRnFnfoY9lZRuKzTz/JVBwDW9v+pNn7+84ktK2mm",
    SecurityProtocol = SecurityProtocol.SaslSsl,
    SaslMechanism = SaslMechanism.Plain,
    Acks = Acks.Leader
}));

var sp = serviceCollection.BuildServiceProvider();

sp.SubscribeToTopic<OrderCreatedEvent>(topicName, Assembly.GetExecutingAssembly(), new ConsumerConfig()
{
    GroupId = AppDomain.CurrentDomain.FriendlyName,
    BootstrapServers = "pkc-56d1g.eastus.azure.confluent.cloud:9092",
    SaslUsername = "OR666J4Z3XWDSSCV",
    SaslPassword = "s+YBx2NJ9aKXT3SNSeOHD1WcnRnFnfoY9lZRuKzTz/JVBwDW9v+pNn7+84ktK2mm",
    SecurityProtocol = SecurityProtocol.SaslSsl,
    SaslMechanism = SaslMechanism.Plain,
    ClientId = AppDomain.CurrentDomain.FriendlyName,
    EnableAutoCommit = false,
    AutoOffsetReset = AutoOffsetReset.Earliest,
});

while (true)
{
    await Task.Delay(10000);
    // Console.WriteLine($"First date: {Data.ReceivedDateHistory?.FirstOrDefault()}, Last date: {Data.ReceivedDateHistory?.LastOrDefault()}");
    // Console.WriteLine($"Difference: {Data.ReceivedDateHistory?.LastOrDefault() - Data.ReceivedDateHistory?.FirstOrDefault()}");
    // Console.WriteLine($"Total messages: {Data.ReceivedDateHistory?.Count}");
    // Console.WriteLine($"Consumer Info: Total requests: {ConsumerManager.ElapsedHistory.Count} - Total elapsed (ms): {ConsumerManager.ElapsedHistory.Sum(x => x.TotalMilliseconds)}" +
    //                   $" - Average elapsed (ms): {ConsumerManager.ElapsedHistory.Sum(x => x.TotalMilliseconds) / ConsumerManager.ElapsedHistory.Count}");
    // Console.WriteLine($"Troughput: {Data.ReceivedDateHistory?.Count / (ConsumerManager.ElapsedHistory.Sum(x => x.TotalMilliseconds) / 1000)}");
    // Console.WriteLine("-------------------------------------------------------------------------");
}


static void ConfigureServices(IServiceCollection services)
{
    services.AddTransient<Consumer>();
}
