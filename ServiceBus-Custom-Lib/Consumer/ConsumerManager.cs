using System.Collections.Concurrent;
using System.Diagnostics;
using System.Reflection;
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Serilog;
using Serilog.Context;
using Serilog.Core;

namespace ServiceBus_Custom_Lib.Consumer;

public static class ConsumerManager
{
    private static ConsumerConfig ConsumerConfig;

    private static readonly Dictionary<Type, ConsumerInfo>
        ConsumerInfoByMessageType = new(); //Maps the consumers by TMessage

    private static int MaxConcurrentMessages { get; set; } = 100;
    private static TimeSpan MaxDelayConcurrentMessages { get; set; } = TimeSpan.FromSeconds(10);
    private static MongoService _mongoService = new();


    public static void SubscribeToTopic<TMessage>(this IServiceProvider services, string topicName, Assembly assembly,
        ConsumerConfig? consumerConfig = null)
    {
        ConsumerConfig = GetConsumerConfig(services, consumerConfig);

        var consumerInfo = GetConsumerInfoByMessageType<TMessage>(services, assembly);
        ConsumerInfoByMessageType.Add(typeof(TMessage), consumerInfo);

        var kafkaConsumer = BuildKafkaConsumer();

        StartConsumer<TMessage>(topicName, kafkaConsumer);
    }

    private static void StartConsumer<TMessage>(string topicName, IConsumer<string, string> consumer)
    {
        TimeSpan elapsedSinceLastRun;
        DateTimeOffset lastRunDate = DateTimeOffset.UtcNow;

        Task.Run(async () =>
        {
            try
            {
                consumer.Subscribe(topicName);
                Log.Information("(ServiceBus) Subscribing to topic '{TopicName}' with consumer group '{ConsumerName}'",
                    topicName, ConsumerConfig.GroupId);

                var tasks = new List<Task>();
                while (true)
                {
                    var consumeResult = consumer.Consume();

                    var task = Task.Run(async () =>
                    {
                        try
                        {
                            await HandleReceivedMessage<TMessage>(consumeResult, consumer);
                        }
                        catch (KafkaException e)
                        {
                            Log.Error(
                                "(ServiceBus) Error to consume message of type {MessageType}. Reason: {ExceptionReason}, " +
                                "Exception: {@Exception}", typeof(TMessage), e.Message, e);
                        }
                    });

                    tasks.Add(task);

                    elapsedSinceLastRun = DateTimeOffset.UtcNow - lastRunDate;
                    var hasMinTasks = tasks.Count >= MaxConcurrentMessages;
                    var hasMinDelay = elapsedSinceLastRun > MaxDelayConcurrentMessages;
                    
                    if (hasMinTasks || hasMinDelay)
                    {
                        lastRunDate = DateTimeOffset.UtcNow;
                        await Task.WhenAll(tasks);
                        tasks.Clear();
                    }
                }
            }
            catch (Exception e)
            {
                Log.Error($"(ServiceBus) Error during subscription in topic {topicName}: {e}");
            }
        });
    }

    private static IConsumer<string, string> BuildKafkaConsumer()
    {
        var kafkaConsumer = new ConsumerBuilder<string, string>(ConsumerConfig)
            .SetLogHandler((_, logMessage) =>
            {
                Log.Error("(Kafka Log) {Facility}: {Message}", logMessage.Facility, logMessage.Message);
            })
            .SetPartitionsAssignedHandler((c, partitions) =>
            {
                foreach (var partition in partitions)
                {
                    Log.Information("(Kafka Log) Assigned to partition: {Partition}", partition.Partition);
                }
            })
            .SetPartitionsRevokedHandler((c, partitions) =>
            {
                foreach (var partition in partitions)
                {
                    Log.Information("(Kafka Log) Revoked from partition: {Partition}", partition.Partition);
                }
            })
            .Build();

        return kafkaConsumer;
    }

    private static ConsumerConfig GetConsumerConfig(IServiceProvider services,
        ConsumerConfig? consumerConfig)
    {
        consumerConfig ??= services.GetService<ConsumerConfig>()!;
        if (consumerConfig is null)
        {
            throw new Exception(
                $"{nameof(Confluent.Kafka.ConsumerConfig)} not found. Please inject {nameof(Confluent.Kafka.ConsumerConfig)} or pass it as parameter");
        }

        return consumerConfig;
    }

    private static ConsumerInfo GetConsumerInfoByMessageType<TMessage>(IServiceProvider services, Assembly assembly)
    {
        var consumersOfMessage = GetConsumerByMessageType<TMessage>(assembly);

        var instance = GetConsumerInstanceFromContainer(services, consumersOfMessage);
        var methodInfo = GetHandleAsyncMethodFromConsumer<TMessage>(consumersOfMessage);

        var consumerInfo = new ConsumerInfo(instance, typeof(TMessage), methodInfo);

        return consumerInfo;
    }


    private static async Task HandleReceivedMessage<TMessage>(ConsumeResult<string, string> consumeResult,
        IConsumer<string, string> consumer)
    {
        Log.Information("(ServiceBus) Received message of type {MessageType}. " +
                  "Topic: {Topic}, Partition: {Partition}, Offset: {Offset}",
            typeof(TMessage), consumeResult.Topic, consumeResult.Partition, consumeResult.Offset);

        var deserializedMessage = JsonSerializer.Deserialize<TMessage>(consumeResult.Message.Value);

        var consumerInfo = ConsumerInfoByMessageType[typeof(TMessage)];
        await consumerInfo.HandleAsync(deserializedMessage);

        await _mongoService.InsertMessage(consumeResult.Partition, consumeResult.Offset);

        try
        {
            consumer.Commit();
        }
        catch (KafkaException e) when (e.Error.Code == ErrorCode.OffsetNotAvailable)
        {
            var notHasConcurrentMessages = MaxConcurrentMessages <= 1;
            if (notHasConcurrentMessages)
            {
                throw;
            }

            Log.Debug(
                "Ignoring commit error: {Exception} because has multiples threads running (MaxConcurrentMessages = {MaxConcurrentMessages})",
                e, MaxConcurrentMessages);
        }

        Log.Information("(ServiceBus) Success on consume message of type {MessageType}. " +
                        "Topic: {Topic}, Partition: {Partition}, Offset: {Offset}",
            typeof(TMessage), consumeResult.Topic, consumeResult.Partition, consumeResult.Offset);
    }

    private static MethodInfo GetHandleAsyncMethodFromConsumer<TMessage>(Type consumerType)
    {
        var handleAsyncMethod = consumerType.GetMethods()
            .FirstOrDefault(m => m.Name == "HandleAsync" &&
                                 m.GetParameters().Length == 1 &&
                                 m.GetParameters()[0].ParameterType == typeof(TMessage));

        if (handleAsyncMethod is null)
        {
            throw new Exception($"Method HandleAsync not found in {consumerType}");
        }

        return handleAsyncMethod;
    }

    private static object GetConsumerInstanceFromContainer(IServiceProvider services, Type consumerType)
    {
        var instance = services.GetRequiredService(consumerType);

        if (instance is null)
        {
            throw new Exception($"Instance of {consumerType} not found in injection container");
        }

        return instance;
    }

    private static Type GetConsumerByMessageType<TMessage>(Assembly assembly)
    {
        var consumerInterfaceType = typeof(IConsume<TMessage>);

        var consumerTypes = assembly.GetTypes()
            .Where(type => type.GetInterfaces()
                .Any(i => i.IsGenericType &&
                          i.GetGenericTypeDefinition() == consumerInterfaceType.GetGenericTypeDefinition()))
            .ToList();

        if (consumerTypes.Count > 1)
        {
            throw new Exception($"Multiple consumers found for message type {typeof(TMessage)}." +
                                $" Please implement only one IConsume<{typeof(TMessage).Name}>");
        }

        return consumerTypes.First();
    }
}