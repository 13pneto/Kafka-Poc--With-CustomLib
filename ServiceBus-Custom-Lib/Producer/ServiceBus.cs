using System.Text.Json;
using Confluent.Kafka;
using Serilog;

namespace ServiceBus_Custom_Lib.Producer;

public class ServiceBus : IServiceBus
{
    private readonly ProducerConfig _kafkaProducerConfig;

    private readonly IProducer<string, string> _producer;

    public ServiceBus(ProducerConfig kafkaProducerConfig)
    {
        _kafkaProducerConfig = kafkaProducerConfig;

        _producer = new ProducerBuilder<string, string>(_kafkaProducerConfig)
            .Build();
    }

    public async Task PublishAsync<T>(T message, string topic, string? key = null) where T : class
    {
        Log.Information("(ServiceBus) Publishing message of type {MessageType} on topic {Topic}", typeof(T), topic);
        
        // _producer.Produce(topic, new Message<string, string>
        // {
        //     Value = JsonSerializer.Serialize(message)
        // }, OnDelivered<T>);
        
        var result = await _producer.ProduceAsync(topic, new Message<string, string>()
        {
            Key = key ?? Guid.NewGuid().ToString(),
            Value = JsonSerializer.Serialize(message)
        });
        
        OnDelivered<T>(result);
    }
    
    

    public Task PublishAsync<T>(IEnumerable<T> messages, string topic) where T : class
    {
        foreach (var message in messages)
        {
            _producer.Produce(topic, new Message<string, string>
            {
                Value = JsonSerializer.Serialize(message)
            }, OnDelivered<T>);
        }
        
        _producer.Flush();
        
        return Task.CompletedTask;
    }
    
    private static void OnDelivered<T>(DeliveryResult<string, string> deliveryHandler)
    {
        switch (deliveryHandler.Status)
        {
            case PersistenceStatus.Persisted:
                Log.Information("(ServiceBus) Success on publish message of type {MessageType}. " +
                                "Topic: {Topic}, Partition: {Partition}, Offset: {Offset}",
                    typeof(T), deliveryHandler.Topic, deliveryHandler.Partition, deliveryHandler.Offset);
                break;
            case PersistenceStatus.PossiblyPersisted:
                Log.Warning("(ServiceBus) Success on publish message of type {MessageType}. " +
                            "Topic: {Topic}, Partition: {Partition}, Offset: {Offset}",
                    typeof(T), deliveryHandler.Topic, deliveryHandler.Partition, deliveryHandler.Offset);
                break;
            case PersistenceStatus.NotPersisted:
                Log.Error("(ServiceBus) Error on publish message of type {MessageType}. " +
                          "Topic: {Topic}, Partition: {Partition}, Offset: {Offset}",
                    typeof(T), deliveryHandler.Topic, deliveryHandler.Partition, deliveryHandler.Offset);
                break;
            default:
                throw new ArgumentOutOfRangeException();
        }
    }
}