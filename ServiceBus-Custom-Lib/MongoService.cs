using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;

namespace ServiceBus_Custom_Lib;

public class MongoService
{
    private readonly IMongoCollection<MessageRecord> _collection;

    public MongoService()
    {
        var client = new MongoClient("mongodb://localhost:27017");
        var database = client.GetDatabase("Kafka");
        _collection = database.GetCollection<MessageRecord>("Kafka");
    }

    public async Task InsertMessage(int partition, long offset)
    {
        var record = new MessageRecord
        {
            Partition = partition,
            Offset = offset,
            Timestamp = DateTime.UtcNow
        };

        await _collection.InsertOneAsync(record);
    }
}

public class MessageRecord
{
    [BsonId]
    public ObjectId Id { get; set; }

    [BsonElement("partition")]
    public int Partition { get; set; }

    [BsonElement("offset")]
    public long Offset { get; set; }

    [BsonElement("timestamp")]
    public DateTime Timestamp { get; set; }
}