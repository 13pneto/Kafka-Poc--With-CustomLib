using System.Collections.Concurrent;

namespace Kafka_Receiver;

public class Data
{
    public static ConcurrentQueue<DateTimeOffset> ReceivedDateHistory = new();
}