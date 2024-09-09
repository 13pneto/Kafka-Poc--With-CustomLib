using System.Reflection;

namespace ServiceBus_Custom_Lib.Consumer;

public class ConsumerInfo
{
    public object Consumer { get; private set; }
    public Type MessageType { get; private set; }
    public MethodInfo Method { get; private set; }

    public ConsumerInfo(object consumer, Type messageType, MethodInfo method)
    {
        Consumer = consumer;
        MessageType = messageType;
        Method = method;
    }

    public async Task HandleAsync(params object[] args)
    {
        await (Task)Method.Invoke(Consumer, args)!;
    }
}