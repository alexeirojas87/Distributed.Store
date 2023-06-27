using Confluent.Kafka;
using System.Text;
using System.Text.Json;

namespace Distributed.Store.Shared.Kafka
{
    public class KafkaValueSerializer<T> : ISerializer<T>, IDeserializer<T>
    {
        public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            var jsonString = Encoding.UTF8.GetString(data);
            return JsonSerializer.Deserialize<T>(jsonString) ?? throw new InvalidOperationException("Deserialization returned null");
        }

        public byte[] Serialize(T data, SerializationContext context)
        {
            var jsonString = JsonSerializer.Serialize(data);
            return Encoding.UTF8.GetBytes(jsonString);
        }
    }
}
