using Confluent.Kafka;
using Confluent.SchemaRegistry;

namespace Distributed.Store.Shared.Kafka.Models
{
    public class KafkaConfig
    {
        public string ValueDeserializer { get; set; } = "json";

        public uint CommitPeriod { get; set; } = 10;

        public int CommitMaxRetryCount { get; set; } = 5;

        public int CommitRetryDelay { get; set; } = 3000;

        public int MaxRetryAttempts { get; set; } = 3;

        public ConsumerConfig? ConsumerConfig { get; set; }

        public ProducerConfig? ProducerConfig { get; set; }

        public SchemaRegistryConfig? SchemaRegistryConfig { get; set; }
    }
}
