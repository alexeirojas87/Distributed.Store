namespace Distributed.Store.Shared.Kafka.Models
{
    public class KafkaResponse
    {
        public bool Completed { get; set; }
        public string? ErrorMessage { get; set; }
    }
}
