namespace Distributed.Store.Shared.RabbitMQ.Models
{
    public class RabbitMQConfig
    {
        public string Hostname { get; set; } = null!;
        public required RabbitMQCredentials Credentials { get; set; }
        public required Exchange Exchange { get; set; }
    }

    public record RabbitMQCredentials
    {
        public string Username { get; init; } = null!;
        public string Password { get; init; } = null!;
    }
    public record Exchange
    {
        public string Name { get; init; } = null!;
        public ExchangeType Type { get; init; } = ExchangeType.fanout!;
    }

    public enum ExchangeType
    {
        direct,
        topic,
        header,
        fanout
    }
}
