namespace Distributed.Store.Shared.RabbitMQ.Models
{
    public class RabbitMQConfig
    {
        public string Hostname { get; private set; } = null!;
        public RabbitMQCredentials? Credentials { get; private set; }
        public required Exchange Exchange { get; init; }

        public void SetCredentials(RabbitMQCredentials credentials)
        {
            Credentials = credentials;
        }

        public void SetHostName(string hostname)
        {
            Hostname = hostname;
        }
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
