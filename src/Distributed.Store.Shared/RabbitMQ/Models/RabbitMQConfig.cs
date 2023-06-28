namespace Distributed.Store.Shared.RabbitMQ.Models
{
    public class RabbitMQConfig
    {
        public string Hostname { get; private set; } = null!;
        public RabbitMQCredentials? Credentials { get; private set; }
        public string? Exchange { get; init; }

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
}
