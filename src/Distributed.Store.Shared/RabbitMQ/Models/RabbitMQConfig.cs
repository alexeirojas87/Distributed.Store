﻿namespace Distributed.Store.Shared.RabbitMQ.Models
{
    public class RabbitMQConfig
    {
        public string Hostname { get; private set; } = null!;
        public RabbitMQCredentials? Credentials { get; private set; }
        public PublisherSettings? Publisher { get; init; }
        public ConsumerSettings? Consumer { get; init; }

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

    public record PublisherSettings
    {
        public string? IntegrationExchange { get; init; }
        public string? DomainExchange { get; init; }
    }

    public record ConsumerSettings
    {
        public string? IntegrationQueue { get; init; }
        public string? DomainQueue { get; init; }
    }
}
