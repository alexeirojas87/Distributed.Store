namespace Distributed.Store.Shared.RabbitMQ.Interfaces
{
    public interface IRabbitMQConsumer<TData>
    {
        Task StartAsync(string queueName, string routingKey = "", CancellationToken cancelToken = default);
    }
}
