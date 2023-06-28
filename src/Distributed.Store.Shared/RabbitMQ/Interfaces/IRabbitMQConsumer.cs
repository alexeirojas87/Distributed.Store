namespace Distributed.Store.Shared.RabbitMQ.Interfaces
{
    public interface IRabbitMQConsumer<TData>
    {
        Task StartAsync(string queueName, CancellationToken cancelToken = default);
    }
}
