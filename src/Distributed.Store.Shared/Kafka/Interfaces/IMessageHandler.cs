namespace Distributed.Store.Shared.Kafka.Interfaces
{
    public interface IMessageHandler<TData>
    {
        Task HandleMessage(TData message, CancellationToken cancellationToken);
    }
}
