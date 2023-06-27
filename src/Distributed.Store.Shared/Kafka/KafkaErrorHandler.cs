using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace Distributed.Store.Shared.Kafka
{
    public class KafkaErrorHandler
    {
        public static void HandleError<T>(ILogger<T> logger, Error error)
        {
            if (!error.IsFatal)
                logger.LogWarning(error.ToString());
            else
                logger.LogCritical(error.ToString());
        }
    }
}
