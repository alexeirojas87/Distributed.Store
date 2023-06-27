using System.Diagnostics;

namespace Distributed.Store.Shared.Tools
{
    public class TraceTool
    {
        public static void ModifyOTLPActivity(string traceId)
        {
            if (traceId.Contains('-'))
                traceId = traceId.Replace("-", "");
            var traceIdObj = ActivityTraceId.CreateFromString(traceId.AsSpan());
            var spanId = traceId.Remove(16).AsSpan();
            var spanIdObj = ActivitySpanId.CreateFromString(spanId);
            if (Activity.Current is null)
                Activity.Current = new Activity(traceId).SetParentId(traceIdObj, spanIdObj, ActivityTraceFlags.Recorded).Start();
            else
                _ = (Activity.Current?.SetParentId(traceIdObj, spanIdObj, ActivityTraceFlags.Recorded));

        }
    }
}
