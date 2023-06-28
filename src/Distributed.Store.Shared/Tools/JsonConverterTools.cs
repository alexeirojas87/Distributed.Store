using System.Text.Json;

namespace Distributed.Store.Shared.Tools
{
    public static class JsonConverterTools
    {
        private static readonly JsonSerializerOptions DefaultJsonSerializerOptions = new()
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        };

        public static string Serialize(this object value, JsonSerializerOptions? options = null)
        {
            options ??= DefaultJsonSerializerOptions;

            return JsonSerializer.Serialize(value, options);
        }
        public static T Deserialize<T>(this byte[] value, JsonSerializerOptions? options = null)
        {
            options ??= DefaultJsonSerializerOptions;

            return JsonSerializer.Deserialize<T>(value, options) ?? throw new ArgumentNullException(nameof(value));
        }
    }
}
