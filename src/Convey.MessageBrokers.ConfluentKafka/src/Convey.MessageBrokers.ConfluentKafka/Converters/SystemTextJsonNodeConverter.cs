#nullable enable
using System;
using System.Text.Json.Nodes;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Convey.MessageBrokers.ConfluentKafka.Converters;

/// <summary>
/// Bridges System.Text.Json.Nodes types (JsonNode/JsonObject/JsonArray/JsonValue)
/// with Newtonsoft.Json. Without this, Newtonsoft reflects into JsonNode internals
/// and follows the Parent back-reference on every node, causing a circular-reference
/// exception during Kafka message serialization.
/// </summary>
public sealed class SystemTextJsonNodeConverter : JsonConverter
{
    public override bool CanConvert(Type objectType)
        => typeof(JsonNode).IsAssignableFrom(objectType);

    public override void WriteJson(JsonWriter writer, object? value, JsonSerializer serializer)
    {
        if (value is JsonNode node)
            writer.WriteRawValue(node.ToJsonString());
        else
            writer.WriteNull();
    }

    public override object? ReadJson(JsonReader reader, Type objectType, object? existingValue, JsonSerializer serializer)
    {
        var token = JToken.Load(reader);
        return JsonNode.Parse(token.ToString(Formatting.None));
    }
}
