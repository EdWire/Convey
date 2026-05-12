using System.Text.Json.Nodes;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Shouldly;

namespace Convey.MessageBrokers.ConfluentKafka.Tests;

public class ConsumerHostedService_JsonDeserialization_Tests
{
    // Mirrors a JobsFramework event class with a JsonObject Input property.
    private sealed class TestEvent
    {
        public string? Name { get; set; }
        public JsonObject? Input { get; set; }
    }

    // Inlined copy of SystemTextJsonNodeConverter — same logic as in the package.
    private sealed class SystemTextJsonNodeConverter : Newtonsoft.Json.JsonConverter
    {
        public override bool CanConvert(Type objectType)
            => typeof(JsonNode).IsAssignableFrom(objectType);

        public override void WriteJson(JsonWriter writer, object? value, Newtonsoft.Json.JsonSerializer serializer)
        {
            if (value is JsonNode node)
                writer.WriteRawValue(node.ToJsonString());
            else
                writer.WriteNull();
        }

        public override object? ReadJson(JsonReader reader, Type objectType, object? existingValue, Newtonsoft.Json.JsonSerializer serializer)
        {
            var token = JToken.Load(reader);
            return JsonNode.Parse(token.ToString(Formatting.None));
        }
    }

    // The JSON that arrives on the Kafka wire — produced by BusPublisher with
    // the SystemTextJsonNodeConverter fix applied.
    private const string KafkaMessageJson = """
        {"Name":"Test Job","Input":{"stageType":"sequence","stages":[]}}
        """;

    [Fact]
    public void Step1_Bare_DeserializeObject_throws_because_JsonNode_is_abstract()
    {
        // This is exactly what ConsumerHostedService line 324 was doing before the fix.
        // Newtonsoft can't construct JsonObject — error varies by runtime but always
        // indicates it cannot instantiate the STJ node type.
        Should.Throw<JsonSerializationException>(() =>
            JsonConvert.DeserializeObject(KafkaMessageJson, typeof(TestEvent))
        ).Message.ShouldContain("System.Text.Json.Nodes.JsonObject");
    }

    [Fact]
    public void Step2_SystemTextJsonNodeConverter_deserializes_correctly()
    {
        var settings = new JsonSerializerSettings
        {
            Converters = { new SystemTextJsonNodeConverter() }
        };

        var result = (TestEvent)JsonConvert.DeserializeObject(KafkaMessageJson, typeof(TestEvent), settings)!;

        result.Name.ShouldBe("Test Job");
        result.Input!["stageType"]!.GetValue<string>().ShouldBe("sequence");
        result.Input!["stages"]!.AsArray().Count.ShouldBe(0);
    }
}
