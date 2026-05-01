using System.Text.Json.Nodes;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Shouldly;
using STJ = System.Text.Json;

namespace Convey.MessageBrokers.ConfluentKafka.Tests;

public class BusPublisher_JsonSerialization_Tests
{
    // Mirrors the framework's event class that has JsonObject Input.
    private sealed class TestMessage
    {
        public string? Name { get; set; }
        public JsonObject? Input { get; set; }
    }

    // Inlined copy of the converter from BusPublisher — identical logic.
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

    // Simulates MongoMessageOutbox.GetUnsentAsync: deserializes the stored JSON string
    // back into the typed message using System.Text.Json, producing JsonObject nodes
    // that carry Parent back-references.
    private static TestMessage BuildMessageAsDeserializedByOutbox(string json)
        => STJ.JsonSerializer.Deserialize<TestMessage>(json,
               new STJ.JsonSerializerOptions { PropertyNameCaseInsensitive = true })!;

    private static readonly JsonSerializerSettings FixedSettings = new()
    {
        Converters = { new SystemTextJsonNodeConverter() }
    };

    [Fact]
    public void Step1_CustomConverter_serializes_string_values_correctly()
    {
        var message = BuildMessageAsDeserializedByOutbox("""
            {
              "name": "Test Job",
              "input": { "stageType": "sequence", "stages": [] }
            }
            """);

        var result = JsonConvert.SerializeObject(message, FixedSettings);

        result.ShouldBe("""{"Name":"Test Job","Input":{"stageType":"sequence","stages":[]}}""");
    }

    [Fact]
    public void Step2_CustomConverter_preserves_nested_objects_and_arrays()
    {
        var message = BuildMessageAsDeserializedByOutbox("""
            {
              "name": "Complex Job",
              "input": {
                "stageType": "sequence",
                "stages": [
                  { "stageType": "task", "taskId": "abc" },
                  { "stageType": "task", "taskId": "def" }
                ]
              }
            }
            """);

        var result = JsonConvert.SerializeObject(message, FixedSettings);
        Console.WriteLine("Nested output: " + result);

        var reparsed = STJ.JsonSerializer.Deserialize<JsonObject>(result)!;
        reparsed["Input"]!["stageType"]!.GetValue<string>().ShouldBe("sequence");
        reparsed["Input"]!["stages"]!.AsArray()[0]!["taskId"]!.GetValue<string>().ShouldBe("abc");
        reparsed["Input"]!["stages"]!.AsArray()[1]!["taskId"]!.GetValue<string>().ShouldBe("def");
    }

    [Fact]
    public void Step3_CustomConverter_roundtrips_input_as_equivalent_json()
    {
        const string inputJson = """{"stageType":"sequence","stages":[]}""";

        var message = BuildMessageAsDeserializedByOutbox($$"""
            {
              "name": "Test Job",
              "input": {{inputJson}}
            }
            """);

        var result = JsonConvert.SerializeObject(message, FixedSettings);
        Console.WriteLine("Roundtrip output: " + result);

        var reparsed = STJ.JsonSerializer.Deserialize<JsonObject>(result)!;
        reparsed["Input"]!.ToJsonString().ShouldBe(inputJson);
    }
}
