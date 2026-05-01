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

    [Fact]
    public void Step1_Baseline_bare_SerializeObject_throws_circular_reference()
    {
        var message = BuildMessageAsDeserializedByOutbox("""
            {
              "name": "Test Job",
              "input": { "stageType": "sequence", "stages": [] }
            }
            """);

        // Reproduces the original crash — this throws JsonSerializationException.
        JsonConvert.SerializeObject(message);
    }

    [Fact]
    public void Step2_ReferenceLoopHandling_Ignore_does_not_crash_but_corrupts_values()
    {
        var message = BuildMessageAsDeserializedByOutbox("""
            {
              "name": "Test Job",
              "input": { "stageType": "sequence", "stages": [] }
            }
            """);

        var settings = new JsonSerializerSettings
        {
            ReferenceLoopHandling = ReferenceLoopHandling.Ignore
        };

        // No crash, but let's see what comes out.
        var result = JsonConvert.SerializeObject(message, settings);
        Console.WriteLine("ReferenceLoopHandling.Ignore output: " + result);

        var reparsed = STJ.JsonSerializer.Deserialize<JsonObject>(result)!;

        // This fails — "stageType" is corrupted to {"Options":{...}} instead of "sequence"
        reparsed["Input"]!["stageType"]!.GetValue<string>().ShouldBe("sequence");
    }

    [Fact]
    public void Step3_SystemTextJsonNodeConverter_produces_correct_json()
    {
        var message = BuildMessageAsDeserializedByOutbox("""
            {
              "name": "Test Job",
              "input": { "stageType": "sequence", "stages": [] }
            }
            """);

        var settings = new JsonSerializerSettings
        {
            Converters = { new SystemTextJsonNodeConverter() }
        };

        var result = JsonConvert.SerializeObject(message, settings);
        Console.WriteLine("Custom converter output: " + result);

        var reparsed = STJ.JsonSerializer.Deserialize<JsonObject>(result)!;
        reparsed["Input"]!["stageType"]!.GetValue<string>().ShouldBe("sequence");
    }
}
