using Convey.Types;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace Convey.Persistence.MongoDB.Tests;
public sealed record UserDocument(
        [property: BsonRepresentation(BsonType.String)] Guid Id,
        string Name) : IIdentifiable<Guid>;