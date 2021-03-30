using System;

namespace Convey.MessageBrokers.ConfluentKafka.Exceptions
{
    public class KafkaPersistenceException : ConveyException
    {
        public override string Code { get; } = "kafka_message_not_persisted";
        public string Id { get; }
        public KafkaPersistenceException(string id) : base($"kafka with id: {id} was not persisted to the broker.")
            => Id = id;
    }
}
