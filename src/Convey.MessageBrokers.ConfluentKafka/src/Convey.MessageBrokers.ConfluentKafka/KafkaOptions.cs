using System.Collections.Generic;

namespace Convey.MessageBrokers.ConfluentKafka
{
    public class ProducerSettings
    {
        public string BootstrapServers { get; set; }
    }

    public class ConsumerSettings
    {
        public string BootstrapServers { get; set; }
        public string GroupId { get; set; }
    }

    public class Logger
    {
        public bool Enabled { get; set; }
    }

    public class Context
    {
        public bool Enabled { get; set; }
        public string Header { get; set; }
    }

    public class KafkaOptions
    {
        public ProducerSettings ProducerSettings { get; set; }
        public ConsumerSettings ConsumerSettings { get; set; }
        public string ServicePublishTopic { get; set; }
        public int Retries { get; set; }
        public int RetryInterval { get; set; }
        public Logger Logger { get; set; }
        public Context Context { get; set; }
        public string MessageTypeHeader { get; set; }
        public string MessageIdHeader { get; set; }
        public string CorrelationIdHeader { get; set; }
        public string SpanContextHeader { get; set; }

        public string GetMessageTypeHeader()
            => string.IsNullOrWhiteSpace(MessageTypeHeader) ? "messageType" : MessageTypeHeader;
        public string GetMessageIdHeader()
            => string.IsNullOrWhiteSpace(MessageIdHeader) ? "messageId" : MessageIdHeader;

        public string GetCorrelationIdHeader()
            => string.IsNullOrWhiteSpace(CorrelationIdHeader) ? "correlationId" : CorrelationIdHeader;

        public string GetSpanContextHeader()
            => string.IsNullOrWhiteSpace(SpanContextHeader) ? "span_context" : SpanContextHeader;
    }
}