using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace Convey.MessageBrokers.ConfluentKafka.Publishers
{
    internal sealed class BusPublisher : IBusPublisher
    {
        private readonly KafkaOptions _kafkaOptions; 
        private readonly KafkaDependentProducer<string, string> _kafkaDependentProducer;
        private readonly ILogger<BusPublisher> _logger;
        private readonly string _messageTypeHeader;
        private readonly string _messageIdHeader;
        private readonly string _correlationIdHeader;
        private readonly string _spanContextHeader;
        private readonly bool _loggerEnabled;

        public BusPublisher(KafkaOptions kafkaOptions, KafkaDependentProducer<string, string> kafkaDependentProducer, ILogger<BusPublisher> logger)
        {
            _kafkaOptions = kafkaOptions;
            _kafkaDependentProducer = kafkaDependentProducer;
            _loggerEnabled = _kafkaOptions.Logger?.Enabled ?? false;
            _messageTypeHeader = _kafkaOptions.GetMessageTypeHeader();
            _messageIdHeader = _kafkaOptions.GetMessageIdHeader();
            _correlationIdHeader = _kafkaOptions.GetCorrelationIdHeader();
            _spanContextHeader = _kafkaOptions.GetSpanContextHeader();
            _logger = logger;
        }

        public Task PublishAsync<T>(T message, string messageId = null, string correlationId = null,
            string spanContext = null, object messageContext = null, IDictionary<string, object> headers = null)
            where T : class
        {
            var publishTopic = _kafkaOptions.ServicePublishTopic;
            
            var messageKey = message.GetType().Name;
            var messageValue = JsonConvert.SerializeObject(message);

            var confluentMessage = new Message<string, string>
            {
                Key = messageKey,
                Value = messageValue,
                Timestamp = new Timestamp(DateTimeOffset.UtcNow),
                Headers = new Headers()
            };

            var messageTypeBody = Encoding.UTF8.GetBytes(messageKey);
            confluentMessage.Headers.Add(_messageTypeHeader, messageTypeBody);

            var confluentMessageId = string.IsNullOrWhiteSpace(messageId)
                ? Guid.NewGuid().ToString("N")
                : messageId;
            var messageIdBody = Encoding.UTF8.GetBytes(confluentMessageId);
            confluentMessage.Headers.Add(_messageIdHeader, messageIdBody);

            var confluentCorrelationId = string.IsNullOrWhiteSpace(correlationId)
                ? Guid.NewGuid().ToString("N")
                : correlationId;
            var correlationIdBody = Encoding.UTF8.GetBytes(confluentCorrelationId);
            confluentMessage.Headers.Add(_correlationIdHeader, correlationIdBody);

            if (!string.IsNullOrWhiteSpace(spanContext))
            {
                var spanContextBody = Encoding.UTF8.GetBytes(spanContext);
                confluentMessage.Headers.Add(_spanContextHeader, spanContextBody);
            }

            if (headers is { })
            {
                foreach (var (key, value) in headers)
                {
                    if (string.IsNullOrWhiteSpace(key) || value is null)
                    {
                        continue;
                    }
                    var valueBody = Encoding.UTF8.GetBytes((string)value); //TODO: currently only support string type objects
                    confluentMessage.Headers.Add(key, valueBody);
                }
            }

            if (_loggerEnabled)
            {
                _logger.LogInformation($"Publishing a message with Topic: '{publishTopic}' " +
                                 $"[id: '{confluentMessageId}', correlation id: '{confluentCorrelationId}']");
            }

            return _kafkaDependentProducer.ProduceAsync(publishTopic, confluentMessage);
        }
    }
}