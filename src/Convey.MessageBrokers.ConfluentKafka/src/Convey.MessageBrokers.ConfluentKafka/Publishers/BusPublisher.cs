using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Convey.MessageBrokers.ConfluentKafka.Exceptions;
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
        private readonly string _aggregateIdHeader;
        private readonly bool _loggerEnabled;
        private readonly int _publishTimeoutInMilliseconds;

        public BusPublisher(KafkaOptions kafkaOptions, KafkaDependentProducer<string, string> kafkaDependentProducer, ILogger<BusPublisher> logger)
        {
            _kafkaOptions = kafkaOptions;
            _kafkaDependentProducer = kafkaDependentProducer;
            _publishTimeoutInMilliseconds = kafkaOptions.PublishTimeoutInMilliseconds;

            _loggerEnabled = _kafkaOptions.Logger?.Enabled ?? false;
            _messageTypeHeader = _kafkaOptions.GetMessageTypeHeader();
            _messageIdHeader = _kafkaOptions.GetMessageIdHeader();
            _correlationIdHeader = _kafkaOptions.GetCorrelationIdHeader();
            _spanContextHeader = _kafkaOptions.GetSpanContextHeader();
            _aggregateIdHeader = _kafkaOptions.GetAggregateIdHeader();
            _logger = logger;
        }

        public Task PublishAsync<T>(T message, string messageId = null, string correlationId = null,
            string spanContext = null, object messageContext = null, IDictionary<string, object> headers = null)
            where T : class
        {
            var publishTopic = _kafkaOptions.ServicePublishTopic;

            var aggregateId = string.Empty;

            if (headers is { })
            {
                if (headers.Keys.Contains(_aggregateIdHeader))
                {
                    var aggregateIdObject = headers[_aggregateIdHeader];
                    aggregateId = aggregateIdObject as string;
                }
            }

            if (_loggerEnabled)
            {
                _logger.LogInformation($"After trying to get aggregateId for a message the value is: {aggregateId}");
            }

            var confluentMessageId = string.IsNullOrWhiteSpace(messageId)
                ? Guid.NewGuid().ToString("N")
                : messageId;

            var confluentCorrelationId = string.IsNullOrWhiteSpace(correlationId)
                ? Guid.NewGuid().ToString("N")
                : correlationId;

            var messageKey = string.IsNullOrWhiteSpace(aggregateId) 
                ? confluentMessageId 
                : aggregateId;

            var messageValue = JsonConvert.SerializeObject(message);

            var confluentMessage = new Message<string, string>
            {
                Key = messageKey,
                Value = messageValue,
                Timestamp = new Timestamp(DateTimeOffset.UtcNow),
                Headers = new Headers()
            };

            var messageTypeBody = Encoding.UTF8.GetBytes(message.GetType().Name);
            confluentMessage.Headers.Add(_messageTypeHeader, messageTypeBody);

            var messageIdBody = Encoding.UTF8.GetBytes(confluentMessageId);
            confluentMessage.Headers.Add(_messageIdHeader, messageIdBody);
            
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
                _logger.LogInformation($"Trying to publishing a message with Topic: '{publishTopic}' " + $"[id: '{confluentMessageId}', correlation id: '{confluentCorrelationId}']");
            }


            //throw new KafkaPersistenceException(confluentMessageId);
            CancellationTokenSource tokenSource = null;
            Task<DeliveryResult<string, string>> produceAsync = null;

            try
            {
                tokenSource = new CancellationTokenSource();
                tokenSource.CancelAfter(_publishTimeoutInMilliseconds);
                produceAsync = _kafkaDependentProducer.ProduceAsync(publishTopic, confluentMessage, tokenSource.Token);
                produceAsync.Wait(tokenSource.Token);
            }
            catch (OperationCanceledException e)
            {
                _logger.LogError($"_kafkaDependentProducer.ProduceAsync throwing OperationCanceledException for message for exceeding TimeSpan in Milliseconds :{_publishTimeoutInMilliseconds} with Topic: '{publishTopic}' " + $"[id: '{confluentMessageId}', correlation id: '{confluentCorrelationId}']. Exception:{e}");

                throw;
            }
            catch (Exception e)
            {
                _logger.LogError($"_kafkaDependentProducer.ProduceAsync throwing Exception for message with Topic: '{publishTopic}' " + $"[id: '{confluentMessageId}', correlation id: '{confluentCorrelationId}']. Exception:{e}");
                throw;
            }
            finally
            {
                tokenSource.Dispose();
            }

            if (produceAsync is null)
            {
                _logger.LogError($"Throwing KafkaPersistenceException due to non ability to call _kafkaDependentProducer.ProduceAsync method for message with Topic: '{publishTopic}' " + $"[id: '{confluentMessageId}', correlation id: '{confluentCorrelationId}']");
                //TODO: Add ability to notify the admin about failure to send event
                throw new KafkaPersistenceException(confluentMessageId);
            }
            
            var produceAsyncResult = produceAsync.Result;

            if (produceAsyncResult.Status == PersistenceStatus.NotPersisted)
            {
                _logger.LogError($"Throwing KafkaPersistenceException for message with Topic: '{publishTopic}' " + $"[id: '{confluentMessageId}', correlation id: '{confluentCorrelationId}']");
                //TODO: Add ability to notify the admin about failure to send event
                throw new KafkaPersistenceException(confluentMessageId);
            }

            if (_loggerEnabled)
            {
                _logger.LogInformation($"Published message with Topic: '{publishTopic}' " + $"[id: '{confluentMessageId}', correlation id: '{confluentCorrelationId}'] with persistence status:{produceAsyncResult.Status}");
            }

            //_logger.LogInformation($"Delay after publishing message with Topic: '{publishTopic}' " + $"[id: '{confluentMessageId}', correlation id: '{confluentCorrelationId}'] with persistence status:{produceAsyncResult.Status}");
            //Task.Delay(TimeSpan.FromMinutes(3)).Wait();
            
            return Task.CompletedTask;
            
        }
    }
}