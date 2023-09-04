using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Convey.MessageBrokers.ConfluentKafka.Exceptions;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using OpenTelemetry;
using OpenTelemetry.Context.Propagation;

namespace Convey.MessageBrokers.ConfluentKafka.Publishers
{
    public sealed class BusPublisher : IBusPublisher
    {
        private readonly KafkaOptions _kafkaOptions; 
        private readonly KafkaDependentProducer<string, string> _kafkaDependentProducer;
        private readonly ILogger<BusPublisher> _logger;
        private readonly string _messageTypeHeader;
        private readonly string _messageIdHeader;
        private readonly string _correlationIdHeader;
        private readonly string _spanContextHeader;
        private readonly string _aggregateIdHeader;
        private readonly string _correlationContextHeader;
        private readonly bool _contextEnabled;
        private readonly bool _loggerEnabled;
        private readonly int _publishTimeoutInMilliseconds;

        private readonly TextMapPropagator _propagator;
        public BusPublisher(KafkaOptions kafkaOptions, KafkaDependentProducer<string, string> kafkaDependentProducer, ILogger<BusPublisher> logger)
        {
            _kafkaOptions = kafkaOptions;
            _kafkaDependentProducer = kafkaDependentProducer;
            _publishTimeoutInMilliseconds = kafkaOptions.PublishTimeoutInMilliseconds;

            _contextEnabled = _kafkaOptions.Context?.Enabled == true;
            _loggerEnabled = _kafkaOptions.Logger?.Enabled ?? false;
            _messageTypeHeader = _kafkaOptions.GetMessageTypeHeader();
            _messageIdHeader = _kafkaOptions.GetMessageIdHeader();
            _correlationIdHeader = _kafkaOptions.GetCorrelationIdHeader();
            _spanContextHeader = _kafkaOptions.GetSpanContextHeader();
            _aggregateIdHeader = _kafkaOptions.GetAggregateIdHeader();
            _correlationContextHeader = _kafkaOptions.GetCorrelationContextHeader();

            _logger = logger;

            _propagator = Propagators.DefaultTextMapPropagator;
        }

        public Task PublishAsync<T>(T message, string messageId = null, string correlationId = null, string spanContext = null, object messageContext = null, IDictionary<string, object> headers = null)
        where T : class
        {
            ActivityContext parentContextToInject = default;
            if (Activity.Current != null)
            {
                parentContextToInject = Activity.Current.Context;
                
                _propagator.Extract(default, headers, RemoveTraceContextFromHeaders);

                //if (_loggerEnabled)
                //{
                //    _logger.LogInformation($"Activity.Current found : {Activity.Current}");
                //    _logger.LogInformation($"Activity.Current TraceId : {Activity.Current.TraceId}");
                //    _logger.LogInformation($"Activity.Current SpanId : {Activity.Current.SpanId}");
                //}
            }
            else
            {
                var parentContext = _propagator.Extract(default, headers, ExtractTraceContextFromHeaders);
                Baggage.Current = parentContext.Baggage;
                parentContextToInject = parentContext.ActivityContext;

                _propagator.Extract(default, headers, RemoveTraceContextFromHeaders);

                //if (_loggerEnabled)
                //{
                //    _logger.LogInformation($"Activity.Current not found.");
                //    _logger.LogInformation($"Header extracted parentContext.ActivityContext TraceId : {parentContext.ActivityContext.TraceId}");
                //    _logger.LogInformation($"Header extracted parentContext.ActivityContext SpanId : {parentContext.ActivityContext.SpanId}");
                //}
            }
            var publishTopic = _kafkaOptions.ServicePublishTopic;
            var activityName = $"{publishTopic} send";

            if (_loggerEnabled)
            {
                _logger.LogInformation($"parentContextToInject.TraceId: { parentContextToInject.TraceId}");
            }
            
            //NOTE: make sure that a parent activity is available (parentContext.ActivityContext.TraceId == new ActivityTraceId()) 
            using (var activity = parentContextToInject.TraceId == new ActivityTraceId()? null: Extensions.ConfluentKafkaActivitySource.StartActivity(activityName, ActivityKind.Producer, parentContextToInject))
            {
                var aggregateId = string.Empty;

                if (_loggerEnabled)
                {
                    if (activity is not null)
                    {
                        _logger.LogInformation($"Kafka activity created TraceId: {activity.TraceId},  SpanId: {activity.SpanId}");
                    }
                    else
                    {
                        _logger.LogInformation($"Kafka activity not created.");
                    }
                }

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

                if (_contextEnabled)
                {
                    IncludeMessageContext(messageContext, confluentMessage);
                }

                // Depending on Sampling (and whether a listener is registered or not), the activity above may not be created.
                // If it is created, then propagate its context.
                // If it is not created, then propagate the Current context, if any.
                ActivityContext contextToInject = default;
                if (activity != null)
                {
                    // as per convention in opentelemetry specification
                    activity?.SetTag("peer.service", publishTopic);
                    activity?.SetTag("messaging.system", "kafka");
                    activity?.SetTag("messaging.destination_kind", "topic");
                    activity?.SetTag("messaging.destination", publishTopic);
                    activity?.SetTag("messaging.kafka.message_key", messageKey);
                    contextToInject = activity.Context;
                }
                else if (Activity.Current != null)
                {
                    contextToInject = Activity.Current.Context;
                }
                AddActivityToKafkaMessageHeader(contextToInject, confluentMessage);

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

        private void IncludeMessageContext(object context, Message<string, string> properties)
        {
            if (context is null) return;

            var serializedContext = JsonConvert.SerializeObject(context);
            var contextBody = Encoding.UTF8.GetBytes(serializedContext);
            properties.Headers.Add(_correlationContextHeader, contextBody);
        }

        private void AddActivityToKafkaMessageHeader(ActivityContext activityContext, Message<string, string> props)
        {
            _propagator.Inject(new PropagationContext(activityContext, Baggage.Current), props, InjectContextIntoKafkaMessageHeader);
        }

        private void InjectContextIntoKafkaMessageHeader(Message<string, string> props, string key, string value)
        {
            try
            {
                props.Headers ??= new Headers();
                var valueBody = Encoding.UTF8.GetBytes(value);
                props.Headers.Add(key, valueBody);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to inject trace context.");
            }
        }

        private IEnumerable<string> ExtractTraceContextFromHeaders(IDictionary<string, object> headers, string key)
        {
            try
            {
                if (headers.TryGetValue(key, out var value))
                {
                    if (value is string valueStr)
                    {
                        return new[] { valueStr };
                    }

                    throw new FormatException($"Type:{value.GetType()} of value:{value} not found to be of Type:{nameof(String)}.");

                }
            }
            catch (Exception ex)
            {
                _logger.LogError($"Failed to extract trace context: {ex}");
            }

            return Enumerable.Empty<string>();
        }

        private IEnumerable<string> RemoveTraceContextFromHeaders(IDictionary<string, object> headers, string key)
        {
            if (headers.ContainsKey(key)) headers.Remove(key);
            return Enumerable.Empty<string>();
        }
    }
}