using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Convey.MessageBrokers.Outbox.Messages;
using Convey.Persistence.MongoDB;
using Microsoft.Extensions.Logging;
using MongoDB.Driver;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Serialization;
using OpenTelemetry;
using OpenTelemetry.Context.Propagation;

namespace Convey.MessageBrokers.Outbox.Mongo.Internals
{
    internal sealed class MongoMessageOutbox : IMessageOutbox, IMessageOutboxAccessor
    {
        private static readonly JsonSerializerSettings SerializerSettings = new JsonSerializerSettings
        {
            ContractResolver = new CamelCasePropertyNamesContractResolver(),
            Converters = new List<JsonConverter>
            {
                new StringEnumConverter(new CamelCaseNamingStrategy())
            }
        };

        private const string EmptyJsonObject = "{}";
        private readonly IMongoSessionFactory _sessionFactory;
        private readonly IMongoRepository<InboxMessage, string> _inboxRepository;
        private readonly IMongoRepository<OutboxMessage, string> _outboxRepository;
        private readonly ILogger<MongoMessageOutbox> _logger;
        private readonly bool _transactionsEnabled;

        public bool Enabled { get; }

        
        private readonly TextMapPropagator _propagator;

        public MongoMessageOutbox(IMongoSessionFactory sessionFactory,
            IMongoRepository<InboxMessage, string> inboxRepository,
            IMongoRepository<OutboxMessage, string> outboxRepository,
            OutboxOptions options, ILogger<MongoMessageOutbox> logger)
        {
            _sessionFactory = sessionFactory;
            _inboxRepository = inboxRepository;
            _outboxRepository = outboxRepository;
            _logger = logger;
            _transactionsEnabled = !options.DisableTransactions;
            Enabled = options.Enabled;

            _propagator = Propagators.DefaultTextMapPropagator;
        }

        public async Task HandleAsync(string messageId, Func<Task> handler)
        {
            if (!Enabled)
            {
                _logger.LogWarning("Outbox is disabled, incoming messages won't be processed.");
                return;
            }

            if (string.IsNullOrWhiteSpace(messageId))
            {
                throw new ArgumentException("Message id to be processed cannot be empty.", nameof(messageId));
            }

            _logger.LogTrace($"Received a message with id: '{messageId}' to be processed.");
            if (await _inboxRepository.ExistsAsync(m => m.Id == messageId))
            {
                _logger.LogTrace($"Message with id: '{messageId}' was already processed.");
                return;
            }

            IClientSessionHandle session = null;
            if (_transactionsEnabled)
            {
                session = await _sessionFactory.CreateAsync();
                session.StartTransaction();
            }

            try
            {
                _logger.LogTrace($"Processing a message with id: '{messageId}'...");
                await handler();
                await _inboxRepository.AddAsync(new InboxMessage
                {
                    Id = messageId,
                    ProcessedAt = DateTime.UtcNow
                });

                if (session is {})
                {
                    await session.CommitTransactionAsync();
                }

                _logger.LogTrace($"Processed a message with id: '{messageId}'.");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"There was an error when processing a message with id: '{messageId}'.");
                if (session is {})
                {
                    await session.AbortTransactionAsync();
                }

                throw;
            }
            finally
            {
                session?.Dispose();
            }
        }

        public async Task SendAsync<T>(T message, string originatedMessageId = null, string messageId = null, string correlationId = null, string spanContext = null, object messageContext = null, IDictionary<string, object> headers = null)
        where T : class
        {
            ActivityContext parentContextToInject = default;
            if (Activity.Current != null)
            {
                parentContextToInject = Activity.Current.Context;
            }
            else
            {
                var parentContext = _propagator.Extract(default, headers, ExtractTraceContextFromOutboxMessageHeaders);
                parentContextToInject = parentContext.ActivityContext;
                Baggage.Current = parentContext.Baggage;
            }

            var mongodbCollectionName = _outboxRepository.Collection.CollectionNamespace.FullName;

            var activityName = $"{mongodbCollectionName} send";
            
            //NOTE: make sure that a parent activity is available (parentContext.ActivityContext.TraceId == new ActivityTraceId()) 
            using (var activity = parentContextToInject.TraceId == new ActivityTraceId() ? null : Extensions.MongoMessageOutboxActivitySource.StartActivity(activityName, ActivityKind.Producer, parentContextToInject))
            {

                // Depending on Sampling (and whether a listener is registered or not), the activity above may not be created.
                // If it is created, then propagate its context.
                // If it is not created, then propagate the Current context, if any.
                ActivityContext contextToInject = default;
                if (activity != null)
                {
                    activity?.SetTag("messaging.system", "outbox");
                    activity?.SetTag("messaging.destination", mongodbCollectionName);
                    activity?.SetTag("messaging.event", message.GetType().Name);
                    contextToInject = activity.Context;
                }
                else if (Activity.Current != null)
                {
                    contextToInject = Activity.Current.Context;
                }
                headers ??= new Dictionary<string, object>();

                headers.Add(Convey.MessageBrokers.Outbox.Extensions.Destination, mongodbCollectionName);
                headers.Add(Convey.MessageBrokers.Outbox.Extensions.EventName, message.GetType().Name);
                AddActivityContextToOutboxMessageHeader(contextToInject, headers);
                
                if (!Enabled)
                {
                    _logger.LogWarning("Outbox is disabled, outgoing messages won't be saved into the storage.");
                    return;
                }

                var outboxMessage = new OutboxMessage
                {
                    Id = string.IsNullOrWhiteSpace(messageId) ? Guid.NewGuid().ToString("N") : messageId,
                    OriginatedMessageId = originatedMessageId,
                    CorrelationId = correlationId,
                    SpanContext = spanContext,
                    SerializedMessageContext = messageContext is null ? EmptyJsonObject : JsonConvert.SerializeObject(messageContext, SerializerSettings),
                    MessageContextType = messageContext?.GetType().AssemblyQualifiedName,
                    Headers = (Dictionary<string, object>)headers,
                    SerializedMessage = message is null ? EmptyJsonObject : JsonConvert.SerializeObject(message, SerializerSettings),
                    MessageType = message?.GetType().AssemblyQualifiedName,
                    SentAt = DateTime.UtcNow
                };
                await _outboxRepository.AddAsync(outboxMessage);
            }
        }

        async Task<IReadOnlyList<OutboxMessage>> IMessageOutboxAccessor.GetUnsentAsync()
        {
            var outboxMessages = await _outboxRepository.FindAsync(om => om.ProcessedAt == null);
            return outboxMessages.Select(om =>
            {
                if (om.MessageContextType is {})
                {
                    var messageContextType = Type.GetType(om.MessageContextType);
                    om.MessageContext = JsonConvert.DeserializeObject(om.SerializedMessageContext, messageContextType,
                        SerializerSettings);
                }

                if (om.MessageType is {})
                {
                    var messageType = Type.GetType(om.MessageType);
                    om.Message = JsonConvert.DeserializeObject(om.SerializedMessage, messageType, SerializerSettings);
                }

                return om;
            }).ToList();
        }

        Task IMessageOutboxAccessor.ProcessAsync(OutboxMessage message)
            => _outboxRepository.Collection.UpdateOneAsync(
                Builders<OutboxMessage>.Filter.Eq(m => m.Id, message.Id),
                Builders<OutboxMessage>.Update.Set(m => m.ProcessedAt, DateTime.UtcNow));

        Task IMessageOutboxAccessor.ProcessAsync(IEnumerable<OutboxMessage> outboxMessages)
            => _outboxRepository.Collection.UpdateManyAsync(
                Builders<OutboxMessage>.Filter.In(m => m.Id, outboxMessages.Select(m => m.Id)),
                Builders<OutboxMessage>.Update.Set(m => m.ProcessedAt, DateTime.UtcNow));

        private void AddActivityContextToOutboxMessageHeader(ActivityContext activityContext, IDictionary<string, object> header)
        {
            _propagator.Inject(new PropagationContext(activityContext, Baggage.Current), header, InjectContextIntoOutboxMessageHeader);
        }

        private void InjectContextIntoOutboxMessageHeader(IDictionary<string, object> headers, string key, string value)
        {
            try
            {
                headers ??= new Dictionary<string, object>();
                headers.Add(key, value);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to inject trace context.");
            }
        }
        
        private IEnumerable<string> ExtractTraceContextFromOutboxMessageHeaders(IDictionary<string, object> headers, string key)
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
    }
}