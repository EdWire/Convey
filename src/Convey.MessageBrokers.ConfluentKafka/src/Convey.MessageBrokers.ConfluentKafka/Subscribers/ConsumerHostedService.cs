using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Convey.CQRS.Events;
using Convey.MessageBrokers.ConfluentKafka.Topics;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Polly;

namespace Convey.MessageBrokers.ConfluentKafka.Subscribers
{
    public class EventConsumerHostedService<TTopic> : BackgroundService
    {
        private readonly KafkaOptions _kafkaOptions;
        private readonly string _messageTypeHeader;
        private readonly string _messageIdHeader;
        private readonly string _correlationIdHeader;
        private readonly string _spanContextHeader;
        private readonly bool _loggerEnabled;
        private readonly int _retries;
        private readonly int _retryInterval;
        private IExceptionToMessageMapper _exceptionToMessageMapper;
        private ILogger<EventConsumerHostedService<TTopic>> Logger { get; }
        public ITopic Topic { get; }
        private ConsumerConfig ConsumerConfig { get; set; }
        private IConsumer<string, string> KafkaConsumer { get; }
        private IConfiguration Configuration { get; }

        private Dictionary<string, Type> Events { get; }
        private Dictionary<Type, Type> EventHandlerForEvent { get; }

        private IServiceProvider ServiceProvider { get; set; }

        CancellationTokenSource CancellationTokenSource { get; }

        private bool DisposeCalled { get; set; }


        public EventConsumerHostedService(IConfiguration configuration, KafkaOptions kafkaOptions,
            ILogger<EventConsumerHostedService<TTopic>> logger, ITopic consumerTopic)
        {
            _kafkaOptions = kafkaOptions;
            Configuration = configuration;
            Logger = logger;

            Topic = consumerTopic;

            ConsumerConfig = new ConsumerConfig
            {
                // Disable auto-committing of offsets.
                EnableAutoCommit = false
            };
            
            Configuration.GetSection("Kafka:ConsumerSettings").Bind(ConsumerConfig);
            ConsumerConfig.GroupId = $"{ConsumerConfig.GroupId}.{Topic.TopicName}";

            //this.topic = config.GetValue<List<string>>("Kafka:ServiceConsumerTopic").First();
            this.KafkaConsumer = new ConsumerBuilder<string, string>(ConsumerConfig).Build(); //TODO: Add error handler

            Logger.LogInformation($"Consumer BackgroundService for Topic:{Topic.TopicName} GroupId:{ConsumerConfig.GroupId}");
            
            Events = new Dictionary<string,Type>();
            EventHandlerForEvent = new Dictionary<Type, Type>();

            CancellationTokenSource = new CancellationTokenSource();
            DisposeCalled = false;
            
            _messageTypeHeader = _kafkaOptions.GetMessageTypeHeader();
            _messageIdHeader = _kafkaOptions.GetMessageIdHeader();
            _correlationIdHeader = _kafkaOptions.GetCorrelationIdHeader();
            _spanContextHeader = _kafkaOptions.GetSpanContextHeader();
            _loggerEnabled = _kafkaOptions.Logger?.Enabled ?? false;
            _retries = _kafkaOptions.Retries >= 0 ? _kafkaOptions.Retries : 3;
            _retryInterval = _kafkaOptions.RetryInterval > 0 ? _kafkaOptions.RetryInterval : 2;
        }

        #region override
        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            Logger.LogInformation($"EventConsumerHostedService is running.{Environment.NewLine}");

            return Task.CompletedTask;
        }

        public override async Task StopAsync(CancellationToken stoppingToken)
        {

            Logger.LogInformation("EventConsumerHostedService is stopping.");

            await base.StopAsync(stoppingToken);
        }

        public override void Dispose()
        {
            //NOTE: Disposed called twice. No info found about why dispose is called twice.
            if (DisposeCalled)
            {
                Logger.LogInformation("EventConsumerHostedService:Dispose called. Dispose already found to be called. Ignoring Dispose request.");
                return;
            }
            DisposeCalled = true;
            CancellationTokenSource.Cancel();
            //NOTE: Give the consumer a chance to stop before calling Close
            Task.Delay(TimeSpan.FromSeconds(5)).Wait();
            try
            {
                Logger.LogInformation("EventConsumerHostedService:Dispose called. KafkaConsumer.Close() is about to be called.");
                //NOTE: this throws error on application shutdown
                //this.KafkaConsumer.
                this.KafkaConsumer.Close(); // Commit offsets and leave the group cleanly.
            }
            catch (Exception e)
            {
                Logger.LogInformation($"EventConsumerHostedService:Dispose called. KafkaConsumer.Close() is called. Error:{e}");
            }

            try
            {
                Logger.LogInformation("EventConsumerHostedService:Dispose called. KafkaConsumer.Dispose() is about to be called.");
                this.KafkaConsumer.Dispose();
            }
            catch (Exception e)
            {
                Logger.LogInformation($"EventConsumerHostedService:Dispose called. KafkaConsumer.Dispose() is called. Error:{e}");
            }

            base.Dispose();
        }
        #endregion
        
        public void RegisterConsumerEventType(Type @event, Type handler)
        {
            if (@event is null)
            {
                throw new ArgumentNullException(nameof(@event));
            }

            if (handler is null)
            {
                throw new ArgumentNullException(nameof(handler));
            }

            if (Events.ContainsKey(@event.Name))
            {
                Logger.LogInformation($"EventConsumerHostedService:RegisterConsumerEventType called. EventType:{@event.Name} already exists. RegisterConsumerEventType Ignored.");
                return;
            }

            if (Events.ContainsKey(handler.Name))
            {
                Logger.LogInformation($"EventConsumerHostedService:RegisterConsumerEventType called. HandlerType:{handler.Name} already exists. RegisterConsumerEventType Ignored.");
                return;
            }

            Events.Add(@event.Name, @event);
            EventHandlerForEvent.Add(@event,handler);
        }
        public void Start(IServiceProvider serviceProvider)
        {
            ServiceProvider = serviceProvider;
            _exceptionToMessageMapper = ServiceProvider.GetService<IExceptionToMessageMapper>() ?? new EmptyExceptionToMessageMapper();
            //ConsumerThread = new Thread(() => StartConsumerLoop(CancellationToken));
            var cancellationToken = CancellationTokenSource.Token;

            Task.Run(() =>
            {
                StartConsumerLoop(cancellationToken);
            }, cancellationToken);
        }
        private void StartConsumerLoop(CancellationToken cancellationToken)
        {
            KafkaConsumer.Subscribe(Topic.TopicName);

            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var cr = this.KafkaConsumer.Consume(cancellationToken);

                    string spanContext = GetMessageSpanContext(cr.Message);
                    var messageProperties = GetMessageProperties(cr.Message);

                    if (_loggerEnabled)
                    {
                        Logger.LogTrace($"Received a message with id: '{messageProperties.MessageId}', " +
                                               $"correlation id: '{messageProperties.CorrelationId}', unix timestamp: {messageProperties.Timestamp} " +
                                               $"from Kafka Topic: {Topic.TopicName}.");
                    }

                    if (!Events.ContainsKey(cr.Message.Key))
                    {
                        if (_loggerEnabled)
                        {
                            Logger.LogWarning($"Consumer warning, the event type is not found in the registered event types. Incoming event type:{cr.Message.Key}. This event will be ignored.");
                        }
                        try
                        {
                            KafkaConsumer.Commit(cr);
                        }
                        catch (KafkaException e)
                        {
                            Logger.LogError($"Commit error: {e.Error.Reason}");
                            //TODO: Need to create a strategy to deal with commit failures
                            Logger.LogError($"Irrecoverable error encountered. Decision Taken to stop Confluent consumer for topic: {Topic.TopicName}, ConsumerGroupId: {ConsumerConfig.GroupId}. Admin should fix error and restart service.");
                            break;
                        }
                        continue;
                    }

                    var registeredEventType = Events[cr.Message.Key];
                    if (!EventHandlerForEvent.ContainsKey(registeredEventType))
                    {
                        Logger.LogError($"The EventHandler For  registered event type:{registeredEventType} is not found in the registered EventHandler types. Corrupted state.");
                        //TODO: Need to create a strategy to deal with commit failures
                        Logger.LogError($"Irrecoverable error encountered. Decision Taken to stop Confluent consumer for topic: {Topic.TopicName}, ConsumerGroupId: {ConsumerConfig.GroupId}. Admin should fix error and restart service.");
                        break;
                    }

                    var eventHandlerType = EventHandlerForEvent[registeredEventType];
                    if (_loggerEnabled)
                    {
                        Logger.LogInformation($"Consumer info, the incoming event type is:{cr.Message.Key}, the registered event type : {registeredEventType.Name} and the registered EventHandler type:{eventHandlerType}");
                    }

                    if (_loggerEnabled)
                    {
                        Logger.LogInformation($"Consumer info, the incoming event is about to be deserialized using registered event type");
                    }
                        
                    var deserializeEvent = JsonConvert.DeserializeObject(cr.Message.Value, registeredEventType);
                    if (_loggerEnabled)
                    {
                        Logger.LogInformation($"Consumer info, the incoming event is successfully deserialized using registered event type");
                    }

                    using var scope = ServiceProvider.CreateScope();
                    object eventHandlerObject;
                    try
                    {
                        eventHandlerObject = scope.ServiceProvider.GetRequiredService(eventHandlerType);
                    }
                    catch (InvalidOperationException e)
                    {
                        //NOTE: Following is considered an error as the event is found registered but problem is in resolving eventHandler. This could be due to error in code.
                        Logger.LogError($"The method HandleAsync for EventHandler Type:{eventHandlerType} is not found. Error:{e}");
                        //TODO: Need to create a strategy to deal with commit failures
                        Logger.LogError($"Irrecoverable error encountered. Decision Taken to stop Confluent consumer for topic: {Topic.TopicName}, ConsumerGroupId: {ConsumerConfig.GroupId}. Admin should fix error and restart service.");
                        break;
                    }
                    catch (Exception e)
                    {
                        //NOTE: Following is considered an error as the event is found registered but problem is in resolving eventHandler. This could be due to error in code.
                        Logger.LogError($"The method HandleAsync for EventHandler Type:{eventHandlerType} is not found. Error:{e}");
                        //TODO: Need to create a strategy to deal with commit failures
                        Logger.LogError($"Irrecoverable error encountered. Decision Taken to stop Confluent consumer for topic: {Topic.TopicName}, ConsumerGroupId: {ConsumerConfig.GroupId}. Admin should fix error and restart service.");
                        break;
                    }

                    var parameterTypes = new Type[] { registeredEventType };
                    var eventHandlerMethodInfo = eventHandlerType.GetMethod("HandleAsync", parameterTypes);

                    if (eventHandlerMethodInfo is null)
                    {
                        Logger.LogError($"The Class Method HandleAsync for EventHandler Type:{eventHandlerType} is not found. Check the Type Implementation for the missing method");
                        //TODO: Need to create a strategy to deal with commit failures
                        Logger.LogError($"Irrecoverable error encountered. Decision Taken to stop Confluent consumer for topic: {Topic.TopicName}, ConsumerGroupId: {ConsumerConfig.GroupId}. Admin should fix error and restart service.");
                        break;
                    }
                    
                    var messagePropertiesAccessor = scope.ServiceProvider.GetRequiredService<IMessagePropertiesAccessor>();
                    messagePropertiesAccessor.MessageProperties = messageProperties;
                    //TODO: to be tested if this is scope based singleton or not 
                    
                    //TODO: CorrelationContext is not implement as most probably it is used in plugins 

                    try
                    {
                        var exception = TryHandleAsync(deserializeEvent, eventHandlerObject, eventHandlerMethodInfo, messageProperties.MessageId, messageProperties.CorrelationId);
                        if (exception is null)
                        {
                            try
                            {
                                KafkaConsumer.Commit(cr);
                            }
                            catch (KafkaException e)
                            {
                                Logger.LogError($"Commit error: {e.Error.Reason}");
                                //TODO: Need to create a strategy to deal with commit failures
                                Logger.LogError($"Irrecoverable error encountered. Decision Taken to stop Confluent consumer for topic: {Topic.TopicName}, ConsumerGroupId: {ConsumerConfig.GroupId}. Admin should fix error and restart service.");
                                break;
                            }
                        }
                        else
                        {
                            Logger.LogError($"HandleAsync error : {exception.Message}");
                            //TODO: Need to create a strategy to deal with handler exceptions
                            Logger.LogError($"Irrecoverable error encountered. Decision Taken to stop Confluent consumer for topic: {Topic.TopicName}, ConsumerGroupId: {ConsumerConfig.GroupId}. Admin should fix error and restart service.");
                            break;
                        }

                    }
                    catch (Exception e)
                    {
                        Logger.LogError(e.ToString());
                        break;
                    }
                }
                catch (OperationCanceledException e)
                {
                    Logger.LogInformation($"OperationCanceledException error: {e}");
                    break;
                }
                catch (ConsumeException e)
                {
                    // Consumer errors should generally be ignored (or logged) unless fatal.
                    Logger.LogInformation($"Consume error: {e.Error.Reason}");

                    if (e.Error.IsFatal)
                    {
                        Logger.LogError($"Irrecoverable error encountered. Decision Taken to stop Confluent consumer for topic: {Topic.TopicName}, ConsumerGroupId: {ConsumerConfig.GroupId}. Admin should fix error and restart service.");
                        // https://github.com/edenhill/librdkafka/blob/master/INTRODUCTION.md#fatal-consumer-errors
                        break;
                    }
                }
                catch (Exception e)
                {
                    Logger.LogError($"Unexpected error: {e}");
                    Logger.LogError($"Irrecoverable error encountered. Decision Taken to stop Confluent consumer for topic: {Topic.TopicName}, ConsumerGroupId: {ConsumerConfig.GroupId}. Admin should fix error and restart service.");
                    break;
                }
            }
        }

        private Exception TryHandleAsync(object message, object messageHandler, MethodInfo messageHandlerMethodInfo, string messageId, string correlationId)
        {
            var currentRetry = 0;
            var messageName = message.GetType().Name;

            //TODO: Add Exponential back off
            var retryPolicy = Policy
                .Handle<Exception>()
                .WaitAndRetry(_retries, i => TimeSpan.FromSeconds(_retryInterval));

            return retryPolicy.Execute(() =>
            {
                try
                {
                    var retryMessage = currentRetry == 0 ? string.Empty : $"Retry: {currentRetry}'.";

                    var preLogMessage = $"Handling a message: '{messageName}'. {retryMessage}";

                    Logger.LogInformation(preLogMessage);

                    Logger.LogInformation($"Consumer info, the bus subscriber registered handle is about to be called");
                    object[] parameters = new object[] { message };
                    var handleAsyncTask = (Task)messageHandlerMethodInfo.Invoke(messageHandler, parameters);

                    if (handleAsyncTask is null)
                    {
                        var errorMessage = $"Unable to handle a message: '{messageName}' [id: '{messageId}'] with correlation id: '{correlationId}', retry {currentRetry - 1}/{_retries}... " +
                                           "The expected Task object was not returned from HandleAsync MethodInfo object";
                        Logger.LogError(errorMessage);
                        throw new Exception(errorMessage);
                    }

                    handleAsyncTask.Wait();
                    Logger.LogInformation($"Consumer info, the bus subscriber registered handle is successfully called");

                    var postLogMessage = $"Handled a message: '{messageName}'. {retryMessage}";
                    Logger.LogInformation(postLogMessage);

                    return null;
                }
                catch (Exception ex)
                {
                    currentRetry++;
                    Logger.LogError(ex, ex.Message);
                    var rejectedEvent = _exceptionToMessageMapper.Map(ex, message);
                    if (rejectedEvent is null)
                    {
                        var errorMessage = $"Unable to handle a message: '{messageName}' [id: '{messageId}'] with correlation id: '{correlationId}', retry {currentRetry - 1}/{_retries}...";

                        if (currentRetry > 1)
                        {
                            Logger.LogError(errorMessage);
                        }

                        if (currentRetry - 1 < _retries)
                        {
                            throw new Exception(errorMessage, ex); // Most probably causes retry
                        }

                        Logger.LogError($"Handling a message: '{messageName}' [id: '{messageId}'] with correlation id: '{correlationId}' failed.");
                        return new Exception($"Handling a message: '{messageName}' [id: '{messageId}'] with correlation id: '{correlationId}' failed.", ex);
                    }

                    //TODO: rejected messages postponed until further notice
                    //await _busClient.PublishAsync(rejectedEvent, ctx => ctx.UseMessageContext(correlationContext));
                    //Logger.LogWarning($"Published a rejected event: '{rejectedEvent.GetMessageName()}' " + $"for the message: '{messageName}'.");
                    //return new Exception($"Handling a message: '{messageName}' failed and rejected event: " + $"'{rejectedEvent.GetMessageName()}' was published.", ex);
                    return new Exception($"Handling a message: '{messageName}' [id: '{messageId}'] with correlation id: '{correlationId}' failed.", ex);
                }
            });

        }

        private class EmptyExceptionToMessageMapper : IExceptionToMessageMapper
        {
            public object Map(Exception exception, object message) => null;
        }

        private string GetMessageSpanContext(Message<string, string> message)
        {
            string spanContext = null; //NOTE: This value is optional

            var messageHeaders = message.Headers;

            foreach (var messageHeader in messageHeaders)
            {
                if (messageHeader.Key == _spanContextHeader)
                {
                    spanContext = Encoding.UTF8.GetString(messageHeader.GetValueBytes());
                }
            }

            return spanContext;
        }

        private MessageProperties GetMessageProperties(Message<string, string> message)
        {
            var messageHeaders = message.Headers;
            var messageProperties = new MessageProperties {Timestamp = message.Timestamp.UnixTimestampMs, Headers = new Dictionary<string, object>()};
            foreach (var messageHeader in messageHeaders)
            {
                if (messageHeader.Key == _messageIdHeader)
                {
                    messageProperties.MessageId = Encoding.UTF8.GetString(messageHeader.GetValueBytes());
                }
                if (messageHeader.Key == _correlationIdHeader)
                {
                    messageProperties.CorrelationId = Encoding.UTF8.GetString(messageHeader.GetValueBytes());
                }

                messageProperties.Headers.Add(messageHeader.Key, messageHeader.GetValueBytes());
            }

            //TODO: id messageProperties.MessageId and messageProperties.CorrelationId required field?
            
            return messageProperties;
        }
    }
}
