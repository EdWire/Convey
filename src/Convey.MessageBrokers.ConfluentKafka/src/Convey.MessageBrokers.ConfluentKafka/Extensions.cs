using System;
using System.Diagnostics;
using System.Reflection;
using Convey.CQRS.Events;
using Convey.CQRS.Commands;
using Convey.MessageBrokers.ConfluentKafka.Publishers;
using Convey.MessageBrokers.ConfluentKafka.Subscribers;
using Convey.MessageBrokers.ConfluentKafka.Topics;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Convey.MessageBrokers.ConfluentKafka
{
    public static class Extensions
    {
        private const string SectionName = "Kafka";

        public static IConveyBuilder AddExceptionToMessageMapper<T>(this IConveyBuilder builder)
            where T : class, IExceptionToMessageMapper
        {
            builder.Services.AddTransient<IExceptionToMessageMapper, T>();

            return builder;
        }
        
        public static IConveyBuilder AddConfluentKafka(this IConveyBuilder builder)
        {
            var options = builder.GetOptions<KafkaOptions>(SectionName);
            builder.Services.AddSingleton(options);
            builder.Services.AddSingleton<KafkaClientHandle>();
            builder.Services.AddSingleton<KafkaDependentProducer<string, string>>();
            
            builder.Services.AddTransient<IBusPublisher, BusPublisher>();
            
            builder.Services.AddSingleton<ICorrelationContextAccessor>(new CorrelationContextAccessor());
            builder.Services.AddSingleton<IMessagePropertiesAccessor>(new MessagePropertiesAccessor());

            return builder;
        }
        public static IConveyBuilder AddConfluentKafkaEventConsumer<TTopic>(this IConveyBuilder builder, ITopic consumerTopic) where TTopic:class,ITopic
        {
            //var options = builder.GetOptions<KafkaOptions>(SectionName);

            var serviceProvider = builder.Services.BuildServiceProvider();
            //var tracer = serviceProvider.GetService<ITracer>();
            var options = serviceProvider.GetService<KafkaOptions>();
            var configuration = serviceProvider.GetService<IConfiguration>();
            var logger = serviceProvider.GetService<ILogger<EventConsumerHostedService<TTopic>>>();

            var eventConsumerHostedService = new EventConsumerHostedService<TTopic>(configuration, options, logger, consumerTopic);
            builder.Services.AddSingleton<EventConsumerHostedService<TTopic>>(_ => eventConsumerHostedService);
            builder.Services.AddHostedService<EventConsumerHostedService<TTopic>>(_ => eventConsumerHostedService);

            return builder;
        }
        public static IConveyBuilder AddConfluentKafkaEventConsumerEvent<TTopic,TEvent>(this IConveyBuilder builder)
            where TTopic : class, ITopic
            where TEvent : class, IEvent
        {
            //var options = builder.GetOptions<KafkaOptions>(SectionName);

            var eventType = typeof(TEvent);
            var eventHandlerType = typeof(IEventHandler<TEvent>);

            var serviceProvider = builder.Services.BuildServiceProvider();
            var eventConsumerHostedService = serviceProvider.GetService<EventConsumerHostedService<TTopic>>();

            // ReSharper disable once PossibleNullReferenceException
            eventConsumerHostedService.RegisterConsumerEventType(eventType, eventHandlerType);

            return builder;
        }

        public static IConveyBuilder AddConfluentKafkaEventConsumerCommand<TTopic, TCommand>(this IConveyBuilder builder)
            where TTopic : class, ITopic
            where TCommand : class, ICommand
        {
            //var options = builder.GetOptions<KafkaOptions>(SectionName);

            var eventType = typeof(TCommand);
            var eventHandlerType = typeof(ICommandHandler<TCommand>);

            var serviceProvider = builder.Services.BuildServiceProvider();
            var eventConsumerHostedService = serviceProvider.GetService<EventConsumerHostedService<TTopic>>();

            // ReSharper disable once PossibleNullReferenceException
            eventConsumerHostedService.RegisterConsumerEventType(eventType, eventHandlerType);

            return builder;
        }

        public static IApplicationBuilder UseConfluentKafkaConsumerTopic<TTopic>(this IApplicationBuilder app)
        {
            var serviceProvider = app.ApplicationServices;
            var eventConsumerHostedService = serviceProvider.GetService<EventConsumerHostedService<TTopic>>();

            if (eventConsumerHostedService is null)
            {
                throw new ArgumentNullException(nameof(EventConsumerHostedService<TTopic>), $"Expected Confluent Kafka EventConsumerHostedService not found. The provide TTopic is of type:{typeof(TTopic)}");
            }

            eventConsumerHostedService.Start(serviceProvider);

            return app;
        }

        public static readonly ActivitySource ConfluentKafkaActivitySource = new(Assembly.GetAssembly(typeof(Convey.MessageBrokers.ConfluentKafka.Extensions)).GetName().Name);
    }
}