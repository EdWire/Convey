using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using OpenTelemetry;
using OpenTelemetry.Context.Propagation;

namespace Convey.MessageBrokers.Outbox.Processors;

internal sealed class OutboxProcessor : IHostedService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly IBusPublisher _publisher;
    private readonly OutboxOptions _options;
    private readonly ILogger<OutboxProcessor> _logger;
    private readonly TimeSpan _interval;
    private readonly OutboxType _type;
    private Timer _timer;
    static readonly SemaphoreSlim SemaphoreSlim = new(1, 1);

    private readonly TextMapPropagator _propagator;

    public OutboxProcessor(IServiceProvider serviceProvider, IBusPublisher publisher, OutboxOptions options,
        ILogger<OutboxProcessor> logger)
    {
        if (options.Enabled && options.IntervalMilliseconds <= 0)
        {
            throw new Exception($"Invalid outbox interval: {options.IntervalMilliseconds} ms.");
        }

        _type = OutboxType.Sequential;
        if (!string.IsNullOrWhiteSpace(options.Type))
        {
            if (!Enum.TryParse<OutboxType>(options.Type, true, out var outboxType))
            {
                throw new ArgumentException($"Invalid outbox type: '{_type}', " +
                                            $"valid types: '{OutboxType.Sequential}', '{OutboxType.Parallel}'.");
            }

            _type = outboxType;
        }

        _serviceProvider = serviceProvider;
        _publisher = publisher;
        _options = options;
        _logger = logger;            
        _interval = TimeSpan.FromMilliseconds(options.IntervalMilliseconds);
        _propagator = Propagators.DefaultTextMapPropagator;
        if (options.Enabled)
        {
            _logger.LogInformation($"Outbox is enabled, type: '{_type}', message processing every {options.IntervalMilliseconds} ms.");
            return;
        }

        _logger.LogInformation("Outbox is disabled.");
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (!_options.Enabled)
        {
            return Task.CompletedTask;
        }

        _timer = new Timer(SendOutboxMessages, null, _interval, _interval);
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        if (!_options.Enabled)
        {
            return Task.CompletedTask;
        }

        _timer?.Change(Timeout.Infinite, 0);
        _timer?.Dispose();
        return Task.CompletedTask;
    }

    private void SendOutboxMessages(object state)
    {
        _ = SendOutboxMessagesAsync();
    }

    private async Task SendOutboxMessagesAsync()
    {
        string jobId = Guid.NewGuid().ToString("N");

        _logger.LogTrace($"SendOutboxMessagesAsync start time for outbox messages... [job id: '{jobId}', interval: {_interval}], start time: {DateTimeOffset.UtcNow.ToString("MM/dd/yyyy HH:mm:ss.fff")}");
        try
        {
            _logger.LogTrace($"Trying to obtain lock for outbox messages... [job id: '{jobId}']");
            await SemaphoreSlim.WaitAsync();

            _logger.LogTrace($"Obtained lock for outbox messages... [job id: '{jobId}']");

            _logger.LogTrace($"Stopping timer for outbox messages... [job id: '{jobId}']");
            _timer?.Change(Timeout.Infinite, Timeout.Infinite);
                
            _logger.LogTrace($"Started processing outbox messages... [job id: '{jobId}']");
            var stopwatch = new Stopwatch();
            stopwatch.Start();
            using var scope = _serviceProvider.CreateScope();
            var outbox = scope.ServiceProvider.GetRequiredService<IMessageOutboxAccessor>();

            _logger.LogTrace($"Start time for unsent messages of outbox messages... [job id: '{jobId}', interval: {_interval}], start time: {DateTimeOffset.UtcNow.ToString("MM/dd/yyyy HH:mm:ss.fff")}");
            var messages = await outbox.GetUnsentAsync();
            _logger.LogTrace($"End time for unsent messages of outbox messages... [job id: '{jobId}', interval: {_interval}], end time: {DateTimeOffset.UtcNow.ToString("MM/dd/yyyy HH:mm:ss.fff")}");

            _logger.LogTrace($"Found {messages.Count} unsent messages in outbox [job ID: '{jobId}'].");
            if (!messages.Any())
            {
                _logger.LogTrace($"No messages to be processed in outbox [job ID: '{jobId}'].");
                return;
            }

            foreach (var message in messages.OrderBy(m => m.SentAt))
            {
                message.Headers ??= new Dictionary<string, object>();

                var destinationName = "Unknown";
                var eventName = message.GetType().Name;

                if (message.Headers.ContainsKey(Extensions.Destination) && message.Headers[Extensions.Destination] is string)
                {
                    destinationName = message.Headers[Extensions.Destination] as string;
                    message.Headers.Remove(Extensions.Destination);
                }

                if (message.Headers.ContainsKey(Extensions.EventName) && message.Headers[Extensions.EventName] is string)
                {
                    eventName = message.Headers[Extensions.EventName] as string;
                    message.Headers.Remove(Extensions.EventName);
                }

                var parentContext = _propagator.Extract(default, message.Headers, ExtractTraceContextFromOutboxMessageHeaders);
                Baggage.Current = parentContext.Baggage;

                var activityName = $"{destinationName} receive";

                //NOTE: make sure that a parent activity is available (parentContext.ActivityContext.TraceId == new ActivityTraceId()) 
                using (var activity = parentContext.ActivityContext.TraceId== new ActivityTraceId() ? null: Extensions.MessageOutboxActivitySource.StartActivity(activityName, ActivityKind.Consumer, parentContext.ActivityContext))
                {
                    //Add Tags to the Activity
                    activity?.SetTag("messaging.system", "outbox");
                    activity?.SetTag("messaging.destination", destinationName);
                    activity?.SetTag("messaging.event", eventName);

                    await _publisher.PublishAsync(message.Message, message.Id, message.CorrelationId, message.SpanContext, message.MessageContext, message.Headers);
                        
                    if (_type == OutboxType.Sequential)
                    {
                        await outbox.ProcessAsync(message);
                    }
                }
            }

            if (_type == OutboxType.Parallel)
            {
                await outbox.ProcessAsync(messages);
            }

            stopwatch.Stop();
            _logger.LogTrace($"Processed {messages.Count} outbox messages in {stopwatch.ElapsedMilliseconds} ms [job ID: '{jobId}'].");
        }
        finally
        {
            _logger.LogTrace($"Trying to release lock and reset timer for outbox messages... [job id: '{jobId}', interval: {_interval}]");

            try
            {
                SemaphoreSlim.Release();
            }
            catch (Exception e)
            {
                _logger.LogError(e.ToString());
                //throw;
            }

            _logger.LogTrace($"Released lock and reset timer for outbox messages... [job id: '{jobId}', interval: {_interval}]");
                
            _logger.LogTrace($"Starting timer for outbox messages... [job id: '{jobId}', dueTime:{_interval}, period:{_interval}]");
            _timer?.Change(_interval, _interval);

            _logger.LogTrace($"SendOutboxMessagesAsync end time for outbox messages... [job id: '{jobId}', interval: {_interval}], end time: {DateTimeOffset.UtcNow.ToString("MM/dd/yyyy HH:mm:ss.fff")}");
        }
    }

    private enum OutboxType
    {
        Sequential,
        Parallel
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