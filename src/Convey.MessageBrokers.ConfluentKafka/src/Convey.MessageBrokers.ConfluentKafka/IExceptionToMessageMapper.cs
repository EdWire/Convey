using System;

namespace Convey.MessageBrokers.ConfluentKafka
{
    public interface IExceptionToMessageMapper
    {
        object Map(Exception exception, object message);
    }
}