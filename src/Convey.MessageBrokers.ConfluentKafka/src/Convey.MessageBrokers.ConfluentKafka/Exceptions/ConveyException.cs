using System;

namespace Convey.MessageBrokers.ConfluentKafka.Exceptions
{
    public class ConveyException : Exception
    {
        public virtual string Code { get; }

        protected ConveyException(string message) : base(message) { }
    }
}