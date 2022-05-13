using System;
using System.Threading;
using System.Collections.Generic;
using System.Text;
using KafkaWrapper.Lib;

namespace ConsumerConsoleApp
{
    public abstract class MessageHandlerBase : IDisposable
    {
        private readonly ConsumerWrapper _consumer;

        protected MessageHandlerBase()
        {
            _consumer = new ConsumerWrapper();
        }

        protected void Subscribe(string topicName, Action<KeyValuePair<string, int>> action)
        {
            _consumer.SubscribeOnTopic(topicName, (message) => action(message), CancellationToken.None);
        }

        public void Dispose()
        {
            _consumer.Dispose();
        }
    }
}
