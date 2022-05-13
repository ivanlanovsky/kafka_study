using System;
using System.Collections.Generic;
using System.Text;

namespace ConsumerConsoleApp
{
    public class SimpleMessageHandler : MessageHandlerBase
    {
        public SimpleMessageHandler() : base()
        {
        }

        new public void Subscribe(string topicName, Action<KeyValuePair<string, int>> action)
        {
            base.Subscribe(topicName, action);
        }
    }
}
