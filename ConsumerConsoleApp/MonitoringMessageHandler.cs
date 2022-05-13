using System;
using System.Threading;
using System.Collections.Generic;
using System.Text;
using KafkaWrapper.Lib;

namespace ConsumerConsoleApp
{
    public class MonitoringMessageHandler : MessageHandlerBase
    {
        
        private IDictionary<string, int> storage;
        private IDictionary<string, int> counter;
        

        public MonitoringMessageHandler() : base()
        {
            storage = new Dictionary<string, int>();
            counter = new Dictionary<string, int>();
        }

        public void Subscribe(string topic)
        {
            base.Subscribe(topic, Handle);
        }

        private void Handle(KeyValuePair<string, int> message)
        {
            if (!counter.ContainsKey(message.Key))
            {
                counter.Add(new KeyValuePair<string, int>(message.Key, 1));
            }
            else
            {
                counter[message.Key] += 1;
                if(counter[message.Key] % 10 != 0)
                {
                    return;
                }
                if (!storage.ContainsKey(message.Key))
                {
                    storage.Add(message);
                }
                else
                {
                    storage[message.Key] = message.Value;
                }
                var strBuilder = new StringBuilder();
                foreach (var tuple in storage)
                {
                    strBuilder.Append($"{tuple.Key} : {tuple.Value} | ");
                }
                Console.Write("\r{0}    ", strBuilder.ToString());
            }
        }
    }
}
