using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using Confluent.Kafka;


namespace KafkaWrapper.Lib
{
    public class ConsumerWrapper
    {
        private readonly IDictionary<string, string> _consumerConfig;

        public ConsumerWrapper() : this("localhost") 
        {
        }

        public ConsumerWrapper(string hostname)
        {
            _consumerConfig = new Dictionary<string, string>() 
            { { "bootstrap.servers", hostname }, 
              {"group.id",  "any"} };
        }

        public void SubscribeOnTopic(string topic, Action<KeyValuePair<string, int>> action, CancellationToken cancellationToken)
        {
            var consumer = new ConsumerBuilder<string, int>(_consumerConfig)
                .SetKeyDeserializer(Deserializers.Utf8)
                .SetValueDeserializer(Deserializers.Int32).Build();
            consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(topic, 0, -1) });

            while (!cancellationToken.IsCancellationRequested)
            {
                var result = consumer.Consume(TimeSpan.FromMilliseconds(10));
                if(result != null)
                {
                    action(new KeyValuePair<string, int>(result.Message.Key, result.Message.Value));
                }
            }
            consumer.Close();
            consumer.Dispose();
        }
    }
}
