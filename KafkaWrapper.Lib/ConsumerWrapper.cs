using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using Confluent.Kafka;


namespace KafkaWrapper.Lib
{
    public class ConsumerWrapper : IDisposable
    {
        private readonly IConsumer<string, int> _consumer;

        public ConsumerWrapper() : this("localhost") 
        {
        }

        public ConsumerWrapper(string hostname)
        {
            IDictionary<string, string> _consumerConfig = new Dictionary<string, string>() 
            { { "bootstrap.servers", hostname }, 
              {"group.id",  "any"} };
            _consumer = new ConsumerBuilder<string, int>(_consumerConfig)
                .SetKeyDeserializer(Deserializers.Utf8)
                .SetValueDeserializer(Deserializers.Int32).Build();
        }

        public void SubscribeOnTopic(string topic, Action<KeyValuePair<string, int>> action, CancellationToken cancellationToken)
        {
            _consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(topic, 0, -1) });

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    var result = _consumer.Consume(TimeSpan.FromMilliseconds(10));
                    if(result != null)
                    {
                        action(new KeyValuePair<string, int>(result.Message.Key, result.Message.Value));
                    }
                }
            }
            catch (OperationCanceledException)
            {
                _consumer.Close();
            }
        }

        public void Dispose()
        {
            _consumer.Dispose();
        }
    }
}
