using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace KafkaWrapper.Lib
{
    public class ProducerWrapper : IDisposable
    {
        private readonly IProducer<string, int> _producer;

        public ProducerWrapper() : this("localhost") 
        { 
        }
        public ProducerWrapper(string hostname)
        {
            var producerConfig = new Dictionary<string, string> { { "bootstrap.servers", hostname } };
            _producer = new ProducerBuilder<string, int>(producerConfig)
                            .SetKeySerializer(Serializers.Utf8)
                            .SetValueSerializer(Serializers.Int32).Build();
        }

        public void SendRandomMessage(string topic, string key, int range)
        {
            Random rand = new Random();
            int value = rand.Next(range);
            _producer.Produce(topic, new Message<String, int> {Key = key, Value =  value}, (deliveryReport) =>
            {
                if (deliveryReport.Error.Code != ErrorCode.NoError)
                {
                    Console.WriteLine($"Failed to deliver message: {deliveryReport.Error.Reason}");
                }
                else
                {
                    Console.WriteLine($"Produced message to: {deliveryReport.TopicPartitionOffset} ({key}:{value})");
                }
            });
        }

        public void Dispose()
        {
            _producer.Dispose();
        }
    }
}
