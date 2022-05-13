using System;
using System.Threading;
using KafkaWrapper.Lib;

namespace ProducerConsoleApp
{
    static class Program
    {
        static void Main(string[] args)
        {
            string topicName = "numbers-topic";
            new Thread(delegate () {
                StartProducing(topicName, "Ping", 999);
            }).Start();
            StartProducing(topicName, "FPS", 99);
        }

        private static void StartProducing(string topicName, string key, int range)
        {
            ProducerWrapper producer = new ProducerWrapper();
            try
            {
                while (true)
                {
                    producer.SendRandomMessage(topicName, key, range);
                    Thread.Sleep(500);
                }
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("Exiting...");
            }
            producer.Dispose();
        }
    }
}
