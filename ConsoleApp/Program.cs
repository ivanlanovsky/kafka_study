using System;
using System.Threading;
using KafkaWrapper.Lib;

namespace ProducerConsoleApp
{
    static class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Press Esc to do safely cancel...");
            var cts = new CancellationTokenSource();
            string topicName = "numbers-topic";
            new Thread(delegate ()
            {
                RunSimpleProducer(topicName, cts.Token);
            }).Start();
            while (true)
            {
                if(Console.ReadKey().Key == ConsoleKey.Escape)
                {
                    break;
                }
            }
            cts.Cancel();
            cts.Dispose();
        }

        private static void RunSimpleProducer(string topicName, CancellationToken cancelToken)
        {
            new Thread(delegate () {
                StartProducing(topicName, "Ping", 999, cancelToken);
            }).Start();
            StartProducing(topicName, "FPS", 99, cancelToken);
        }

        //private static void RunManyProducers(CancellationToken cancelToken)
        //{
        //    for(int i = 0; i < 100; i++)
        //    {
                
        //    }
        //}

        private static void StartProducing(string topicName, string key, int range, CancellationToken cancelToken)
        {
            ProducerWrapper producer = new ProducerWrapper();
            while (!cancelToken.IsCancellationRequested)
            {
                producer.SendRandomMessage(topicName, key, range);
                Thread.Sleep(500);
            }
            Thread.Sleep(1000);
            producer.Dispose();
        }
    }
}
