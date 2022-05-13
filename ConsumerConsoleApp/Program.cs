using System;
using System.Threading;
using KafkaWrapper.Lib;

namespace ConsumerConsoleApp
{
    static class Program
    {
        static void Main(string[] args)
        {
            var messageHandler = new ReservoirMessageHandler(5);
            messageHandler.Subscribe("numbers-topic");
            messageHandler.Dispose();
        }
    }
}
