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
            var cts = new CancellationTokenSource();
            new Thread(delegate () { messageHandler.Subscribe("numbers-topic", cts.Token); }).Start();
            while (true)
            {
                if(Console.ReadKey().Key == ConsoleKey.Escape)
                {
                    break;
                }
            }
            cts.Cancel();
        }
    }
}
