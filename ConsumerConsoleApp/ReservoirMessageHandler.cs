using System;
using System.Collections.Generic;
using System.Text;

namespace ConsumerConsoleApp
{
    class ReservoirMessageHandler : MessageHandlerBase
    {
        private KeyValuePair<string, int>[] reservoir;
        private int counter;

        public ReservoirMessageHandler(int capacity) : base()
        {
            reservoir = new KeyValuePair<string, int>[capacity];
            counter = 0;
        }
        public void Subscribe(string topic)
        {
            base.Subscribe(topic, Handle);
        }

        private void Handle(KeyValuePair<string, int> message)
        {
            if(counter < reservoir.Length)
            {
                reservoir[counter] = message;
            }
            else
            {
                int randNumber = new Random().Next(counter);
                if(randNumber >= reservoir.Length)
                {
                    counter += 1;
                    return;
                }
                reservoir[randNumber] = message;
            }
            counter += 1;
            PrintReservoirData();
        }

        private void PrintReservoirData()
        {
            var strBuilder = new StringBuilder();
            int i = 0;
            while(i < reservoir.Length && !reservoir[i].Equals(default(KeyValuePair<string, int>)))
            {
                strBuilder.Append($"{reservoir[i].Key} : {reservoir[i].Value} | ");
                i++;
            }
            Console.Write($"\r{strBuilder}               ");
        }
    }
}
