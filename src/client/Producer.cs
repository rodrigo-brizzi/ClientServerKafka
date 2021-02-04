using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace client
{
    public class Producer
    {
        public string[] bootstrapServers { get; set; }
        public string nomeTopico { get; set; }

        public Producer(string[] bootstrapServers, string nomeTopico)
        {
            this.bootstrapServers = bootstrapServers;
            this.nomeTopico = nomeTopico;
        }

        public async Task Enviar(string mensagem)
        {
            try
            {
                var config = new ProducerConfig
                {
                    BootstrapServers = string.Join(',', bootstrapServers)
                };

                using (var producer = new ProducerBuilder<Null, string>(config).Build())
                {
                    var result = await producer.ProduceAsync(nomeTopico, new Message<Null, string> { Value = mensagem });
                    Console.WriteLine($"Mensagem enviada: {mensagem} particao {result.TopicPartitionOffset}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
        }
    }
}
