using client.Models;
using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Linq;

namespace client
{
    public class Consumer
    {
        public string[] bootstrapServers { get; set; }
        public string nomeTopico { get; set; }
        public string grupo { get; set; }

        public Consumer(string[] bootstrapServers, string nomeTopico, string grupo)
        {
            this.bootstrapServers = bootstrapServers;
            this.nomeTopico = nomeTopico;
            this.grupo = grupo;
        }

        public void Receber(Mensagem objMensagem)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = string.Join(',', bootstrapServers),
                GroupId = grupo,
                AutoOffsetReset = AutoOffsetReset.Latest,
                EnableAutoCommit = false
            };

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
            };

            try
            {
                using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
                {
                    consumer.Subscribe(nomeTopico);
                    
                    try
                    {
                        while (true)
                        {
                            var cr = consumer.Consume(cts.Token);
                            Console.WriteLine($"Mensagem consumida: {cr.Message.Value} particao: {cr.TopicPartitionOffset}");

                            Mensagem msgRec = JsonSerializer.Deserialize<Mensagem>(cr.Message.Value);
                            if (objMensagem.id == msgRec.id)
                            {
                                consumer.Commit();
                                Console.WriteLine($"Mensagem recebida: {cr.Message.Value} particao: {cr.TopicPartitionOffset}");
                                consumer.Close();
                                break;
                            }
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        consumer.Close();
                        Console.WriteLine("Cancelada a execução do Consumer...");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Exceção: {ex.GetType().FullName} | " +
                             $"Mensagem: {ex.Message}");
            }
        }
    }
}
