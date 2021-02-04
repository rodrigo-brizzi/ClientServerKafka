using Confluent.Kafka;
using server.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;

namespace server
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

        public void Receber()
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = string.Join(',', bootstrapServers),
                GroupId = grupo,
                AutoOffsetReset = AutoOffsetReset.Earliest
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
                            consumer.Commit();
                            Console.WriteLine($"Mensagem recebida: {cr.Message.Value} particao: {cr.TopicPartitionOffset}");

                            Mensagem msgRec = JsonSerializer.Deserialize<Mensagem>(cr.Message.Value);
                            if (Program.arrMensagem.Keys.FirstOrDefault(m => m == msgRec.id) == null)
                            {
                                //Aqui é o processamento da mensagem, pode ser enviado para outras camadas e depois entregue para a lista que fará o retorno
                                msgRec.status = "OK";
                                msgRec.dataRetorno = DateTime.Now;
                                Program.arrMensagem.Add(msgRec.id, msgRec);
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
