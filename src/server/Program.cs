using server.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace server
{
    class Program
    {
        public static Dictionary<string, Mensagem> arrMensagem;
        static async Task Main(string[] args)
        {
            arrMensagem = new Dictionary<string, Mensagem>();
            Console.WriteLine($"Servidor Iniciado!");

            //Servidor consome mensagem no grupo SP
            //Para entendimento de como os grupos funcionam dentro de um topico, sugiro a leitura:
            //https://docs.cloudera.com/runtime/7.1.1/kafka-developing-applications/topics/kafka-develop-groups-fetching.html

            Consumer cons = new Consumer(new string[] { "localhost:9092" }, "req", "SP");
            Thread thrReceber = new Thread(cons.Receber);
            thrReceber.IsBackground = true;
            thrReceber.Start();
            Console.WriteLine($"Escutando requisições do grupo SP!");

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
            };

            try
            {
                while (true && !cts.IsCancellationRequested)
                {
                    if (arrMensagem.Count > 0)
                    {
                        string key = arrMensagem.Keys.First();
                        Mensagem msg = arrMensagem[key];
                        Producer prod = new Producer(new string[] { "localhost:9092" }, "resp");
                        await prod.Enviar($"{JsonSerializer.Serialize(msg)}");
                        arrMensagem.Remove(key);
                    }
                }
                Console.WriteLine("Cancelada a execução do Producer...");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Erro: {ex.ToString()}");
            }
            
        }
    }
}
