using client.Models;
using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace client
{
    class Program
    {
        static async Task Main(string[] args)
        {
            if (args.Length != 1)
            {
                Console.WriteLine("Informe o conteúdo da mensagem!");
                return;
            }

            string mensagem = args[0];

            //Cliente produz mensagem para o topico req no grupo SP
            Mensagem objMensagem = new Mensagem();
            objMensagem.id = Guid.NewGuid().ToString();
            objMensagem.conteudo = $"Mensagem: {mensagem}";
            objMensagem.status = "Pendente";
            objMensagem.dataEnvio = DateTime.Now;
            objMensagem.grupo = "SP";
            Producer prod = new Producer(new string[] { "localhost:9092" }, "req");
            await prod.Enviar($"{JsonSerializer.Serialize(objMensagem)}");

            //Cliente consome mensagem do topico resp no grupo SP
            Consumer cons = new Consumer(new string[] { "localhost:9092" }, "resp", "SP");
            cons.Receber(objMensagem);
        }
    }
}
