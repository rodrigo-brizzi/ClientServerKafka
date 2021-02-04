using System;
using System.Collections.Generic;
using System.Text;

namespace server.Models
{
    public class Mensagem
    {
        public string id { get; set; }
        public string conteudo { get; set; }
        public string status { get; set; }
        public DateTime dataEnvio { get; set; }
        public DateTime dataRetorno { get; set; }
        public int particao { get; set; }
        public string grupo { get; set; }
    }
}
