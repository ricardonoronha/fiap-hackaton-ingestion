using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace AgroSolution.SensorSimulator.Services
{
    public class SendReadingsService
    {
        private readonly string requestUrl;
        private readonly ReadFileService readFileService;
        public SendReadingsService(string readingsApiUri)
        {
            requestUrl = readingsApiUri;
            readFileService = new ReadFileService();
        }

        private async Task SendReading(SensorFileDto reading, CancellationToken ct)
        {
            var httpClient = new HttpClient();

            while (!ct.IsCancellationRequested) {

                var data = new SensorDataDto(
                   FieldId: reading.FieldId,
                   Culture: reading.Culture, 
                   FarmerId: reading.FarmerId,
                   SensorType: reading.SensorType,
                   SensorUnit: reading.SensorUnit,
                   Value: new Random().Next(reading.StartRange, reading.EndRange + 1),
                   Timestamp: DateTime.Now
                   );

                string json = JsonSerializer.Serialize(data);
                var content = new StringContent(json, Encoding.UTF8, "application/json");
                Console.WriteLine("Enviando dados de: " + json);
                var response = await httpClient.PostAsync(requestUrl, content);
                Console.WriteLine("Resposta de envio: " + response.StatusCode);

                await Task.Delay(3000, ct);
            }
        }

        public Task SendFiles(string[] files, CancellationToken ct)
        {
            List<SensorFileDto> sensorTemplates = new List<SensorFileDto>();

            for (int i = 0; i < files.Length; i++)
            {
                var sensorData = readFileService.ReadSensorFile(files[i]);
                if (sensorData is null || sensorData.Count == 0)
                {
                    Console.WriteLine($"Nenhum dado encontrado no arquivo {files[i]}. Pulando envio...");
                    continue;
                }

                sensorTemplates.AddRange(sensorData);
                Console.WriteLine($"Dados do arquivo {files[i]} lidos com sucesso. Enviando dados...");
            }
            
            var sendTasks = sensorTemplates.Select( sensorTemplate => Task.Run(() => SendReading(sensorTemplate, ct), ct)).ToArray();
            return Task.WhenAll(sendTasks);
        }
    }
}
