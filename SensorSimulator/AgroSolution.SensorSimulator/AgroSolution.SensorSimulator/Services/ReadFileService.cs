using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AgroSolution.SensorSimulator.Services
{
    public class ReadFileService
    {
        public List<SensorFileDto> ReadSensorFile(string filePath)
        {
            var result = new List<SensorFileDto>();

            if (!File.Exists(filePath))
                throw new FileNotFoundException("Arquivo não encontrado", filePath);

            foreach (var line in File.ReadLines(filePath))
            {
                if (string.IsNullOrWhiteSpace(line))
                    continue;

                result.Add(ParseSensorFileLine(line));
            }

            return result;
        }

        public SensorFileDto ParseSensorFileLine(string line)
        {
            var parts = line.Split('|');

            if (parts.Length != 7)
                throw new FormatException($"Linha com formato inválido: {line}");

            return new SensorFileDto(
                FieldId: parts[0].Trim(),
                Culture: parts[1].Trim(),
                FarmerId: parts[2].Trim(),
                SensorType: parts[3].Trim(),
                SensorUnit: parts[4].Trim(),
                StartRange: int.Parse(parts[5].Trim()),
                EndRange: int.Parse(parts[6].Trim())
            );
        }
    }
}
