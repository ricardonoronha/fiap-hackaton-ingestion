using AgroSolution.SensorSimulator.Services;

Console.WriteLine("Iniciando o simulador de sensores...");
Console.WriteLine($"Digite 1 para enviar dados de todos os arquivos. Caso queira enviar de um arquivo específico, digite o nome do arquivo.{Environment.NewLine}(ex: sensor_data.txt).");
Console.Write("Digite sua resposta: ");
string? option = Console.ReadLine();

string[] files = Array.Empty<string>();

if (option is null || option.Trim() == "1")
{
    Console.WriteLine("Enviando dados de todos os arquivos...");
    string filesPath = Environment.GetEnvironmentVariable("SENSOR_FILES_PATH") ?? Path.Combine(AppContext.BaseDirectory, "sensors");
    files = Directory.GetFiles(filesPath);
}
else
{
    Console.WriteLine($"Enviando dados do arquivo {option}...");
    files.Append(option);
}

string requestUrl = "http://localhost:8080/api/readings";

var ctSource = new CancellationTokenSource();

var sendService = new SendReadingsService(requestUrl);

var sendTask = sendService.SendFiles(files, ctSource.Token);

Console.WriteLine("Pressionar qualquer tecla para parar o envio de dados...");
Console.ReadKey();

ctSource.Cancel();

await sendTask;








