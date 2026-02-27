using InfluxDB.Client;
using RabbitMQ.Client;
using System.Text;
using System.Text.Json;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddHealthChecks();

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

builder.Services.AddSingleton<IInfluxDBClient>(_ =>
{
    var url = Environment.GetEnvironmentVariable("INFLUX_URL") ?? string.Empty;
    var token = Environment.GetEnvironmentVariable("INFLUX_TOKEN") ?? string.Empty;
    return new InfluxDBClient(url, token);
});

var queueName = Environment.GetEnvironmentVariable("RABBITMQ_QUEUE") ?? "sensor_readings";

builder.Services.AddSingleton(_ =>
{
    var rabbitHost = Environment.GetEnvironmentVariable("RABBITMQ_HOST") ?? string.Empty;
    var rabbitUser = Environment.GetEnvironmentVariable("RABBITMQ_USER") ?? string.Empty;
    var rabbitPass = Environment.GetEnvironmentVariable("RABBITMQ_PASS") ?? string.Empty;

    return new ConnectionFactory
    {
        HostName = rabbitHost,
        UserName = rabbitUser,
        Password = rabbitPass,
    };
});

builder.Services.AddHostedService<IngestionWorker>();

var app = builder.Build();

app.MapHealthChecks("/healthz");

app.UseSwagger();
app.UseSwaggerUI();


app.MapPost("/readings", async (SensorReading reading, ConnectionFactory factory) =>
{
    // validação mínima
    if (string.IsNullOrWhiteSpace(reading.FieldId)) return Results.BadRequest("fieldId required");
    if (string.IsNullOrWhiteSpace(reading.SensorType)) return Results.BadRequest("sensorType required");

    var payload = JsonSerializer.Serialize(reading);
    var body = Encoding.UTF8.GetBytes(payload);

    using var conn = await factory.CreateConnectionAsync();
    using var ch = await conn.CreateChannelAsync();

    await ch.QueueDeclareAsync(queue: queueName, durable: true, exclusive: false, autoDelete: false, arguments: null);

    var props = new BasicProperties
    {
        Persistent = true
    };

    await ch.BasicPublishAsync(
    exchange: "",
    routingKey: queueName,
    mandatory: false,
    basicProperties: props,
    body: body
);

    return Results.Accepted();
})
.WithName("IngestReading");

app.Run();

record SensorReading(
    string FieldId,
    string SensorType,
    int Value,
    DateTime? Timestamp
);