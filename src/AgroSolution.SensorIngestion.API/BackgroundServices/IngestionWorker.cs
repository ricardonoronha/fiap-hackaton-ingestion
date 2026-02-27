using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using InfluxDB.Client;
using InfluxDB.Client.Api.Domain;
using InfluxDB.Client.Writes;

public sealed class IngestionWorker : BackgroundService
{
    private readonly ILogger<IngestionWorker> _logger;
    private readonly ConnectionFactory _factory;

    private IConnection? _conn;
    private IChannel? _ch;

    private readonly string QueueName = Environment.GetEnvironmentVariable("RABBITMQ_QUEUE") ?? "sensor_readings";
    private readonly IInfluxDBClient _influx;
    private readonly string _bucket = Environment.GetEnvironmentVariable("INFLUX_BUCKET") ?? "default";
    private readonly string _org = Environment.GetEnvironmentVariable("INFLUX_ORG") ?? "influxdata";
    private readonly string _measurement = Environment.GetEnvironmentVariable("INFLUX_MEASUREMENT") ?? "sensor_reading";

    public IngestionWorker(
    ILogger<IngestionWorker> logger,
    ConnectionFactory factory,
    IInfluxDBClient influx)
    {
        _logger = logger;
        _factory = factory;
        _influx = influx;
    }

    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        // 1 conexão pra vida do worker
        _conn = await _factory.CreateConnectionAsync(cancellationToken);
        _ch = await _conn.CreateChannelAsync(cancellationToken: cancellationToken);

        await _ch.QueueDeclareAsync(
            queue: QueueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: null,
            cancellationToken: cancellationToken
        );

        // Evita puxar 1000 mensagens de uma vez: processa N por vez.
        await _ch.BasicQosAsync(prefetchSize: 0, prefetchCount: 10, global: false, cancellationToken: cancellationToken);

        await base.StartAsync(cancellationToken);

        _logger.LogInformation("Rabbit consumer iniciado na fila {Queue}", QueueName);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (_ch is null) throw new InvalidOperationException("Channel não inicializado.");

        var consumer = new AsyncEventingBasicConsumer(_ch);

        consumer.ReceivedAsync += async (sender, ea) =>
        {
            try
            {
                // corpo
                var json = Encoding.UTF8.GetString(ea.Body.ToArray());

                // desserializa conforme seu DTO
                var reading = JsonSerializer.Deserialize<SensorReading>(json);
                if (reading is null)
                    throw new InvalidOperationException("Payload inválido (JSON não desserializou).");

                // TODO: sua lógica real aqui
                await ProcessReadingAsync(reading, stoppingToken);

                // sucesso -> ACK
                await _ch.BasicAckAsync(ea.DeliveryTag, multiple: false, cancellationToken: stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                // shutdown: não tenta mexer muito
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Erro ao processar mensagem. DeliveryTag={Tag}", ea.DeliveryTag);

                // falhou -> decide requeue
                // requeue:true = volta pra fila (cuidado loop infinito)
                // requeue:false = descarta / vai pra DLQ (se configurada)
                await _ch.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: true, cancellationToken: stoppingToken);
            }
        };

        // mantém o consumidor vivo até o cancelamento
        await _ch.BasicConsumeAsync(
            queue: QueueName,
            autoAck: false,
            consumer: consumer,
            cancellationToken: stoppingToken
        );

        // Não finaliza o método, senão o worker morre
        await Task.Delay(Timeout.Infinite, stoppingToken);
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Parando Rabbit consumer...");

        try
        {
            if (_ch is not null) await _ch.CloseAsync(cancellationToken);
            if (_conn is not null) await _conn.CloseAsync(cancellationToken);
        }
        finally
        {
            _ch?.Dispose();
            _conn?.Dispose();
        }

        await base.StopAsync(cancellationToken);
    }

    private async Task ProcessReadingAsync(SensorReading reading, CancellationToken ct)
    {
        // WriteApiAsync usa batching interno e é thread-safe
        var writeApi = _influx.GetWriteApiAsync();

        // IMPORTANTE:
        // Tags = indexadas (cuidado com cardinalidade alta)
        // Fields = valores medidos

        var point = PointData
            .Measurement(_measurement)
            .Tag("fieldId", reading.FieldId)
            .Tag("sensorType", reading.SensorType)
            .Field("value", reading.Value)
            .Timestamp(reading.Timestamp.GetValueOrDefault(), WritePrecision.Ns);

        await writeApi.WritePointAsync(point, _bucket, _org, ct);
    }
}
