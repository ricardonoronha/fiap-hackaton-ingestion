public record SensorDataDto(
    string FieldId,
    string Culture,
    string FarmerId,
    string SensorType,
    string SensorUnit,
    double Value,
    DateTime? Timestamp
);
