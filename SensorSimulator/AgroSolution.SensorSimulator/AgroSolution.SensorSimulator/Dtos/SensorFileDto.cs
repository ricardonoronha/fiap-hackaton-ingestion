public record SensorFileDto(
    string FieldId,
    string Culture,
    string FarmerId,
    string SensorType,
    string SensorUnit,
    int StartRange, 
    int EndRange
);