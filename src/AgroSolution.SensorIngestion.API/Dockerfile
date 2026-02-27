# build
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

COPY . .

# publica o projeto certo (não a solução)
RUN dotnet restore ./AgroSolution.SensorIngestion.API.csproj
RUN dotnet publish ./AgroSolution.SensorIngestion.API.csproj -c Release -o /app/publish --no-restore

# runtime
FROM mcr.microsoft.com/dotnet/aspnet:8.0
WORKDIR /app
COPY --from=build /app/publish .

EXPOSE 8080
ENV ASPNETCORE_URLS=http://+:8080

ENTRYPOINT ["dotnet", "AgroSolution.SensorIngestion.API.dll"]