# Build context: the module root (01-microservices-sync-pain/).
FROM mcr.microsoft.com/dotnet/sdk:10.0.403 AS build
WORKDIR /src

# Restore first, in its own layer. It is only redone when the .csproj changes.
COPY src/PaymentService/PaymentService.csproj PaymentService/
RUN dotnet restore PaymentService/PaymentService.csproj

COPY src/PaymentService/ PaymentService/
RUN dotnet publish PaymentService/PaymentService.csproj -c Release -o /app/publish --no-restore /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:10.0.14 AS final
WORKDIR /app
COPY --from=build /app/publish .

# The base image ships a non-root "app" user; APP_UID is defined by the image.
USER $APP_UID
# ASP.NET Core images listen on 8080 by default (ASPNETCORE_HTTP_PORTS=8080).
EXPOSE 8080
ENTRYPOINT ["dotnet", "PaymentService.dll"]