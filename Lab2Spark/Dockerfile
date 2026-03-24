FROM mcr.microsoft.com/dotnet/runtime:10.0 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["Lab2Spark.csproj", "./"]
RUN dotnet restore "Lab2Spark.csproj"
COPY . .
RUN dotnet publish "./Lab2Spark.csproj" -c $BUILD_CONFIGURATION -o /app/publish
