# Convey - a simple recipe for .NET Core microservices 
## Read the docs [here](https://convey-stack.github.io) or [see it in action](https://www.youtube.com/watch?v=cxEXx4UT1FI).


Supported features that will help to quickly set up your next microservices:

- Authentication [JWT](http://jwt.io) with secret key & certificates extensions
- CQRS basic abstractions
- [Consul](https://www.consul.io) service registry integration
- [Swagger](https://swagger.io) extensions
- [RestEase](https://github.com/canton7/RestEase) extensions
- [Fabio](https://github.com/fabiolb/fabio) load balancer integration
- Logging extensions for [Serilog](https://serilog.net/) & integration with [Seq](https://datalust.co/seq), [ELK](https://www.elastic.co/what-is/elk-stack), [Loki](https://grafana.com/oss/loki/)
- Message brokers abstractions & CQRS support
- [RabbitMQ](https://www.rabbitmq.com) integration
- Inbox + Outbox implementation for EF Core, Mongo
- [AppMetrics](https://www.app-metrics.io) extensions
- [Prometheus](https://prometheus.io) integration
- [MongoDB](https://www.mongodb.com/cloud) extensions
- [OpenStack OCS](https://specs.openstack.org/openstack/ironic-specs/specs/4.0/msft-ocs-power-driver.html) support
- [Redis](https://redis.io) extensions
- [Vault](https://www.vaultproject.io) secrets engine (settings, dynamic credentials, PKI etc.) integration
- Security extensions (certificates, mTLS, encryption etc.)
- [Jaeger](https://www.jaegertracing.io) tracing integration
- Web API extensions (minimal routing-based API, CQRS support)

Created & maintained by [devmentors.io](http://devmentors.io).

## How to update a package?

### Terminology

- **Project folder**: The directory where the .csproj file of your library is located.
- **NuGet Package file:** A file with the extension .nupkg

### Steps

1. Update the version of the package accordingly, i.e. in the .csproj file of your project add the `Version` tag:

```xml
<PropertyGroup>    
    ...    
    <Version>1.0.0</Version>    
    ...
</PropertyGroup>
```

2. Generate the nuget package file by running this command in your project folder:

```sh
dotnet pack {PackageName}.csproj -c Release
```

e.g.

```sh
dotnet pack Convey.Persistence.MongoDB.csproj -c Release
```

3. Copy the generated nuget package file from {ProjectFolder}/bin/Release to the project folder, e.g:
```
// Copy from here
{ProjectFolder}/bin/Release/{PackageName}.nupkg

// To here
{ProjectFolder}/{PackageName}.nupkg
```

4. Get the nuget.exe tool.    
    1. Go to https://dev.azure.com/edwire/EW.Educate/_artifacts/feed/edgraph/connect.    
    2. Select the "NuGet.exe" option from the list.    
    3. Click on the "Get the tools" button and then on the "Download the latest NuGet" link.    
    4. A nuget.exe file will be downloaded into your Downloads folder.    
    5. Copy the nuget.exe into the project folder.

5. Publish the nuget package by running the following command in the project folder:

```sh
.\nuget.exe push {PackageName}.nupkg -src https://edwire.pkgs.visualstudio.com/EW.Educate/_packaging/edgraph/nuget/v3/index.json -ApiKey "Azure DevOps Artifacts - EdGraph Feed (Read-Only)"
```

6. Verify that the package was pushed with the correct version by going to https://dev.azure.com/edwire/EW.Educate/_artifacts/feed/edgraph and searching your package name.