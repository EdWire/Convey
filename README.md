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

#Step1:
-Build project(s) with release
-Go to "bin" folder and copy the nuget to the "project folder"

#Step2 Auth:
- Go to  https://dev.azure.com/edwire/EW.Educate/_artifacts/feed/edgraph/connect
- download nuget.exe and place it side by side with "bin" and project file in the "project folder" (IMPORTNANT!!!)


#Step3: Push Nuget package (login with ewconsoltent.net required, change nuget file name)
.\nuget.exe push Convey.MessageBrokers.Outbox.1.1.450.nupkg -src https://edwire.pkgs.visualstudio.com/EW.Educate/_packaging/edgraph/nuget/v3/index.json -ApiKey "Azure DevOps Artifacts - EdGraph Feed (Read-Only)"

.\nuget.exe push Convey.MessageBrokers.Outbox.Mongo.1.1.450.nupkg -src https://edwire.pkgs.visualstudio.com/EW.Educate/_packaging/edgraph/nuget/v3/index.json -ApiKey "Azure DevOps Artifacts - EdGraph Feed (Read-Only)"

.\nuget.exe push Convey.MessageBrokers.ConfluentKafka.1.1.450.nupkg -src https://edwire.pkgs.visualstudio.com/EW.Educate/_packaging/edgraph/nuget/v3/index.json -ApiKey "Azure DevOps Artifacts - EdGraph Feed (Read-Only)"