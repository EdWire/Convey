using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MongoDB.Driver;

namespace Convey.Persistence.MongoDB.Tests;

[CollectionDefinition(nameof(TestCollectionFixture))]
public sealed class TestCollectionFixture : ICollectionFixture<TestFixture>;

public sealed class TestFixture : IAsyncLifetime
{
    public CancellationToken CancellationToken { get; }
    public IHost App { get; }
    public IMongoRepository<UserDocument, Guid> UserRepository { get; }

    public TestFixture()
    {
        CancellationToken = new CancellationTokenSource(TimeSpan.FromMinutes(10)).Token;

        App = new HostBuilder()
            .ConfigureAppConfiguration(config => config
                .AddJsonFile("./appsettings.json"))
            .ConfigureLogging(logging => logging
                .SetMinimumLevel(LogLevel.Information)
                .AddConsole())
            .ConfigureServices((host, services) =>
            {
                services
                    .AddConvey()
                    .AddMongo()
                    .AddMongoRepository<UserDocument, Guid>("Users");
            })
            .Build();

        UserRepository = App.Services.GetRequiredService<IMongoRepository<UserDocument, Guid>>();
    }

    public async Task InitializeAsync()
    {
        var users = new List<UserDocument>
        {
            new UserDocument(Guid.NewGuid(), "cuser"),
            new UserDocument(Guid.NewGuid(), "Buser"),
            new UserDocument(Guid.NewGuid(), "auser"),
            new UserDocument(Guid.NewGuid(), "Auser"),
            new UserDocument(Guid.NewGuid(), "buser"),
            new UserDocument(Guid.NewGuid(), "Cuser"),
        };

        foreach (var user in users)
        {
            await UserRepository.AddAsync(user);
        }
    }

    public async Task DisposeAsync()
    {
        await UserRepository.Collection.DeleteManyAsync(Builders<UserDocument>.Filter.Empty);
    }
}
