using Convey.CQRS.Queries;
using Microsoft.Extensions.DependencyInjection;
using MongoDB.Driver;
using Shouldly;

namespace Convey.Persistence.MongoDB.Tests;

[Collection(nameof(TestCollectionFixture))]
public sealed class TestSuite
{
    private readonly TestFixture _fixture;

    public TestSuite(TestFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact(DisplayName = "Should have cancellation token")]
    public void Test_01()
    {
        _fixture.CancellationToken.ShouldNotBe(CancellationToken.None);
    }

    [Fact(DisplayName = "Should connect to mongodb")]
    public async Task Test_02()
    {
        var client = _fixture.App.Services.GetRequiredService<IMongoClient>();

        using var cursor = await client.ListDatabaseNamesAsync(_fixture.CancellationToken);

        var result = await cursor.AnyAsync(_fixture.CancellationToken);

        result.ShouldBeTrue();
    }

    [Fact(DisplayName = "Should sort with default collation")]
    public async Task Test_03()
    {
        var results = await _fixture.UserRepository.BrowseAsync(_ => true, new GetUsersQuery(1, 1000, "name", "desc"));

        results.Items.ElementAt(0).Name.ShouldMatch("Cuser");
        results.Items.ElementAt(1).Name.ShouldMatch("cuser");
        results.Items.ElementAt(2).Name.ShouldMatch("Buser");
        results.Items.ElementAt(3).Name.ShouldMatch("buser");
        results.Items.ElementAt(4).Name.ShouldMatch("Auser");
        results.Items.ElementAt(5).Name.ShouldMatch("auser");
    }

    [Fact(DisplayName = "Should sort with custom collation")]
    public async Task Test_04()
    {
        var results = await _fixture.UserRepository.BrowseAsync(_ => true, new GetUsersQuery(1, 1000, "name", "desc"), new AggregateOptions
        {
            Collation = null
        });

        results.Items.ElementAt(0).Name.ShouldMatch("cuser");
        results.Items.ElementAt(1).Name.ShouldMatch("buser");
        results.Items.ElementAt(2).Name.ShouldMatch("auser");
        results.Items.ElementAt(3).Name.ShouldMatch("Cuser");
        results.Items.ElementAt(4).Name.ShouldMatch("Buser");
        results.Items.ElementAt(5).Name.ShouldMatch("Auser");
    }

    private sealed record GetUsersQuery(
        int Page,
        int Results,
        string OrderBy,
        string SortOrder) : IPagedQuery;
}