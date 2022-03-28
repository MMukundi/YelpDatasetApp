using NUnit.Framework;
using Queries;
using System;
using System.Collections.Generic;
using System.Linq;
namespace TestQueries;

public class Tests
{
    Query query;

    [SetUp]
    public void Setup()
    {
        query = new Query("postgres", "Python3.7", "yelpdb");
        query.Open();
    }

    [Test]
    public void TestStates()
    {
        var states = new HashSet<string>(query.GetAllStates());
        Console.WriteLine(String.Join(", ",states));
        Assert.AreNotEqual(0, states.Count);
    }
    [Test]
    public void TestBusinesses()
    {
        var businessData = from x in query.GetBusinessesInZip(15203, new List<string>())
                           select (
                               (string)x["business_id"],
                               (string)x["business_name"],
                               (string)x["business_address"]
                           );
        var businesses = new HashSet<(string,string,string)>(businessData);
        Console.WriteLine(String.Join(", ", businesses));
        Assert.AreEqual(112, businesses.Count);
    }
}
