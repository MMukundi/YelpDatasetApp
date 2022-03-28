using System;
using System.Collections.Generic;
using Npgsql;
using NpgsqlTypes;

namespace Queries
{
    class PreparedStatement
    {
        NpgsqlCommand command;

        public PreparedStatement(NpgsqlConnection connection, string sql, Dictionary<string, NpgsqlDbType> parameters)
        {
            command = new NpgsqlCommand(sql, connection);
            foreach (var parameter in parameters)
            {
                command.Parameters.Add(parameter.Key, parameter.Value);
            }
            command.Prepare();
        }
        public PreparedStatement(NpgsqlConnection connection, string sql): this(connection, sql, new Dictionary<string, NpgsqlDbType>())  { }


        public void SetParameters(Dictionary<string, object> parameters)
        {
            foreach (var parameter in parameters)
            {
                command.Parameters[parameter.Key].Value = parameter.Value;
            }
        }

        public NpgsqlDataReader Query()
        {
            return Query(new Dictionary<string, object>());
        }
        public NpgsqlDataReader Query(Dictionary<string, object> parameters)
        {
            SetParameters(parameters);
            return command.ExecuteReader();
        }

        public int Run()
        {
            return Run(new Dictionary<string, object>());
        }
        public int Run(Dictionary<string, object> parameters)
        {
            SetParameters(parameters);
            return command.ExecuteNonQuery();
        }


    }
    public class Query
    {
        

        NpgsqlConnection Connection;

        PreparedStatement AllStatesCommand;
        PreparedStatement CitiesInStateCommand;
        PreparedStatement ZipsInCityCommand;
        PreparedStatement BusinessesInZipCommand;
        PreparedStatement BusinessesInZipWithCategoriesCommand;
        PreparedStatement InsertTipCommand;

        public Query(string user, string password, string database)
        {
            var connectionString = $"Server=127.0.0.1;Username={user};Password={password};Database={database}";
            Connection = new NpgsqlConnection(connectionString);

        }

        public void Open()
        {
            Connection.Open();

            AllStatesCommand = new PreparedStatement(Connection, "SELECT DISTINCT business_state FROM business;");
            CitiesInStateCommand = new PreparedStatement(
                Connection,
                "SELECT DISTINCT city FROM business WHERE business_state = @state;",
                new Dictionary<string, NpgsqlDbType>()
                {
                    {"state",NpgsqlDbType.Varchar }
                }
             );
            ZipsInCityCommand = new PreparedStatement(
               Connection,
               "SELECT DISTINCT zip FROM business WHERE business_state = @state AND city = @city;",
               new Dictionary<string, NpgsqlDbType>()
               {
                    {"state",NpgsqlDbType.Varchar },
                    {"city",NpgsqlDbType.Varchar }
               }
            );
            BusinessesInZipCommand = new PreparedStatement(
               Connection,
               "SELECT * FROM (SELECT DISTINCT BusinessesInZip.business_id FROM " +
               "(SELECT business_id FROM business WHERE zip = @zip ) AS BusinessesInZip " +
               "JOIN Category ON Category.business_id = BusinessesInZip.business_id) AS Matches JOIN business ON business.business_id = Matches.business_id;",
               new Dictionary<string, NpgsqlDbType>()
               {
                    {"zip", NpgsqlDbType.Integer },
               }
            );
            BusinessesInZipWithCategoriesCommand = new PreparedStatement(
               Connection,
               "SELECT * FROM (SELECT DISTINCT BusinessesInZip.business_id FROM " +
               "(SELECT business_id FROM business WHERE zip = @zip ) AS BusinessesInZip " +
               "JOIN Category ON Category.business_id = BusinessesInZip.business_id WHERE Category.category = ANY( :categories)) AS Matches JOIN business ON business.business_id = Matches.business_id;",
               new Dictionary<string, NpgsqlDbType>()
               {
                    {"zip", NpgsqlDbType.Integer },
                    {"categories", NpgsqlDbType.Varchar | NpgsqlDbType.Array }
               }
            );

            InsertTipCommand = new PreparedStatement(
                Connection,
                "INSERT INTO tip " +
                "(user_id, business_id, date_posted, body) VALUES " +
                "(@user, @business, @date, @body)",
                new Dictionary<string, NpgsqlDbType>()
               {
                    {"user", NpgsqlDbType.Varchar },
                    {"business", NpgsqlDbType.Varchar },
                    {"date", NpgsqlDbType.Timestamp },
                    {"body", NpgsqlDbType.Varchar }
               }
            );
        }



        public IEnumerable<string> GetAllStates()
        {
            NpgsqlDataReader states = AllStatesCommand.Query();
            while (states.Read())
            {
                yield return (string)states["business_state"];
            }
        }

        public IEnumerable<string> GetCitiesInState(string state)
        {
            NpgsqlDataReader cities = CitiesInStateCommand.Query(new Dictionary<string, object>()
            {
                { "state", state }
            });
            while (cities.Read())
            {
                yield return (string)cities["city"];
            }
        }
        public IEnumerable<string> GetZipsInCity(string state, string city)
        {
            NpgsqlDataReader zips = ZipsInCityCommand.Query(new Dictionary<string, object>()
            {
                { "state", state },
                { "city", city }
            });
            while (zips.Read())
            {
                yield return (string)zips["zip"];
            }
        }

        // NOTE: Do not store the results directly! All objects in this
        // IEnumerable are the same NpgsqlDataReader. To see how to use
        // this method, see TestQueries.TestQueries.TestBusinesses
        public IEnumerable<NpgsqlDataReader> GetBusinessesInZip(int zip, List<string> categories)
        {
            NpgsqlDataReader businesses = categories.Count == 0 ?
            BusinessesInZipCommand.Query(new Dictionary<string, object>()
            {
                { "zip", zip },
            }) :
            BusinessesInZipWithCategoriesCommand.Query(new Dictionary<string, object>()
            {
                { "zip", zip },
                {"categories",categories }
            });

            while (businesses.Read())
            {
                yield return businesses;
            }

        }

        public int InsertTip(string user, string business, string body, string date)
        {
            return InsertTipCommand.Run(new Dictionary<string, object>() {
                { "user", user},
                { "business", business},
                { "body", body},
                { "date", date}
            });
        }
    }
}

