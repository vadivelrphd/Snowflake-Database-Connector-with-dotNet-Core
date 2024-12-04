using System;
using System.Data;
using System.Data.Common;
using Snowflake.Data.Client;

namespace SnowflakeConnect.Classes
{
    public class SnowFlakeDBConnect
    {
        // Placeholder variables for database connection details
        private string pDBAccount = "YourAccount";
        private string pDBHost = "YourHost";
        private string pDBUserName = "YourUserName";
        private string pDBPassword = "YourPassword";
        private string pDBRole = "YourRole";
        private string pDBName = "YourDatabase";
        private string pDBSchemaName = "YourSchema";
        private string pTableName = "YourTable";

        private SnowflakeDbConnection _snowflakeDbConnection = new SnowflakeDbConnection();

        public void OpenSnowflakeDbConnection()
        {
            try
            {
                _snowflakeDbConnection.ConnectionString = $"account={pDBAccount};host={pDBHost};user={pDBUserName};password={pDBPassword};role={pDBRole}";
                _snowflakeDbConnection.Open();
                Console.WriteLine("Connection opened successfully.");
            }
            catch (DbException exc)
            {
                Console.WriteLine("Error Message: {0}", exc.Message);
            }
        }

        public void CloseSnowflakeDbConnection()
        {
            if (_snowflakeDbConnection != null && _snowflakeDbConnection.State != ConnectionState.Closed)
            {
                _snowflakeDbConnection.Close();
                _snowflakeDbConnection.Dispose();
                _snowflakeDbConnection = null;
                Console.WriteLine("Connection closed successfully.");
            }
        }

        public bool GetDataFromSnowFlakeDB()
        {
            bool flag = false;
            try
            {
                if (!string.IsNullOrEmpty(pDBName) && !string.IsNullOrEmpty(pDBSchemaName) && !string.IsNullOrEmpty(pTableName))
                {
                    using (IDbCommand cmd = _snowflakeDbConnection.CreateCommand())
                    {
                        // Use the database
                        cmd.CommandText = $"USE DATABASE {pDBName};";
                        cmd.ExecuteNonQuery();

                        // Fetch data from the table
                        cmd.CommandText = $"SELECT \"Incident Number\", \"Incident Date\", \"Event Created On\", \"Area\", \"Short Description\", \"Long Description\" FROM {pDBName}.{pDBSchemaName}.{pTableName};";
                        using (IDataReader dataReader = cmd.ExecuteReader())
                        {
                            while (dataReader.Read())
                            {
                                // Declare variables to hold data
                                int incidentNumber = dataReader.GetInt32(0);
                                DateTime iDate = dataReader.GetDateTime(1);
                                DateTime eventCreatedOn = dataReader.GetDateTime(2);
                                string area = dataReader.GetString(3);
                                string shortDescription = dataReader.GetString(4);
                                string longDescription = dataReader.GetString(5);
                                DateTime lastUpdated = dataReader.GetDateTime(6);

                                // Process data as needed
                                Console.WriteLine($"Incident: {incidentNumber}, Date: {iDate}, Area: {area}");
                            }
                        }
                    }
                    flag = true;
                }
                else
                {
                    Console.WriteLine("Database details are missing.");
                }
            }
            catch (DbException exc)
            {
                Console.WriteLine($"Error: {exc.Message}");
                flag = false;
            }

            return flag;
        }
    }
}
