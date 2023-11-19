using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.IO;

namespace DBRepair
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                Wrapper(args);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Fatal error: ");
                Console.WriteLine(ex.Message);
                Console.WriteLine("Press any key to exit");
                Console.ReadKey();
            }
        }

        static void Wrapper(string[] args)
        {
            //if (File.Exists("Sandbox_230915.db"))
            //{
            //    File.Delete("Sandbox_230915.db");
            //}
            //File.Copy("Sandbox_230915.db.b", "Sandbox_230915.db");


            string fileName = "Sandbox_230915.db";

            if (args.Length > 0)
            {
                fileName = args[0];
            }

            Console.WriteLine("Working on database file: " + fileName);
            if (!File.Exists(fileName))
            {
                Console.WriteLine("Fatal error: could not find database file " + fileName);
                Console.WriteLine("Press any key to exit");
                Console.ReadKey();
                return;
            }
               
            
            try
            {
                Console.WriteLine("Evaluating the database change counter");
                //FileStream fileStream = File.Open(fileName, FileMode.Open);
                BinaryReader binaryReader = new BinaryReader(File.Open(fileName, FileMode.Open));
                binaryReader.BaseStream.Position = 24;

                int dbChangeCounter = binaryReader.ReadInt32();
                if (dbChangeCounter < 0)
                {
                    Console.WriteLine("The database change counter is negative, resetting...");
                    binaryReader.BaseStream.Position = 24;
                    byte[] bytesToWrite = { 0x0, 0x0, 0x0, 0x0 };
                    binaryReader.BaseStream.Write(bytesToWrite, 0, bytesToWrite.Length);
                }
                binaryReader.Close();
            }
            catch (Exception ex)
            {
                Console.WriteLine("Fatal error: Binary evaluation failed.");
                Console.WriteLine(ex.Message);
                Console.WriteLine("Press any key to exit");
                Console.ReadKey();
                return;
            }
            

            string fileNameBackup = fileName + ".backup";
            if (File.Exists(fileNameBackup))
            {
                File.Delete(fileNameBackup);
            }

            File.Move(fileName, fileNameBackup);

            SQLiteConnection conn;
            try
            {
                conn = new SQLiteConnection("Data Source=" + fileName + ";New=True");
                conn.Open();
            }
            catch (SQLiteException ex)
            {
                Console.WriteLine("Failed to open database: " + ex.Message);
                Console.WriteLine("Press any key to exit");
                Console.ReadKey();
                return;
            }

            SQLiteCommand command = conn.CreateCommand();

            command.CommandText = "ATTACH DATABASE '" + fileNameBackup + "' as T";
            command.ExecuteNonQuery();

            Console.WriteLine("Finding all tables");
            command.CommandText = "SELECT * FROM T.sqlite_master WHERE type='table'";
            List<string> allTables = new List<string>();
            SQLiteDataReader reader = command.ExecuteReader();
            while (reader.Read())
            {
                allTables.Add(reader.GetString(1));
                //string myreader = reader.GetString(0);
                //Console.WriteLine(myreader);
            }
            reader.Close();

            Console.WriteLine("Copying schemas");
            command.CommandText = "SELECT * FROM T.sqlite_master";
            List<string> allSchemas = new List<string>();
            reader = command.ExecuteReader();
            while (reader.Read())
            {
                if (!reader.IsDBNull(4))
                {
                    allSchemas.Add(reader.GetString(4));
                }
            }
            reader.Close();

            Console.WriteLine("Rebuilding schemas...");
            foreach (string schema in allSchemas)
            {
                try
                {
                    command.CommandText = schema;
                    command.ExecuteNonQuery();
                }
                catch (SQLiteException ex)
                {
                    Console.WriteLine("Error: " + ex.Message);
                }
            }
            Console.WriteLine("Finished copying schemas");

            foreach (string table in allTables)
            {
                Console.WriteLine("Copying table: " + table);
                string tTable = "T." + table;
                Int32 count = 0;
                try
                {
                    command.CommandText = "SELECT COUNT(*) FROM " + tTable;
                    reader = command.ExecuteReader();
                    reader.Read();
                    count = reader.GetInt32(0);
                    reader.Close();
                }
                catch (SQLiteException ex)
                {
                    Console.WriteLine("Error: Failed to get row count of " + tTable);
                    Console.WriteLine(ex.Message);
                    continue;
                }

                for (Int32 i = count; i > 0; i--)
                {
                    try
                    {
                        command.CommandText = "INSERT OR IGNORE INTO " + table + " SELECT * FROM " + tTable + " LIMIT " + i.ToString();
                        command.ExecuteNonQuery();
                        break;
                    }
                    catch (SQLiteException ex)
                    {
                        Console.WriteLine("Warning: " + ex.Message);
                    }
                }
                if (count == 0)
                {
                    Console.WriteLine("No data was copied for table: " + table);
                }
                else
                {
                    Console.WriteLine("Finished copying table: " + table);
                }
            }

            conn.Close();
            Console.WriteLine("\nFinished restoring the database");
            Console.WriteLine("Press any key to exit");
            Console.ReadKey();
        }
    }
}
