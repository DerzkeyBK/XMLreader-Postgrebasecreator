using System;
using Npgsql;
using Microsoft.AspNetCore;
using Microsoft.Net;
using System.Xml;
using System.IO;
using System.Collections.Generic;



namespace WorkXML
{
    
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("111111");

            var path =Console.ReadLine();//c:\base new\AS_NORMDOC_20190527_b5c0569e-10cf-44f1-ae3b-fe8b87331d84.XML
            var stream = File.OpenRead(path);
            var myTextReader = XmlReader.Create(stream);
            var connection_string = "Server=127.0.0.1;Port=5432;User Id=postgres;Password=1q2w3e4r;Database=postgres;";
            NpgsqlConnection conn = new NpgsqlConnection(connection_string);


            string tablename = "1";
            var id = 0;
            bool check = false;

            while (myTextReader.Read())
            {
                string insert1="";
                string insert2="";
                string[] array1 = new string[0];
                string[] array2 = new string[0];
                string command_text;
                NpgsqlCommand comm;
                if (!(myTextReader.NodeType == XmlNodeType.Element))
                {
                    //пропускаем информации о версии и кодировке xml
                    continue;
                }
                if (!myTextReader.HasAttributes)
                {
                    //таблицы будут называться по корневому узлу. и так получилось что именно у этого узла нет аттрибутов
                    //поэтому при обработке xml без аттрибутов мы будем в нашей базе создавать новую таблицу
                    //или проверять что мы нашли нашу таблицу и в последствии заливать данные в неё
                    command_text = "CREATE TABLE " + myTextReader.LocalName + "\n(\nid SERIAL PRIMARY KEY\n)";
                    try
                    {
                        comm = new NpgsqlCommand(command_text, conn);
                        conn.Open();
                        var result = comm.ExecuteScalar();
                        conn.Close();
                        Console.WriteLine("Таблица {0} была создана", myTextReader.LocalName);
                        tablename = myTextReader.LocalName;
                        continue;
                    }
                    catch
                    {
                        //эта ветка нужна для того,если мы работаем с уже созданной таблицей
                        Console.WriteLine("Таблица {0} уже былы создана", myTextReader.LocalName);
                        tablename = myTextReader.LocalName;
                        command_text = "SELECT id FROM " + tablename + " ORDER BY id DESC LIMIT 1";
                        comm = new NpgsqlCommand(command_text, conn);
                        var result = comm.ExecuteScalar();
                        id = Convert.ToInt32(result);
                        Console.WriteLine("Последнее значение id = ", id);
                        conn.Close();
                        check = true;
                        Console.WriteLine("Идёт заполнение данных");
                        continue;

                    }
                }
                else
                {
                    while (myTextReader.MoveToNextAttribute())
                    {
                        //заполнение таблицы
                        if (!check)
                        {
                            //создание столбцов в таблице
                            command_text = "ALTER TABLE " + tablename + "\nADD " + myTextReader.LocalName + " TEXT";
                            comm = new NpgsqlCommand(command_text, conn);
                            conn.Open();
                            var result = comm.ExecuteNonQuery().ToString();
                            conn.Close();
                            Console.WriteLine("В таблицу был добавлен столбец {0}", myTextReader.LocalName);
                            Array.Resize(ref array1, array1.Length + 1);
                            Array.Resize(ref array2, array2.Length + 1);
                            array1[array1.Length-1] = myTextReader.LocalName;
                            array2[array2.Length-1] = myTextReader.Value;
                        }
                        else
                        {
                            //просто заполнение последующих данных
                            Array.Resize(ref array1, array1.Length + 1);
                            Array.Resize(ref array2, array2.Length + 1);
                            array1[array1.Length - 1] = myTextReader.LocalName;
                            array2[array2.Length - 1] = myTextReader.Value;
                        }
                    }
                    check = true;
                }

                if (check)
                {
                    id++;
                    for (int i = 0; i < array1.Length; i++)
                    {
                        insert1 =insert1 + array1[i] + ",";
                        insert2 =insert2 +"'"+ array2[i] +"'" + ",";
                    }
                    insert1 = insert1.Remove(insert1.Length - 1);
                    insert2 = insert2.Remove(insert2.Length - 1);
                    command_text = "INSERT INTO " + tablename + " (id," + insert1 + ") VALUES (" + id + "," + insert2 + ")";
                    comm = new NpgsqlCommand(command_text, conn);
                    conn.Open();
                    var resultstr = comm.ExecuteNonQuery().ToString();
                    conn.Close();
                }
                Console.ReadKey();
            }
        }
    }
}
