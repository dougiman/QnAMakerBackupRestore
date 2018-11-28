// This is a prototype tool that allows for extraction of data from an Azure Search index
// to TSV. Once done you can attach it to the QnAMaker runtime
// Since this tool is still under development, it should not be used for production usage

using Microsoft.Azure.Search;
using Microsoft.Azure.Search.Models;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace AzureSearchBackupRestore
{
    class Program
    {
        private static string SourceSearchServiceName = "<Source Azure search service name>";
        private static string SourceAPIKey = "<Source Azure search admin key>";

        private static SearchServiceClient SourceSearchClient;
        private static ISearchIndexClient SourceIndexClient;

        private static int MaxBatchSize = 500;          // Files will contain this many documents / file and can be up to 1000
        private static int ParallelizedJobs = 10;       // Output content in parallel jobs

        static void Main(string[] args)
        {
            // Get all the indexes from source
            Indexes indexList = GetIndexes();

            // For each index in source do the following
            foreach (Index sourceIndex in indexList.value)
            {
                string nameOfIndex = sourceIndex.name;

                SourceSearchClient = new SearchServiceClient(SourceSearchServiceName, new SearchCredentials(SourceAPIKey));
                SourceIndexClient = SourceSearchClient.Indexes.GetClient(nameOfIndex);

                // Extract the content to TSV files 
                int SourceDocCount = GetCurrentDocCount(SourceIndexClient);
                LaunchParallelDataExtraction(SourceDocCount, nameOfIndex);     // Output content from index to TSV files
            }

            Console.WriteLine("Press any key to continue...");
            Console.ReadLine();
        }

        static void LaunchParallelDataExtraction(int CurrentDocCount, string nameOfIndex)
        {
            // Launch output in parallel
            string IDFieldName = GetIDFieldName(nameOfIndex);
            int FileCounter = 0;
            for (int batch = 0; batch <= (CurrentDocCount / MaxBatchSize); batch += ParallelizedJobs)
            {

                List<Task> tasks = new List<Task>();
                for (int job = 0; job < ParallelizedJobs; job++)
                {
                    FileCounter++;
                    int fileCounter = FileCounter;
                    if ((fileCounter - 1) * MaxBatchSize < CurrentDocCount)
                    {
                        Console.WriteLine("Writing {0} docs to {1}", MaxBatchSize, nameOfIndex + fileCounter + ".tsv");

                        tasks.Add(Task.Factory.StartNew(() =>
                            ExportToTSV((fileCounter - 1) * MaxBatchSize, IDFieldName, nameOfIndex + fileCounter + ".tsv")
                        ));
                    }

                }
                Task.WaitAll(tasks.ToArray());  // Wait for all the stored procs in the group to complete
            }

            return;
        }

        static void ExportToTSV(int Skip, string IDFieldName, string FileName)
        {
            // Extract all the documents from the selected index to TSV files in batches of 500 docs / file
            string tsv = string.Empty;
            try
            {
                SearchParameters sp = new SearchParameters()
                {
                    SearchMode = SearchMode.All,
                    Top = MaxBatchSize,
                    Skip = Skip
                };
                DocumentSearchResult response = SourceIndexClient.Documents.Search("*", sp);

                // Add the header.
                tsv += "Question\t";
                tsv += "Answer\t";
                tsv += "Source\t";
                tsv += "Metadata\r\n";

                foreach (var doc in response.Results)
                {
                    string metadataString = string.Empty;

                    // Get the questions, answer, and source from this document.
                    // Note there is a 1:many relationship between an answer and questions.
                    doc.Document.TryGetValue("questions", out var questions);
                    doc.Document.TryGetValue("answer", out var answer);
                    doc.Document.TryGetValue("source", out var source);

                    // We need to reformat the metadata given how it is stored in Azure Search
                    // Create a list of all metadata elements in this doc.
                    var metadataList =
                        from keyValuePair in doc.Document
                        where keyValuePair.Key.StartsWith("metadata_")
                        select keyValuePair;

                    // Form a string with all of the valid metadata elements in this doc.
                    for (int i = 0; i < metadataList.Count(); i++)
                    {
                        string key = metadataList.ElementAt(i).Key.Replace("metadata_", "");
                        string value = (string) metadataList.ElementAt(i).Value;

                        if (!string.IsNullOrEmpty(key) &&
                            !string.IsNullOrEmpty(value))
                        {
                            // If there is existing metadata, add a delimiter.
                            if (!string.IsNullOrEmpty(metadataString))
                            {
                                metadataString += "|";
                            }

                            metadataString += key + ":" + value;
                        }
                    }

                    // Add the various elements to the TSV.
                    // Because of the 1:many question relationship, we will duplicate results in the TSV.
                    foreach (var question in (string[]) questions)
                    {                          
                        tsv += question + "\t";
                        tsv += answer + "\t";
                        tsv += source + "\t";
                        tsv += metadataString;
                        tsv += "\r\n";
                    }
                }
                File.AppendAllText(FileName, tsv);
                Console.WriteLine("Total documents written: {0}", response.Results.Count.ToString());
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: {0}", ex.Message.ToString());
            }
            return;
        }  

        static string GetIDFieldName(string nameOfIndex)
        {
            // Find the id field of this index
            string IDFieldName = string.Empty;
            try
            {
                var schema = SourceSearchClient.Indexes.Get(nameOfIndex);
                foreach (var field in schema.Fields)
                {
                    if (field.IsKey)
                    {
                        IDFieldName = Convert.ToString(field.Name);
                        break;
                    }
                }

            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: {0}", ex.Message.ToString());
            }
            return IDFieldName;
        }

        static Indexes GetIndexes()
        {
            // Get all the indexes from the source
            Uri ServiceUri = new Uri("https://" + SourceSearchServiceName + ".search.windows.net");
            HttpClient HttpClient = new HttpClient();
            HttpClient.DefaultRequestHeaders.Add("api-key", SourceAPIKey);

            string indexResponse = string.Empty;
            try
            {
                Uri uri = new Uri(ServiceUri, "/indexes/");
                HttpResponseMessage response = AzureSearchHelper.SendSearchRequest(HttpClient, HttpMethod.Get, uri, null, "$select=name");
                AzureSearchHelper.EnsureSuccessfulSearchResponse(response);
                indexResponse = response.Content.ReadAsStringAsync().Result.ToString();

            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: {0}", ex.Message.ToString());
            }

            return JsonConvert.DeserializeObject<Indexes>(indexResponse);
        }

        static int GetCurrentDocCount(ISearchIndexClient IndexClient)
        {
            // Get the current doc count of the specified index
            try
            {
                SearchParameters sp = new SearchParameters()
                {
                    SearchMode = SearchMode.All,
                    IncludeTotalResultCount = true
                };

                DocumentSearchResult response = IndexClient.Documents.Search("*", sp);
                return Convert.ToInt32(response.Count);

            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: {0}", ex.Message.ToString());
            }

            return -1;

        }
    }

    public class Index
    {
        public string name { get; set; }
    }

    public class Indexes
    {
        [JsonProperty("odata.context")]
        public string context { get; set; }

        public List<Index> value { get; set; }
    }
}