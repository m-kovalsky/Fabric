#r "System.IO"
#r "System.IO.Compression.FileSystem"
#r "System.Xml.Linq"

using System.IO;
using System.IO.Compression;
using System.Xml.Linq;

public class QueryMetadata
{
    public string QueryName { get; set; }
    public object QueryGroupId { get; set; }
    public string LastKnownIsParameter { get; set; }
    public string LastKnownResultTypeName { get; set; }
    public bool LoadEnabled { get; set; }
    public bool IsHidden { get; set; }
}
public class RootObject
{
    public string DocumentLocale { get; set; }
    public string EngineVersion { get; set; }
    public List<QueryMetadata> QueriesMetadata { get; set; }
    public List<object> QueryGroups { get; set; }
}

// ***Make sure to fill in the folderPath parameter***

string fileName = "PowerQueryTemplate"; // This is the name of the .pqt file which will be created
string folderPath = @"C:\Desktop\MyFolder"; // This is the folder where the .pqt file will be created
string subFolderPath = folderPath + @"\" + "pqtnewfolder";
Directory.CreateDirectory(subFolderPath);

// STEP 1: Create MashupDocument.pq
string mdfileName = "MashupDocument.pq";
string mdfilePath = subFolderPath + @"\" + mdfileName;
var sb = new System.Text.StringBuilder();
string newline = Environment.NewLine;
sb.Append("section Section1;");
foreach (var t in Model.Tables)
{
    string tName = "#\"" + t.Name + "\"";
    sb.Append(newline + "shared " + tName + " = ");
    foreach (var p in t.Partitions)
    {
        sb.Append(p.Query + ";");
    }
}

foreach (var e in Model.Expressions)
{
    string expr = e.Expression;
    string eName = "#\"" + e.Name + "\"";
    sb.Append(newline + "shared " + eName + " = " + expr + ";");
}

File.WriteAllText(mdfilePath, sb.ToString());

// STEP 2: Create the MashupMetadata.json file based on looping through the table names to create a QueryMetadata object for each table
string mmFileName = "MashupMetadata.json";
string mmFilePath = subFolderPath + @"\" + mmFileName;

var queryMetadata = new List<QueryMetadata>();

// Create a QueryMetadata object for each table
foreach (var t in Model.Tables)
{
    string tName = t.Name;
    queryMetadata.Add( new QueryMetadata {QueryName = tName, QueryGroupId = null, LastKnownIsParameter = null, LastKnownResultTypeName = null, LoadEnabled = true, IsHidden = false});
}
foreach (var e in Model.Expressions)
{
    string eName = e.Name;
    if (e.Kind.ToString() == "M")
    {
        queryMetadata.Add( new QueryMetadata {QueryName = eName, QueryGroupId = null, LastKnownIsParameter = null, LastKnownResultTypeName = null, LoadEnabled = true, IsHidden = false});
    }
    else
    {
        queryMetadata.Add( new QueryMetadata {QueryName = eName, QueryGroupId = null, LastKnownIsParameter = null, LastKnownResultTypeName = null, LoadEnabled = false, IsHidden = false});
    }
}

// Create a single Root object
var rootObject = new {DocumentLocale = "en-US", EngineVersion = "2.126.453.0", QueriesMetadata = queryMetadata, QueryGroups = new List<object>()};
string jsonContent = JsonConvert.SerializeObject(rootObject, Formatting.Indented);
System.IO.File.WriteAllText(mmFilePath, jsonContent);

// STEP 3: Create Metadata.json file
string mFileName = "Metadata.json";
string mFilePath = subFolderPath + @"\" + mFileName;
var metaData = new {Name = fileName, Description = "", Version = "1.0.0.0"};
jsonContent = JsonConvert.SerializeObject(metaData, Formatting.Indented);
System.IO.File.WriteAllText(mFilePath, jsonContent);

// STEP 4: Create [Content_Types].xml file
XNamespace ns = "http://schemas.openxmlformats.org/package/2006/content-types";

XDocument xmlDocument = new XDocument(
    new XDeclaration("1.0", "utf-8", null),
    new XElement(ns + "Types",
        new XElement(ns + "Default",
            new XAttribute("Extension", "json"),
            new XAttribute("ContentType", "application/json")),
        new XElement(ns + "Default",
            new XAttribute("Extension", "pq"),
            new XAttribute("ContentType", "application/x-ms-m"))
    )
);

string xmlFileName = "[Content_Types].xml";
string xmlFilePath = subFolderPath + @"\" + xmlFileName;
xmlDocument.Save(xmlFilePath);

// STEP 5: Zip up the 4 files
string zipFileName = fileName + ".zip";
string zipFilePath = folderPath + @"\" + zipFileName;
ZipFile.CreateFromDirectory(subFolderPath, zipFilePath);

// STEP 6: Convert the zip file back into a pqt file
string newExt = ".pqt";
string directory = Path.GetDirectoryName(zipFilePath);
string fileNameWithoutExtension = Path.GetFileNameWithoutExtension(zipFilePath);
string newFilePath = Path.Combine(directory, fileNameWithoutExtension + newExt);
File.Move(zipFilePath, newFilePath);

// STEP 7: Delete subFolder directory which is no longer needed
Directory.Delete(subFolderPath, true);

Info("The " + fileName + ".pqt file has been created here: " + folderPath);