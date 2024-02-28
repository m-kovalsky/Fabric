// Recreate measures
var sb = new System.Text.StringBuilder();
string newline = Environment.NewLine;
sb.Append("// Run this script in your new model to recreate all measures" + newline + newline);
int i=0;
foreach (var t in Model.Tables)
{
    string tName = t.Name;
    foreach (var m in t.Measures)
    {
        string expr = m.Expression.Replace("\t"," ").Replace("\r"," ").Replace("\n"," ");
        string desc = m.Description.Replace("\t"," ").Replace("\r"," ").Replace("\n"," ");        
        string varName = "x_" + i.ToString();
        
        sb.Append("var " + varName + " = Model.Tables[\"" + tName +"\"].AddMeasure(\"" + m.Name + "\");" + newline);
        sb.Append(varName + ".Description = \"" + desc + "\";" + newline);
        sb.Append(varName + ".IsHidden = " + m.IsHidden.ToString().ToLower() + ";" + newline);
        sb.Append(varName + ".DisplayFolder = \"" + m.DisplayFolder + "\";" + newline);
        sb.Append(varName + ".FormatString = @\"" + m.FormatString + "\";" + newline);
        sb.Append(varName + ".Expression = \"" + expr + "\";" + newline + newline);
        i++;
    }
}

sb.Output();

// Recreate relationships
sb = new System.Text.StringBuilder();
sb.Append("// Run this script in your new model to recreate all relationships" + newline + newline);
i=0;
foreach (var r in Model.Relationships)
{
    string varName = "x_" + i.ToString();
    sb.Append("var " + varName + " = Model.AddRelationship();" + newline);
    sb.Append(varName + ".FromColumn = Model.Tables[\"" + r.FromTable.Name + "\"].Columns[\"" + r.FromColumn.Name + "\"];" + newline);
    sb.Append(varName + ".ToColumn = Model.Tables[\"" + r.ToTable.Name + "\"].Columns[\"" + r.ToColumn.Name + "\"];" + newline);
    sb.Append(varName + ".IsActive = " + r.IsActive.ToString().ToLower() + ";" + newline);
    sb.Append(varName + ".FromCardinality = RelationshipEndCardinality." + r.FromCardinality +";" + newline);
    sb.Append(varName + ".ToCardinality = RelationshipEndCardinality." + r.ToCardinality +";" + newline);
    sb.Append(varName + ".CrossFilteringBehavior = CrossFilteringBehavior." + r.CrossFilteringBehavior +";" + newline);
    sb.Append(varName + ".SecurityFilteringBehavior = SecurityFilteringBehavior." + r.SecurityFilteringBehavior +";" + newline);
    sb.Append(varName + ".RelyOnReferentialIntegrity = " + r.RelyOnReferentialIntegrity.ToString().ToLower() + ";" + newline + newline);
    i++;
}

sb.Output();