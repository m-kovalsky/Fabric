// Recreate measures
var sb = new System.Text.StringBuilder();
string newline = Environment.NewLine;
sb.Append("// Run this script in your new model to recreate all measures" + newline + newline);
foreach (var t in Model.Tables)
{
    string tName = t.Name;
    foreach (var m in t.Measures)
    {
        string expr = m.Expression.Replace("\t"," ").Replace("\r"," ").Replace("\n"," ");
        string desc = m.Description.Replace("\t"," ").Replace("\r"," ").Replace("\n"," ");
        
        sb.Append("var x = Model.Tables[\"" + tName +"\"].AddMeasure(\"" + m.Name + "\");" + newline);
        sb.Append("x.Description = \"" + desc + "\";" + newline);
        sb.Append("x.IsHidden = " + m.IsHidden.ToString().ToLower() + ";" + newline);
        sb.Append("x.DisplayFolder = \"" + m.DisplayFolder + "\";" + newline);
        sb.Append("x.FormatString = \"" + m.FormatString + "\";" + newline);
        sb.Append("x.Expression = \"" + expr + "\";" + newline + newline);
    }
}

sb.Output();

// Recreate relationships
sb = new System.Text.StringBuilder();
sb.Append("// Run this script in your new model to recreate all relationships" + newline + newline);
foreach (var r in Model.Relationships)
{
    sb.Append("var obj = Model.AddRelationship();" + newline);
    sb.Append("obj.FromColumn = Model.Tables[\"" + r.FromTable.Name + "\"].Columns[\"" + r.FromColumn.Name + "\"];" + newline);
    sb.Append("obj.ToColumn = Model.Tables[\"" + r.ToTable.Name + "\"].Columns[\"" + r.ToColumn.Name + "\"];" + newline);
    sb.Append("obj.IsActive = " + r.IsActive.ToString().ToLower() + ";" + newline);
    sb.Append("obj.FromCardinality = RelationshipEndCardinality." + r.FromCardinality +";" + newline);
    sb.Append("obj.ToCardinality = RelationshipEndCardinality." + r.ToCardinality +";" + newline);
    sb.Append("obj.CrossFilteringBehavior = CrossFilteringBehavior." + r.CrossFilteringBehavior +";" + newline);
    sb.Append("obj.SecurityFilteringBehavior = SecurityFilteringBehavior." + r.SecurityFilteringBehavior +";" + newline);
    sb.Append("obj.RelyOnReferentialIntegrity = " + r.RelyOnReferentialIntegrity.ToString().ToLower() + ";" + newline + newline);
}

sb.Output();
