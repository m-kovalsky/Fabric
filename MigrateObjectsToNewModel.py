import sempy
import sempy.fabric as fabric
import pandas as pd
import numpy as np
sempy.fabric._client._utils._init_analysis_services()
from sempy.fabric._cache import _get_or_create_workspace_client
from sempy.fabric._client._connection_mode import ConnectionMode
import Microsoft.AnalysisServices.Tabular as TOM
from sempy.fabric._client import DatasetXmlaClient
import System

# This script will migrate the following objects from one semantic model to another. This assumes both models have the same table/column structure
    # Table properties, column properties, hierarchies, measures, relationships, roles, row level security (RLS), perspectives, translations, calculation groups & calculation items

datasetName = '' #Enter the old semantic model name
newDatasetName = '' #Enter the new semantic model name

def list_tables(datasetName, workspaceName = None):

    if workspaceName == None:
        workspaceId = fabric.get_workspace_id()
        workspaceName = fabric.resolve_workspace_name(workspaceId)

    workspace_client = _get_or_create_workspace_client(workspaceName)
    ds = workspace_client.get_dataset(datasetName)
    m = ds.Model

    header = pd.DataFrame(columns=['Name', 'Type', 'Hidden', 'Data Category', 'Description', 'Refresh Policy', 'Source Expression'])
    df = pd.DataFrame(header)

    for t in m.Tables:
        tableType = "Table"
        rPolicy = bool(t.RefreshPolicy)
        sourceExpression = None
        if str(t.CalculationGroup) != "None":
            tableType = "Calculation Group"
        else:
            for p in t.Partitions:
                if str(p.SourceType) == "Calculated":
                    tableType = "Calculated Table"

        if rPolicy:
            sourceExpression = t.RefreshPolicy.SourceExpression

        new_data = {'Name': t.Name, 'Type': tableType,'Hidden': t.IsHidden, 'Data Category': t.DataCategory, 'Description': t.Description, 'Refresh Policy': rPolicy, 'Source Expression': sourceExpression}
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df

workspaceId = fabric.get_workspace_id()
workspaceName = fabric.resolve_workspace_name(workspaceId)

tom_server = _get_or_create_workspace_client(workspaceName).get_dataset_client(newDatasetName, ConnectionMode.XMLA)._get_connected_dataset_server(readonly=False)

dfT = list_tables(datasetName)
dfC = fabric.list_columns(datasetName)
dfM = fabric.list_measures(datasetName)
dfR = fabric.list_relationships(datasetName)
dfRole = fabric.get_roles(datasetName)
dfRLS = fabric.get_row_level_security_permissions(datasetName)
dfCI = fabric.list_calculation_items(datasetName)
dfP = fabric.list_perspectives(datasetName)
dfTranslation = fabric.list_translations(datasetName)
dfH = fabric.list_hierarchies(datasetName)

recT, cfbT, sfbT, mp = TOM.RelationshipEndCardinality, TOM.CrossFilteringBehavior, TOM.SecurityFilteringBehavior, TOM.ModelPermission

for d in tom_server.Databases:
    if d.Name == newDatasetName:
        print(f"Updating '{d.Name}' based on '{datasetName}'...")
        m = d.Model
        isDirectLake = any(str(p.Mode) == 'DirectLake' for t in m.Tables for p in t.Partitions)

        print(f"\nUpdating table properties...")
        for t in m.Tables:
            t.IsHidden = bool(dfT.loc[dfT['Name'] == t.Name, 'Hidden'].iloc[0])
            t.Description = dfT.loc[dfT['Name'] == t.Name, 'Description'].iloc[0]
            t.DataCategory = dfT.loc[dfT['Name'] == t.Name, 'Data Category'].iloc[0]

            print(f"The '{t.Name}' table's properties have been updated.")

        print(f"\nUpdating column properties...")
        for t in m.Tables:
            for c in t.Columns:
                if not c.Name.startswith('RowNumber-'):
                    dfC_filt = dfC[(dfC['Table Name'] == t.Name) & (dfC['Column Name'] == c.Name)]
                    cName = dfC_filt['Column Name'].iloc[0]
                    c.Name = cName
                    c.SourceColumn = cName.replace(' ', '_')
                    c.IsHidden = bool(dfC_filt['Hidden'].iloc[0])
                    c.DataType = System.Enum.Parse(TOM.DataType, dfC_filt['Data Type'].iloc[0])
                    c.DisplayFolder = dfC_filt['Display Folder'].iloc[0]
                    c.FormatString = dfC_filt['Format String'].iloc[0]
                    #c.SummarizeBy = System.Enum.Parse(TOM.SummarizationType, dfC_filt['Summarize By'].iloc[0])
                    c.DataCategory = dfC_filt['Data Category'].iloc[0]
                    c.IsKey = bool(dfC_filt['Key'].iloc[0])
                    sbc = dfC_filt['Sort By Column'].iloc[0]

                    if sbc != None:
                        try:
                            c.SortByColumn = m.Tables[t.Name].Columns[sbc]
                        except:
                            print(f"ERROR: Failed to create '{sbc}' as a Sort By Column for the '{c.Name}' in the '{t.Name}' table.")
                    print(f"The '{t.Name}'[{c.Name}] column's properties have been updated.")

        print(f"\nCreating hierarchies...")
        for row in dfH[['Table Name', 'Hierarchy Name']].drop_duplicates().itertuples(index=False):
            tName, hName = row
            hDesc = dfH[(dfH['Table Name'] == tName) & (dfH['Hierarchy Name'] == hName)]['Hierarchy Description'].iloc[0]
            hHidden = dfH[(dfH['Table Name'] == tName) & (dfH['Hierarchy Name'] == hName)]['Hierarchy Hidden'].iloc[0]
        
            if any(h.Name == hName and t.Name == tName for t in m.Tables for h in t.Hierarchies):
                print(f"WARNING: The '{hName}' hierarchy already exists in the '{tName}' table.")
            else:
                try:
                    hier = TOM.Hierarchy()
                    hier.Name = hName
                    hier.Description = hDesc
                    hier.IsHidden = bool(hHidden)

                    m.Tables[tName].Hierarchies.Add(hier)
                    print(f"The '{hName}' hierarchy within the '{tName}' table has been added.")
                except:
                    print(f"ERROR: The '{hName}' hierarchy within the '{tName}' table was not added.")
        for index, row in dfH.iterrows():
            tName = row['Table Name']
            cName = row['Column Name']
            hName = row['Hierarchy Name']
            hDesc = row['Hierarchy Description']
            hHid = row['Hierarchy Hidden']
            lName = row['Level Name']
            lDesc = row['Level Description']
            lOrdinal = row['Level Ordinal']
            lvl = TOM.Level()

            if any(l.Name == lName and h.Name == hName and t.Name == tName for t in m.Tables for h in t.Hierarchies for l in h.Levels):
                print(f"WARNING: The '{lName}' level in the '{hName}' hierarchy within the '{tName}' table already exists.")
            else:
                try:
                    lvl.Column = m.Tables[tName].Columns[cName]
                    lvl.Name = lName
                    lvl.Description = lDesc
                    lvl.Ordinal = lOrdinal

                    m.Tables[tName].Hierarchies[hName].Levels.Add(lvl)
                    print(f"The '{lName}' level within the '{hName}' hierarchy in the '{tName}' table has been added.")
                except:
                    pass

        print(f"\nCreating measures...")
        for i, r in dfM.iterrows():
            tName = r['Table Name']
            mName = r['Measure Name']
            mExpr = r['Measure Expression']
            mHidden = bool(r['Measure Hidden'])
            mDF = r['Measure Display Folder']
            mDesc = r['Measure Description']
            #mFS = r['Measure Format String']
            
            measure = TOM.Measure()
            measure.Name = mName
            measure.Expression = mExpr
            measure.IsHidden = mHidden
            measure.DisplayFolder = mDF
            measure.Description = mDesc
            #measure.FormatString = mFS

            for t in m.Tables:
                if t.Name == tName:
                    if not any(existing_measure.Name == mName for existing_measure in t.Measures):
                        t.Measures.Add(measure)
                        print(f"'{mName}' has been added to the '{tName}' table.")
                    else:
                        print(f"WARNING: '{mName}' already exists as a measure. No measure was added.")

        print(f"\nCreating relationships...")
        for index, row in dfR.iterrows():
            fromTable = row['From Table']
            fromColumn = row['From Column']
            toTable = row['To Table']
            toColumn = row['To Column']
            isActive = row['Active']
            cfb = row['Cross Filtering Behavior']
            sfb = row['Security Filtering Behavior']
            rori = row['Rely On Referential Integrity']
            mult = row['Multiplicity']

            if cfb == 'OneDirection':
                crossFB = cfbT.OneDirection
            elif cfb == 'BothDirections':
                crossFB = cfbT.BothDirections
            elif cfb == 'Automatic':
                crossFB = cfbT.Automatic

            if sfb == 'OneDirection':
                secFB = sfbT.OneDirection
            elif sfb == 'BothDirections':
                secFB = sfbT.BothDirections
            elif sfb == 'None':
                secFB = System.Enum.Parse(TOM.SecurityFilteringBehavior, "None")

            if mult[0] == 'm':
                fromCardinality = recT.Many
            elif mult[0] == '1':
                fromCardinality = recT.One
            elif mult[0] == '0':
                fromCardinality = System.Enum.Parse(TOM.RelationshipEndCardinality, "None")
            if mult[-1] == 'm':
                toCardinality = recT.Many
            elif mult[-1] == '1':
                toCardinality = recT.One
            elif mult[-1] == '0':
                toCardinality = System.Enum.Parse(TOM.RelationshipEndCardinality, "None")

            if any(r.FromTable.Name == fromTable and r.FromColumn.Name == fromColumn and r.ToTable.Name == toTable and r.ToColumn.Name == toColumn for r in m.Relationships):
                print(f"WARNING: '{fromTable}'[{fromColumn}] -> '{toTable}'[{toColumn}] already exists as a relationship in the semantic model.")
            elif isDirectLake and any(r.FromTable.Name == fromTable and r.FromColumn.Name == fromColumn and r.ToTable.Name == toTable and r.ToColumn.Name == toColumn and (r.FromColumn.DataType == 'DateTime' or r.ToColumn.DataType == 'DateTime') for r in m.Relationships):
                print(f"ERROR: '{fromTable}'[{fromColumn}] -> '{toTable}'[{toColumn}] was not created since relationships based on DateTime columns are not supported.")
            elif isDirectLake and any(r.FromTable.Name == fromTable and r.FromColumn.Name == fromColumn and r.ToTable.Name == toTable and r.ToColumn.Name == toColumn and (r.FromColumn.DataType != r.ToColumn.DataType) for r in m.Relationships):
                print(f"ERROR: '{fromTable}'[{fromColumn}] -> '{toTable}'[{toColumn}] was not created since columns used in a relationship must have the same data type.")
            else:
                try:
                    rel = TOM.SingleColumnRelationship()
                    rel.FromColumn = m.Tables[fromTable].Columns[fromColumn]
                    rel.FromCardinality = fromCardinality
                    rel.ToColumn = m.Tables[toTable].Columns[toColumn]
                    rel.ToCardinality = toCardinality
                    rel.IsActive = isActive
                    rel.CrossFilteringBehavior = crossFB
                    rel.SecurityFilteringBehavior = secFB
                    rel.RelyOnReferentialIntegrity = rori
                    m.Relationships.Add(rel)
                    print(f"'{fromTable}'[{fromColumn}] -> '{toTable}'[{toColumn}] relationship has been added.")
                except:
                    print(f"ERROR: '{fromTable}'[{fromColumn}] -> '{toTable}'[{toColumn}] relationship has not been created.")

        print('\nCreating roles...')
        for index, row in dfRole.iterrows():
            roleName = row['Role']
            roleDesc = row['Description']
            modPerm = row['Model Permission']            

            role = TOM.ModelRole()
            role.Name = roleName
            role.Description = roleDesc

            if modPerm == 'Read':
                role.ModelPermission = mp.Read
            elif modPerm == 'ReadRefresh':
                role.ModelPermission = mp.ReadRefresh
            elif modPerm == 'Refresh':
                role.ModelPermission = mp.Refresh
            elif modPerm == 'Administrator':
                role.ModelPermission = mp.Administrator
            elif modPerm == 'None':
                role.ModelPermission = System.Enum.Parse(TOM.ModelPermission, "None")

            if any(r.Name == roleName for r in m.Roles):
                print(f"WARNING: The '{roleName}' role already exists in the semantic model.")
            else:
                m.Roles.Add(role)
                print(f"The '{roleName}' role has been added.")

        print('\nCreating row level security...')
        for index, row in dfRLS.iterrows():
            roleName = row['Role']
            tName = row['Table']
            expr = row['Filter Expression']

            try:
                tp = TOM.TablePermission()
                tp.Table = m.Tables[tName]
                tp.FilterExpression = expr

                if any(r.Name == roleName and tperm.Table.Name == tName for r in m.Roles for tperm in r.TablePermissions):
                    print(f"WARNING: Row level security already exists for the '{tName}' table within the '{roleName}' role.")
                else:
                    m.Roles[roleName].TablePermissions.Add(tp)
                    print(f"Row level security for the '{tName}' table within the '{roleName}' role has been added.")
            except:
                print(f"ERROR: Row level security not created for the '{tName}' table within the '{roleName}' role.")

        print('\nCreating perspectives...')
        for pName in dfP['Perspective Name'].unique():
            if not any(existing_persp.Name == pName for existing_persp in m.Perspectives):
                persp = TOM.Perspective()
                persp.Name = pName
                m.Perspectives.Add(persp)
                print(f"The '{pName}' perspective has been added.")
            else:
                print(f"WARNING: The '{pName}' perspective already exists in the semantic model.")
        print('\nAdding objects to perspectives...')
        for index, row in dfP.iterrows():
            pName = row['Perspective Name']
            tName = row['Table Name']
            oName = row['Object Name']
            oType = row['Object Type']
            tType = dfT.loc[(dfT['Name'] == tName), 'Type'].iloc[0]

            perspTbl, perspCol, perspMeas, perspHier = TOM.PerspectiveTable(), TOM.PerspectiveColumn(), TOM.PerspectiveMeasure(), TOM.PerspectiveHierarchy()

            if tType == 'Table':
                try:
                    perspTbl.Table = m.Tables[tName]
                except:
                    print(f"The '{tName}' was not added to the '{pName}' perspective.")
                try:
                    m.Perspectives[pName].PerspectiveTables.Add(perspTbl)
                    print(f"'The '{tName}' table has been added to the '{pName}' perspective.")
                except:
                    pass

                if oType == 'Column':
                    colType = dfC.loc[(dfC['Table Name'] == tName) &  (dfC['Column Name'] == oName), 'Type'].iloc[0]
                    if colType == 'Data':
                        try:
                            perspCol.Column = m.Tables[tName].Columns[oName]
                        except:
                            print(f"WARNING: The {tName}'[{oName}] {oType.lower()} does not exist in the model. As such, it was not added to the '{pName}' perspective.")
                        try:
                            m.Perspectives[pName].PerspectiveTables[tName].PerspectiveColumns.Add(perspCol)
                            print(f"'The {tName}'[{oName}] {oType.lower()} has been added to the '{pName}' perspective.")
                        except:
                            print(f"WARNING The {tName}'[{oName}] {oType.lower()} was not added to the '{pName}' perspective.")
                elif oType == 'Measure':
                    try:
                        perspMeas.Measure = m.Tables[tName].Measures[oName]
                    except:
                        print(f"WARNING: The {tName}'[{oName}] {oType.lower()} does not exist in the model. As such, it was not added to the '{pName}' perspective.")
                    try:
                        m.Perspectives[pName].PerspectiveTables[tName].PerspectiveMeasures.Add(perspMeas)
                        print(f"'The {tName}'[{oName}] {oType.lower()} has been added to the '{pName}' perspective.")
                    except:
                        print(f"WARNING: The {tName}'[{oName}] {oType.lower()} was not added to the '{pName}' perspective.")
                elif oType == 'Hierarchy':
                    try:            
                        perspHier.Hierarchy = m.Tables[tName].Hierarchies[oName]
                    except:
                        print(f"WARNING: The {tName}'[{oName}] {oType.lower()} does not exist in the model. As such, it was not added to the '{pName}' perspective.")
                    try:
                        m.Perspectives[pName].PerspectiveTables[tName].PerspectiveHierarchies.Add(perspHier)
                        print(f"'The {tName}'[{oName}] {oType.lower()} has been added to the '{pName}' perspective.")
                    except:
                        print(f"WARNING: The {tName}'[{oName}] {oType.lower()} was not added to the '{pName}' perspective.")
        
        for cgName in dfCI['Calculation Group Name'].unique():
            cgExists = any(t.Name == cgName for t in m.Tables if t.CalculationGroup is not None)
            if cgExists:
                print(f"WARNING: '{cgName}' already exists as a calculation group in the semantic model.")
            else:
                isHidden = bool(dfCI.loc[(dfCI['Calculation Group Name'] == cgName), 'Hidden'].iloc[0])
                prec = dfCI.loc[(dfCI['Calculation Group Name'] == cgName), 'Precedence'].iloc[0]
                desc = dfCI.loc[(dfCI['Calculation Group Name'] == cgName), 'Description'].iloc[0]
                tbl = TOM.Table()
                tbl.Name = cgName
                tbl.CalculationGroup = TOM.CalculationGroup()
                tbl.IsHidden = isHidden
                tbl.CalculationGroup.Precedence = int(prec)
                tbl.Description = desc                                    

                print('\nCreating calculation group columns...')
                dfC_filt = dfC[(dfC['Table Name'] == cgName) ]
                for index, row in dfC_filt.iterrows():
                    colName = row['Column Name']
                    colType = row['Type']
                    sbc = row['Sort By Column']

                    col = TOM.DataColumn()
                    col.Name = colName
                    col.Description = row['Description']
                    col.DataType = System.Enum.Parse(TOM.DataType, row['Data Type'])
                    col.IsHidden = bool(row['Hidden'])
                    col.FormatString = row['Format String']
                    col.SourceColumn = row['Source']
                    col.DataCategory = row['Data Category']
                    col.DisplayFolder = row['Display Folder']
                    
                    try:
                        col.SortByColumn = m.Tables[cgName].Columns[sbc]
                    except:
                        pass

                    if colType == 'Data':  
                        tbl.Columns.Add(col)
                        print(f"The '{colName}' column in the '{cgName}' calculation group table has been added.")
                        m.DiscourageImplicitMeasures = True

                calcItems = dfCI.loc[dfCI['Calculation Group Name'] == cgName, 'Calculation Item Name'].unique()

                print('\nCreating calculation items...')
                for calcItem in calcItems:
                    ciExists = any(ci.Name  == calcItem for t in m.Tables if t.Name == tName and t.CalculationGroup is not None for ci in t.CalculationGroup.CalculationItems)
                    if ciExists:
                        print(f"The '{calcItem}' calculation item already exists in the '{cgName}' calculation group.")
                    else:
                        ordinal = dfCI.loc[(dfCI['Calculation Group Name'] == cgName) & (dfCI['Calculation Item Name'] == calcItem), 'Ordinal'].iloc[0]
                        expr = dfCI.loc[(dfCI['Calculation Group Name'] == cgName) & (dfCI['Calculation Item Name'] == calcItem), 'Expression'].iloc[0]
                        fse = dfCI.loc[(dfCI['Calculation Group Name'] == cgName) & (dfCI['Calculation Item Name'] == calcItem), 'Format String Expression'].iloc[0]

                        ci = TOM.CalculationItem()
                        fsd = TOM.FormatStringDefinition()
                        ci.Name = calcItem
                        ci.Ordinal = int(ordinal)
                        ci.Expression = expr
                        ci.FormatStringDefinition = fsd.Expression = fse

                        tbl.CalculationGroup.CalculationItems.Add(ci)
                        print(f"The '{calcItem}' calculation item has been added to the '{cgName}' calculation group.")

                print('\nCreating calculation groups...')
                part = TOM.Partition()
                part.Name = cgName
                part.Source = TOM.CalculationGroupSource()

                tbl.Partitions.Add(part)       

                m.Tables.Add(tbl)
                print(f"'{cgName}' has been added as a calculation group.")  

        print('\nCreating translation languages...')
        for trName in dfTranslation['Culture Name'].unique():
            if not any(exiting_cult.Name == trName for exiting_cult in m.Cultures):
                cult = TOM.Culture()
                cult.Name = trName
                m.Cultures.Add(cult)
                print(f"The '{trName}' translation language has been added.")
            else:
                    print(f"WARNING: The '{trName}' translation language already exists in the semantic model.")
        print('\nCreating translation values...')
        for index, row in dfTranslation.iterrows():
            trName = row['Culture Name']
            tName = row['Table Name']
            oName = row['Object Name']
            oType = row['Object Type']
            translation = row['Translation']
            prop = row['Property']

            objTrans, transProp = TOM.ObjectTranslation(), TOM.TranslatedProperty
            objTrans.Value = translation

            property_mapping = {
                'Caption': transProp.Caption,
                'Description': transProp.Description,
                'DisplayFolder': transProp.DisplayFolder
            }

            if prop in property_mapping:
                    objTrans.Property = property_mapping[prop]

            if oType == 'Table':
                try:
                    objTrans.Object = m.Tables[tName]
                except:
                    pass

                try:
                    m.Cultures[trName].ObjectTranslations.Add(objTrans)
                    print(f"'A translation for the '{prop}' in the '{trName}' language for the '{tName}' {oType.lower()} has been added.")
                except:
                    pass
            elif oType == 'Column':                
                try:
                    objTrans.Object = m.Tables[tName].Columns[oName]
                    m.Cultures[trName].ObjectTranslations.Add(objTrans)
                    print(f"'A translation for the '{prop}' in the '{trName}' language for the '{tName}'[{oName}] {oType.lower()} has been added.")
                except:
                    pass
            elif oType == 'Measure':
                try:
                    objTrans.Object = m.Tables[tName].Measures[oName]
                    m.Cultures[trName].ObjectTranslations.Add(objTrans)
                    print(f"'A translation for the '{prop}' in the '{trName}' language for the '{tName}'[{oName}] {oType.lower()} has been added.")
                except:
                    pass
            elif oType == 'Hierarchy':
                try:
                    objTrans.Object = m.Tables[tName].Hierarchies[oName]
                    m.Cultures[trName].ObjectTranslations.Add(objTrans)
                    print(f"'A translation for the '{prop}' in the '{trName}' language for the '{tName}'[{oName}] {oType.lower()} has been added.")
                except:
                    pass
            elif oType == 'Level':
                try:
                    hierName = oName.split("'[")[0][1:]
                    levelName = oName.split("'[")[1][0:-1]
                    objTrans.Object = m.Tables[tName].Hierarchies[hierName].Levels[levelName]
                    m.Cultures[trName].ObjectTranslations.Add(objTrans)
                    print(f"'A translation for the '{prop}' in the '{trName}' language for the '{tName}'[{hierName}].[{levelName}] {oType.lower()} has been added.")
                except:
                    pass

        m.SaveChanges()
        print(f"\nMigration of objects from '{datasetName}' -> '{newDatasetName}' is complete.")