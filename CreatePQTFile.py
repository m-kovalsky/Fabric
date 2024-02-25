import sempy
import sempy.fabric as fabric
import pandas as pd
import json
import os
import xml.etree.ElementTree as ET
import shutil
from sempy.fabric._client import DatasetXmlaClient
from sempy.fabric._cache import _get_or_create_workspace_client
sempy.fabric._client._utils._init_analysis_services()

def create_pqt_file(datasetName, fileName = 'PowerQueryTemplate'):

    fileName = 'PowerQueryTemplate'
    folderPath = '/lakehouse/default/Files'
    subFolderPath = os.path.join(folderPath, 'pqtnewfolder')
    os.makedirs(subFolderPath, exist_ok=True)

    def list_tables(datasetName, workspaceName = None):

        if workspaceName == None:
            workspaceId = fabric.get_workspace_id()
            workspaceName = fabric.resolve_workspace_name(workspaceId)

        workspace_client = _get_or_create_workspace_client(workspaceName)
        ds = workspace_client.get_dataset(datasetName)
        m = ds.Model

        header = pd.DataFrame(columns=['Name', 'Refresh Policy', 'Source Expression'])
        df = pd.DataFrame(header)

        for t in m.Tables:
            rPolicy = bool(t.RefreshPolicy)
            sourceExpression = None

            if rPolicy:
                sourceExpression = t.RefreshPolicy.SourceExpression

            new_data = {'Name': t.Name, 'Refresh Policy': rPolicy, 'Source Expression': sourceExpression}
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

        return df

    dfP = fabric.list_partitions(datasetName)
    dfT = list_tables(datasetName)
    dfE = fabric.list_expressions(datasetName)

    # Check if M-partitions are used
    if any(dfP['Source Type'] == 'M'):
        class QueryMetadata:
            def __init__(self, QueryName, QueryGroupId=None, LastKnownIsParameter=None, LastKnownResultTypeName=None, LoadEnabled=True, IsHidden=False):
                self.QueryName = QueryName
                self.QueryGroupId = QueryGroupId
                self.LastKnownIsParameter = LastKnownIsParameter
                self.LastKnownResultTypeName = LastKnownResultTypeName
                self.LoadEnabled = LoadEnabled
                self.IsHidden = IsHidden

        class RootObject:
            def __init__(self, DocumentLocale, EngineVersion, QueriesMetadata, QueryGroups=None):
                if QueryGroups is None:
                    QueryGroups = []
                self.DocumentLocale = DocumentLocale
                self.EngineVersion = EngineVersion
                self.QueriesMetadata = QueriesMetadata
                self.QueryGroups = QueryGroups

        # STEP 1: Create MashupDocument.pq
        mdfileName = 'MashupDocument.pq'
        mdFilePath = os.path.join(subFolderPath, mdfileName)
        sb = 'section Section1;'
        for table_name in dfP['Table Name'].unique():
            tName = '#\"' + table_name + '"'
            sb = sb + '\n' + 'shared ' + tName + ' = '
            sourceExpression = dfT.loc[(dfT['Name'] == table_name), 'Source Expression'].iloc[0]
            refreshPolicy = dfT.loc[(dfT['Name'] == table_name), 'Refresh Policy'].iloc[0]

            if refreshPolicy == True:
                sb = sb + sourceExpression + ';'

            partitions_in_table = dfP.loc[dfP['Table Name'] == table_name, 'Partition Name'].unique()

            i=1
            for partition_name in partitions_in_table:
                pSourceType = dfP.loc[(dfP['Table Name'] == table_name) & (dfP['Partition Name'] == partition_name), 'Source Type'].iloc[0]
                if refreshPolicy == False and pSourceType == 'M' and i==1:
                    pQuery = dfP.loc[(dfP['Table Name'] == table_name) & (dfP['Partition Name'] == partition_name), 'Query'].iloc[0]
                    sb = sb + pQuery + ';'
                i+=1

        for index, row in dfE.iterrows():
            expr = row['Expression']
            eName = row['Name']
            eName = '#"' + eName + '"'
            sb = sb + '\n' + "shared " + eName + " = " + expr + ";"

        with open(mdFilePath, 'w') as file:
            file.write(sb)

        # STEP 2: Create the MashupMetadata.json file
        mmfileName = 'MashupMetadata.json'
        mmFilePath = os.path.join(subFolderPath, mmfileName)
        queryMetadata = []

        for i, r in dfT.iterrows():
            tName = r['Name']
            queryMetadata.append(QueryMetadata(tName, None, None, None, True, False))

        for i, r in dfE.iterrows():
            eName = r['Name']
            eKind = r['Kind']
            if eKind == 'M':
                queryMetadata.append(QueryMetadata(eName, None, None, None, True, False))
            else:
                queryMetadata.append(QueryMetadata(eName, None, None, None, False, False))

        rootObject = RootObject("en-US", "2.126.453.0", queryMetadata)

        def obj_to_dict(obj):
            if isinstance(obj, list):
                return [obj_to_dict(e) for e in obj]
            elif hasattr(obj, "__dict__"):
                return {k: obj_to_dict(v) for k, v in obj.__dict__.items()}
            else:
                return obj
        jsonContent = json.dumps(obj_to_dict(rootObject), indent=4)

        with open(mmFilePath, 'w') as json_file:
            json_file.write(jsonContent)

        # STEP 3: Create Metadata.json file
        mFileName = 'Metadata.json'
        mFilePath = os.path.join(subFolderPath, mFileName)
        metaData = {"Name": "fileName", "Description": "", "Version": "1.0.0.0"}
        jsonContent = json.dumps(metaData, indent=4)

        with open(mFilePath, 'w') as json_file:
            json_file.write(jsonContent)

        # STEP 4: Create [Content_Types].xml file:
        ns = 'http://schemas.openxmlformats.org/package/2006/content-types'
        ET.register_namespace('', ns) 
        types = ET.Element("{%s}Types" % ns)
        default1 = ET.SubElement(types, "{%s}Default" % ns, {"Extension": "json", "ContentType": "application/json"})
        default2 = ET.SubElement(types, "{%s}Default" % ns, {"Extension": "pq", "ContentType": "application/x-ms-m"})
        xmlDocument = ET.ElementTree(types)
        xmlFileName = '[Content_Types].xml'
        xmlFilePath = os.path.join(subFolderPath, xmlFileName)
        xmlDocument.write(xmlFilePath, xml_declaration=True, encoding='utf-8', method='xml')

        # STEP 5: Zip up the 4 files
        zipFileName = fileName + '.zip'
        zipFilePath = os.path.join(folderPath, zipFileName)
        shutil.make_archive(zipFilePath[:-4], 'zip', subFolderPath)

        # STEP 6: Convert the zip file back into a .pqt file
        newExt = '.pqt'
        directory = os.path.dirname(zipFilePath)
        fileNameWithoutExtension = os.path.splitext(os.path.basename(zipFilePath))[0]
        newFilePath = os.path.join(directory, fileNameWithoutExtension + newExt)
        shutil.move(zipFilePath, newFilePath)

        #STEP 7: Delete subFolder directory which is no longer needed
        shutil.rmtree(subFolderPath, ignore_errors=True)

        print(f"'{fileName}.pqt' has been created based on the '{datasetName}' semantic model within the Files section of your lakehouse.")

    else:
        print(f"The '{datasetName}' semantic model does not use Power Query so a Power Query Template file cannot be generated.")

create_pqt_file('IncrementalRefreshTest') #Enter semantic model name