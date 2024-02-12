import sempy
import sempy.fabric as fabric

def clear_cache(datasetName):

    x = fabric.list_datasets()
    x = x[x['Dataset Name'] == datasetName]
    rowCount = len(x)
    if rowCount == 1:
        datasetID = x["Dataset ID"].values[0]
        xmla = f"""
                <ClearCache xmlns="http://schemas.microsoft.com/analysisservices/2003/engine">  
                    <Object>  
                        <DatabaseID>{datasetID}</DatabaseID>  
                    </Object>  
                </ClearCache>
                """
        fabric.execute_xmla(datasetName,xmla_command=xmla)

        outputtext = f"Cache cleared for dataset '{datasetName}'."

    else:
        outputtext = f"Dataset '{datasetName}' does not exist in this workspace."
    return outputtext

clear_cache('') #nter dataset name
