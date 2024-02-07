import sempy
import sempy.fabric as fabric
from sempy.fabric._cache import _get_or_create_workspace_client
import pandas as pd

def list_perspectives(datasetName, workspaceName = None, include_objects = False):

    workspace_client = _get_or_create_workspace_client(workspaceName)
    ds = workspace_client.get_dataset(datasetName)
    m = ds.Model
   
    d = fabric.list_datasets()
    filter_condition = [datasetName]
    d = d[d['Dataset Name'].isin(filter_condition)]
    datasetID = d['Dataset ID'].values

    if workspaceName == None:
        workspaceID = fabric.get_workspace_id()
        workspaceName = fabric.resolve_workspace_name(workspaceID)
    else:
        workspaceID = fabric.resolve_workspace_id(workspaceName)

    if include_objects == True:
        header = {'Perspective Name': [], 'Table Name': [], 'Object Name': [], 'Object Type': [], 'Dataset Name': [], 'Dataset ID': [], 'Workspace Name': [], 'Workspace ID': []}
    else:
        header = {'Perspective Name': [], 'Dataset Name': [], 'Dataset ID': [], 'Workspace Name': [], 'Workspace ID': []}
    df = pd.DataFrame(header)

    if include_objects == True:
        for p in m.Perspectives:
            for pt in p.PerspectiveTables:
                for pc in pt.PerspectiveColumns:
                    objectType = "Column"
                    new_data = {'Perspective Name': p.Name, 'Table Name': pt.Name, 'Object Name': pc.Name, 'Object Type': objectType, 'Dataset Name': datasetName, 'Dataset ID': datasetID, 'Workspace Name': workspaceName, 'Workspace ID': workspaceID}
                    df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
                for pm in pt.PerspectiveMeasures:
                    objectType = "Measure"
                    new_data = {'Perspective Name': p.Name, 'Table Name': pt.Name, 'Object Name': pm.Name, 'Object Type': objectType, 'Dataset Name': datasetName, 'Dataset ID': datasetID, 'Workspace Name': workspaceName, 'Workspace ID': workspaceID}
                    df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
                for ph in pt.PerspectiveHierarchies:
                    objectType = "Hierarchy"
                    new_data = {'Perspective Name': p.Name, 'Table Name': pt.Name, 'Object Name': ph.Name, 'Object Type': objectType, 'Dataset Name': datasetName, 'Dataset ID': datasetID, 'Workspace Name': workspaceName, 'Workspace ID': workspaceID}
                    df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
    else:
        for p in m.Perspectives:
            new_data = {'Perspective Name': p.Name, 'Dataset Name': datasetName, 'Dataset ID': datasetID, 'Workspace Name': workspaceName, 'Workspace ID': workspaceID}
            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

    return df