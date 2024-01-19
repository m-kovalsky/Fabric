import sempy
import sempy.fabric as fabric
import pandas as pd
import base64
import json

def export_report_objects(reportName):

    client = fabric.FabricRestClient()

    workspaceId = fabric.get_workspace_id()
    objectName = reportName
    objectType = "Report"
    itemList = fabric.list_items()
    itemListFilt = itemList[(itemList['Display Name'] == objectName) & (itemList['Type'] == objectType)]
    itemId = itemListFilt['Id'].iloc[0]
    response = client.post(f"/v1/workspaces/{workspaceId}/items/{itemId}/getDefinition")
    df_items = pd.json_normalize(response.json()['definition']['parts'])
    df_items_filt = df_items[df_items['path'] == 'report.json']
    payload = df_items_filt['payload'].iloc[0]

    reportFile = base64.b64decode(payload).decode('utf-8')
    reportJson = json.loads(reportFile)

    # Data frame prep
    reportHeader = {'Report Name': [], 'Page Count': []}
    reportDF = pd.DataFrame(reportHeader)

    reportFiltersHeader = {'Filter Name': [], 'Type': [], 'Hidden': [], 'Locked': []}
    reportFiltersDF = pd.DataFrame(reportFiltersHeader)

    pageHeader = {'Page ID': [], 'Page Name': [], 'Hidden': [], 'Page Width': [], 'Page Height': [], 'Display Option': [], 'Visual Count': []}
    pageDF = pd.DataFrame(pageHeader)

    pageFiltersHeader = {'Page ID': [], 'Page Name': [], 'Filter Name': [], 'Type': [], 'Hidden': [], 'Locked': []}
    pageFiltersDF = pd.DataFrame(pageFiltersHeader)

    visualHeader = {'Visual ID': [], 'Page Name': [], 'Title': [], 'Type': [], 'Hidden': [], 'Group': [], 'X': [], 'Y': [], 'Z': [], 'Width': [], 'Height': []}
    visualDF = pd.DataFrame(visualHeader)

    visualFiltersHeader = {'Visual ID': [], 'Page ID': [], 'Page Name': [], 'Filter Name': [], 'Type': [], 'Hidden': [], 'Locked': []}
    visualFiltersDF = pd.DataFrame(visualFiltersHeader)

    # Report
    pageCount = len(reportJson['sections'])
    new_data = {'Report Name': objectName, 'Page Count': pageCount}
    reportDF = pd.concat([reportDF, pd.DataFrame(new_data, index=[0])], ignore_index=True) 

    # Report Filters
    reportFilters = reportJson['filters']
    reportFilterJson = json.loads(reportFilters)

    for flt in reportFilterJson:
        filterName = flt['name']
        filterType = flt['type']
        filterLocked = False
        filterHidden = False
        try:
            filterLocked = flt['isLockedInViewMode']
        except:
            pass
        try:
            filterHidden = flt['isHiddenInViewMode']
        except:
            pass
        new_data = {'Filter Name': filterName, 'Type': filterType, 'Hidden': filterHidden, 'Locked': filterLocked}
        reportFiltersDF = pd.concat([reportFiltersDF, pd.DataFrame(new_data, index=[0])], ignore_index=True)
        reportFiltersDF['Hidden'] = reportFiltersDF['Hidden'].astype(bool)
        reportFiltersDF['Locked'] = reportFiltersDF['Locked'].astype(bool)

    # Pages
    for section in reportJson['sections']:
        pageID = section['name']
        pageName = section['displayName']
        pageFilters = section['filters']
        pageWidth = section['width']
        pageHeight = section['height']
        visualCount = len(section['visualContainers'])
        pageHidden = False
        pageType = None
        pageConfig = section['config']
        pageConfigJson = json.loads(pageConfig)
        pageNumber = 0
        displayOption = section['displayOption']
        pageHidden = False

        try:
            pageNumber = section['ordinal']
        except:
            pass
        try:
            pageH = pageConfigJson['visibility']
            if pageH == 1:
                pageHidden = True
        except:
            pass
        
        new_data = {'Page ID': pageID, 'Page Name': pageName, 'Hidden': pageHidden, 'Page Width': pageWidth, 'Page Height': pageHeight, 'Display Option': displayOption, 'Visual Count': visualCount}
        pageDF = pd.concat([pageDF, pd.DataFrame(new_data, index=[0])], ignore_index=True)
        pageDF['Hidden'] = pageDF['Hidden'].astype(bool)
        pageDF['Page Width'] = pageDF['Page Width'].astype(int)
        pageDF['Page Height'] = pageDF['Page Height'].astype(int)
        pageDF['Display Option'] = pageDF['Display Option'].astype(int)
        pageDF['Visual Count'] = pageDF['Visual Count'].astype(int)

        # Page Filters
        pageFiltersJson = json.loads(pageFilters)

        for flt in pageFiltersJson:
            filterName = flt['name']
            filterType = flt['type']
            filterLocked = False
            filterHidden = False
            try:
                filterLocked = flt['isLockedInViewMode']
            except:
                pass
            try:
                filterHidden = flt['isHiddenInViewMode']
            except:
                pass
            new_data = {'Page ID': pageID, 'Page Name': pageName, 'Filter Name': filterName, 'Type': filterType, 'Hidden': filterHidden, 'Locked': filterLocked}
            pageFiltersDF = pd.concat([pageFiltersDF, pd.DataFrame(new_data, index=[0])], ignore_index=True)
            pageFiltersDF['Hidden'] = pageFiltersDF['Hidden'].astype(bool)
            pageFiltersDF['Locked'] = pageFiltersDF['Locked'].astype(bool)

        # Visuals
        for visual in section['visualContainers']:
            visualConfig = visual['config']
            visualConfigJson = json.loads(visualConfig)        
            visualID = visualConfigJson['name']
            #print(visualConfigJson)
            visualType = "Unknown"
            visualX = visual['x']
            visualY = visual['y']
            visualZ = visual['z']
            visualWidth = visual['width']
            visualHeight = visual['height']
            visualHidden = False
            visualGroup = False
            
            try:
                visualType = visualConfigJson['singleVisual']['visualType']
            except:
                visualType = "Group"
                visualGroup = True
            try:
                vH = visualConfigJson['singleVisual']['display']['mode']
                if vH == "hidden":
                    visualHidden = True
            except:
                pass 
            try:
                visualHidden = visualConfigJson['singleVisualGroup']['isHidden']
            except:
                pass
            try:
                title = visualConfigJson["singleVisual"]["vcObjects"]["title"][0]["properties"]["text"]["expr"]["Literal"]["Value"]
                title = title[1:-1]
            except:
                pass

            new_data = {'Visual ID': visualID, 'Page Name': pageName, 'Title': title, 'Type': visualType, 'Hidden': visualHidden, 'Group': visualGroup, 'X': visualX, 'Y': visualY, 'Z': visualZ, 'Width': visualWidth, 'Height': visualHeight}
            visualDF = pd.concat([visualDF, pd.DataFrame(new_data, index=[0])], ignore_index=True)

            visualDF['Hidden'] = visualDF['Hidden'].astype(bool)
            visualDF['Group'] = visualDF['Group'].astype(bool)

            # Visual Filters
            try:
                visualFilters = visual['filters']
                visualFiltersJson = json.loads(visualFilters)

                for flt in visualFiltersJson:
                    filterName = flt['name']
                    filterType = flt['type']
                    filterLocked = False
                    filterHidden = False
                    try:
                        filterLocked = flt['isLockedInViewMode']
                    except:
                        pass
                    try:
                        filterHidden = flt['isHiddenInViewMode']
                    except:
                        pass
                    new_data = {'Visual ID': visualID, 'Page ID': pageID, 'Page Name': pageName, 'Filter Name': filterName, 'Type': filterType, 'Hidden': filterHidden, 'Locked': filterLocked}
                    visualFiltersDF = pd.concat([visualFiltersDF, pd.DataFrame(new_data, index=[0])], ignore_index=True)
                    visualFiltersDF['Hidden'] = visualFiltersDF['Hidden'].astype(bool)
                    visualFiltersDF['Locked'] = visualFiltersDF['Locked'].astype(bool)
            except:
                pass

    print('Report')
    display(reportDF)
    print('Page')
    display(pageDF)
    print('Visual')
    display(visualDF)
    print('Report Filters')
    display(reportFiltersDF)
    print('Page Filters')
    display(pageFiltersDF)
    print('Visual Filters')
    display(visualFiltersDF)

export_report_objects("") # Enter Report Name