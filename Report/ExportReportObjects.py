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
    reportHeader = {'Report Name': [], 'Page Count': [], 'Custom Visual Count': [], 'Theme Count': [], 'Image Count': []}
    reportDF = pd.DataFrame(reportHeader)

    reportFiltersHeader = {'Filter Name': [], 'Type': [], 'Object Name': [], 'Object Type': [], 'Table Name': [], 'Hidden': [], 'Locked': []}
    reportFiltersDF = pd.DataFrame(reportFiltersHeader)

    pageHeader = {'Page ID': [], 'Page Name': [], 'Hidden': [], 'Page Width': [], 'Page Height': [], 'Display Option': [], 'Visual Count': []}
    pageDF = pd.DataFrame(pageHeader)

    pageFiltersHeader = {'Page ID': [], 'Page Name': [], 'Filter Name': [], 'Type': [], 'Object Name': [], 'Object Type': [], 'Table Name': [], 'Hidden': [], 'Locked': []}
    pageFiltersDF = pd.DataFrame(pageFiltersHeader)

    visualHeader = {'Visual ID': [], 'Page Name': [], 'Title': [], 'Type': [], 'Hidden': [], 'Group': [], 'X': [], 'Y': [], 'Z': [], 'Width': [], 'Height': [], 'Tab Order': [], 'Custom Visual': [], 'Object Count': [], 'Data Visual': []}
    visualDF = pd.DataFrame(visualHeader)

    visualFiltersHeader = {'Visual ID': [], 'Page ID': [], 'Page Name': [], 'Filter Name': [], 'Type': [], 'Object Name': [], 'Object Type': [], 'Table Name': [], 'Hidden': [], 'Locked': []}
    visualFiltersDF = pd.DataFrame(visualFiltersHeader)

    bookmarksHeader = {'Bookmark ID': [], 'Bookmark Name': [], 'Page ID': []}
    bookmarksDF = pd.DataFrame(bookmarksHeader)

    customVisualsHeader = {'Custom Visual Name': []}
    customVisualsDF = pd.DataFrame(customVisualsHeader)

    themesHeader = {'Theme Name': [], 'Theme Path': []}
    themesDF = pd.DataFrame(themesHeader)

    imagesHeader = {'Image Name': [], 'Image Path': []}
    imagesDF = pd.DataFrame(imagesHeader)

    visualObjectsHeader = {'Visual ID': [], 'Data Point Location': [], 'Object Name': [], 'Object Type': [], 'Table Name': [], 'Active': [], 'Sparkline': []}
    visualObjectsDF = pd.DataFrame(visualObjectsHeader)

    # Custom Visuals
    try:
        for customVisual in reportJson['publicCustomVisuals']:
            new_data = {'Custom Visual Name': customVisual}
            customVisualsDF = pd.concat([customVisualsDF, pd.DataFrame(new_data, index=[0])], ignore_index=True)
    except:
        pass

    # Themes and Images
    for rp in reportJson['resourcePackages']:
        rpType = rp['resourcePackage']['type']
        for theme in rp['resourcePackage']['items']:
            themeName = theme['name']
            themePath = theme['path']
            
            if rpType == 2:
                new_data = {'Theme Name': themeName, 'Theme Path': themePath}
                themesDF = pd.concat([themesDF, pd.DataFrame(new_data, index=[0])], ignore_index=True)
            if rpType == 1:
                new_data = {'Image Name': themeName, 'Image Path': themePath}
                imagesDF = pd.concat([imagesDF, pd.DataFrame(new_data, index=[0])], ignore_index=True)    

    # Report
    reportConfig = reportJson['config']
    reportConfigJson = json.loads(reportConfig)
    pageCount = len(reportJson['sections'])
    customVisualCount = len(customVisualsDF)
    themeCount = len(themesDF)
    imageCount = len(imagesDF)
    new_data = {'Report Name': objectName, 'Page Count': pageCount, 'Custom Visual Count': customVisualCount, 'Theme Count': themeCount, 'Image Count': imageCount}
    reportDF = pd.concat([reportDF, pd.DataFrame(new_data, index=[0])], ignore_index=True)
    reportDF['Page Count'] = reportDF['Page Count'].astype(int)
    reportDF['Custom Visual Count'] = reportDF['Custom Visual Count'].astype(int)
    reportDF['Theme Count'] = reportDF['Theme Count'].astype(int)
    reportDF['Image Count'] = reportDF['Image Count'].astype(int)

    # Report Filters
    try:
        reportFilters = reportJson['filters']
        reportFilterJson = json.loads(reportFilters)

        for flt in reportFilterJson:
            filterName = flt['name']
            filterType = flt['type']
            filterLocked = False
            filterHidden = False
            filterObjName = None
            filterObjType = None
            filterTblName = None
            try:
                filterLocked = flt['isLockedInViewMode']
            except:
                pass
            try:
                filterHidden = flt['isHiddenInViewMode']
            except:
                pass
            try:
                filterObjName = flt['expression']['Column']['Property']
                filterObjType = 'Column'
                filterTblName = flt['expression']['Column']['Expression']['SourceRef']['Entity']
            except:
                pass
            try:
                filterObjName = flt['expression']['Measure']['Property']
                filterObjType = 'Measure'
                filterTblName = flt['expression']['Measure']['Expression']['SourceRef']['Entity']
            except:
                pass
            try:
                filterLevel = flt['expression']['HierarchyLevel']['Level']
                filterHierName = flt['expression']['HierarchyLevel']['Expression']['Hierarchy']['Hierarchy']
                filterObjName = filterHierName + "." + filterLevel
                filterObjType = 'Hierarchy'
                filterTblName = flt['expression']['HierarchyLevel']['Expression']['Hierarchy']['Expression']['SourceRef']['Entity']
            except:
                pass

            new_data = {'Filter Name': filterName, 'Type': filterType, 'Object Name': filterObjName, 'Object Type': filterObjType, 'Table Name': filterTblName, 'Hidden': filterHidden, 'Locked': filterLocked}
            reportFiltersDF = pd.concat([reportFiltersDF, pd.DataFrame(new_data, index=[0])], ignore_index=True)
    except:
        pass

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
        try:
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
                try:
                    filterObjName = flt['expression']['Column']['Property']
                    filterObjType = 'Column'
                    filterTblName = flt['expression']['Column']['Expression']['SourceRef']['Entity']
                except:
                    pass
                try:
                    filterObjName = flt['expression']['Measure']['Property']
                    filterObjType = 'Measure'
                    filterTblName = flt['expression']['Measure']['Expression']['SourceRef']['Entity']
                except:
                    pass
                try:
                    filterLevel = flt['expression']['HierarchyLevel']['Level']
                    filterHierName = flt['expression']['HierarchyLevel']['Expression']['Hierarchy']['Hierarchy']
                    filterObjName = filterHierName + "." + filterLevel
                    filterObjType = 'Hierarchy'
                    filterTblName = flt['expression']['HierarchyLevel']['Expression']['Hierarchy']['Expression']['SourceRef']['Entity']
                except:
                    pass
                new_data = {'Page ID': pageID, 'Page Name': pageName, 'Filter Name': filterName, 'Type': filterType, 'Object Name': filterObjName, 'Object Type': filterObjType, 'Table Name': filterTblName, 'Hidden': filterHidden, 'Locked': filterLocked}
                pageFiltersDF = pd.concat([pageFiltersDF, pd.DataFrame(new_data, index=[0])], ignore_index=True)
                pageFiltersDF['Hidden'] = pageFiltersDF['Hidden'].astype(bool)
                pageFiltersDF['Locked'] = pageFiltersDF['Locked'].astype(bool)
        except:
            pass

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
            tabOrder = None
            visualHidden = False
            visualGroup = False
            customVisualFlag = False
            objectCount = 0
            dataVisual = False
            title = None

            try:
                objectCount = len(visualConfigJson['singleVisual']['prototypeQuery']['Select'])
            except:
                pass
            if objectCount > 0:
                dataVisual = True
            try:
                tabOrder = visualConfigJson['layouts'][0]['position']['tabOrder']
            except:
                pass
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

            if visualType in customVisualsDF['Custom Visual Name'].values:
                customVisualFlag = True

            new_data = {'Visual ID': visualID, 'Page Name': pageName, 'Title': title, 'Type': visualType, 'Hidden': visualHidden, 'Group': visualGroup, 'X': visualX, 'Y': visualY, 'Z': visualZ, 'Width': visualWidth, 'Height': visualHeight, 'Tab Order': tabOrder, 'Custom Visual': customVisualFlag, 'Object Count': objectCount, 'Data Visual': dataVisual}
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
                    try:
                        filterObjName = flt['expression']['Column']['Property']
                        filterObjType = 'Column'
                        filterTblName = flt['expression']['Column']['Expression']['SourceRef']['Entity']
                    except:
                        pass
                    try:
                        filterObjName = flt['expression']['Measure']['Property']
                        filterObjType = 'Measure'
                        filterTblName = flt['expression']['Measure']['Expression']['SourceRef']['Entity']
                    except:
                        pass
                    try:
                        filterLevel = flt['expression']['HierarchyLevel']['Level']
                        filterHierName = flt['expression']['HierarchyLevel']['Expression']['Hierarchy']['Hierarchy']
                        filterObjName = filterHierName + "." + filterLevel
                        filterObjType = 'Hierarchy'
                        filterTblName = flt['expression']['HierarchyLevel']['Expression']['Hierarchy']['Expression']['SourceRef']['Entity']
                    except:
                        pass

                    new_data = {'Visual ID': visualID, 'Page ID': pageID, 'Page Name': pageName, 'Filter Name': filterName, 'Type': filterType, 'Object Name': filterObjName, 'Object Type': filterObjType, 'Table Name': filterTblName, 'Hidden': filterHidden, 'Locked': filterLocked}
                    visualFiltersDF = pd.concat([visualFiltersDF, pd.DataFrame(new_data, index=[0])], ignore_index=True)
                    visualFiltersDF['Hidden'] = visualFiltersDF['Hidden'].astype(bool)
                    visualFiltersDF['Locked'] = visualFiltersDF['Locked'].astype(bool)
            except:
                pass

            viz1Header = {'Visual ID': [], 'Data Point Location': [], 'Object': [], 'Active': []}
            viz1DF = pd.DataFrame(viz1Header)

            viz2Header = {'Visual ID': [], 'Table Alias': [], 'Table Name': [], 'Table Type': []}
            viz2DF = pd.DataFrame(viz2Header)

            viz3Header = {'Visual ID': [], 'Object Type': [], 'Object Name': [], 'Table Alias': []}
            viz3DF = pd.DataFrame(viz3Header)            

            # Visual Objects
            try:                
                for objLocation in visualConfigJson['singleVisual']['projections']:
                    for o in visualConfigJson['singleVisual']['projections'][objLocation]:
                        obj = o['queryRef']
                        isActive = False
                        try:
                            isActive = o['active']
                        except:
                            pass

                        new_data = {'Visual ID': visualID, 'Data Point Location': objLocation, 'Object': obj, 'Active': isActive }
                        viz1DF = pd.concat([viz1DF, pd.DataFrame(new_data, index=[0])], ignore_index=True)

                for fromStatement in visualConfigJson['singleVisual']['prototypeQuery']['From']:
                    tblAlias = fromStatement['Name']
                    tblName = fromStatement['Entity']
                    tblType = fromStatement['Type']

                    new_data = {'Visual ID': visualID, 'Table Alias': tblAlias, 'Table Name': tblName, 'Table Type': tblType }
                    viz2DF = pd.concat([viz2DF, pd.DataFrame(new_data, index=[0])], ignore_index=True)
                    
                for o in visualConfigJson['singleVisual']['prototypeQuery']['Select']:
                    isSparkline = False
                    try:                        
                        objName = o['Column']['Property']
                        objType = 'Column'
                        alias = o[objType]['Expression']['SourceRef']['Source']

                        new_data = {'Visual ID': visualID, 'Object Type': objType, 'Object Name': objName, 'Table Alias': alias, 'Sparkline': isSparkline }
                        viz3DF = pd.concat([viz3DF, pd.DataFrame(new_data, index=[0])], ignore_index=True)
                    except:
                        pass
                    try:                       
                        objName = o['Measure']['Property']
                        objType = 'Measure'
                        alias = o[objType]['Expression']['SourceRef']['Source']

                        new_data = {'Visual ID': visualID, 'Object Type': objType, 'Object Name': objName, 'Table Alias': alias, 'Sparkline': isSparkline }
                        viz3DF = pd.concat([viz3DF, pd.DataFrame(new_data, index=[0])], ignore_index=True)
                    except:
                        pass
                    try:                        
                        levelName = o['HierarchyLevel']['Level']
                        hierName = o['HierarchyLevel']['Expresssion']['Hierarchy']['Hierarchy']
                        objName = hierName + "." + levelName
                        alias = o['HierarchyLevel']['Expression']['Hierarchy']['Expression']['SourceRef']['Source']
                        objType = 'Hierarchy'

                        new_data = {'Visual ID': visualID, 'Object Type': objType, 'Object Name': objName, 'Table Alias': alias, 'Sparkline': isSparkline }
                        viz3DF = pd.concat([viz3DF, pd.DataFrame(new_data, index=[0])], ignore_index=True)
                    except:
                        pass
                    try:                       
                        objName = o['Aggregation']['Expression']['Column']['Property']
                        objType = 'Column'
                        alias = o['Aggregation']['Expression']['Column']['Expression']['SourceRef']['Source']
                        
                        new_data = {'Visual ID': visualID, 'Object Type': objType, 'Object Name': objName, 'Table Alias': alias, 'Sparkline': isSparkline }
                        viz3DF = pd.concat([viz3DF, pd.DataFrame(new_data, index=[0])], ignore_index=True)
                    except:
                        pass
                    #Sparklines
                    try:                       
                        objName = o['SparklineData']['Measure']['Measure']['Property']
                        objType = 'Measure'
                        alias = o['SparklineData']['Measure']['Measure']['Expression']['SourceRef']['Source']
                        isSparkline = True

                        new_data = {'Visual ID': visualID, 'Object Type': objType, 'Object Name': objName, 'Table Alias': alias, 'Sparkline': isSparkline }
                        viz3DF = pd.concat([viz3DF, pd.DataFrame(new_data, index=[0])], ignore_index=True)
                    except:
                        pass
                    try:                       
                        objName = o['SparklineData']['Measure']['Aggregation']['Expression']['Column']['Property']
                        objType = 'Column'
                        alias = o['SparklineData']['Measure']['Aggregation']['Expression']['Column']['Expression']['SourceRef']['Source']
                        isSparkline = True

                        new_data = {'Visual ID': visualID, 'Object Type': objType, 'Object Name': objName, 'Table Alias': alias, 'Sparkline': isSparkline }
                        viz3DF = pd.concat([viz3DF, pd.DataFrame(new_data, index=[0])], ignore_index=True)
                    except:
                        pass
                    try:
                        for sp in o['SparklineData']['Groupings']:                            
                            alias = sp['Column']['Expression']['SourceRef']['Source']
                            objName = sp['Column']['Property']
                            objType = 'Column'
                            isSparkline = True

                            new_data = {'Visual ID': visualID, 'Object Type': objType, 'Object Name': objName, 'Table Alias': alias, 'Sparkline': isSparkline }
                            viz3DF = pd.concat([viz3DF, pd.DataFrame(new_data, index=[0])], ignore_index=True)
                    except:
                        pass
                
                viz3DF = pd.merge(viz3DF,viz2DF[['Table Alias', 'Table Name']], on='Table Alias', how='left')
                viz3DF['Object'] = viz3DF['Table Name'] + "." + viz3DF['Object Name']
                viz3DF = pd.merge(viz3DF,viz1DF[['Data Point Location', 'Object', 'Active']], on='Object', how='left')

                visualObjectsDF = pd.concat([visualObjectsDF,viz3DF[['Visual ID', 'Data Point Location', 'Object Name', 'Object Type', 'Table Name', 'Active', 'Sparkline']]], ignore_index=True)

            except:
                pass
    
    # Bookmarks
    try:
        for bookmark in reportConfigJson['bookmarks']:
            bID = bookmark['name']
            bName = bookmark['displayName']
            rptPageId = bookmark['explorationState']['activeSection']

            new_data = {'Bookmark ID': bID, 'Bookmark Name': bName, 'Page ID': rptPageId}
            bookmarksDF = pd.concat([bookmarksDF, pd.DataFrame(new_data, index=[0])], ignore_index=True)

        bookmarksDF = pd.merge(bookmarksDF, pageDF[['Page ID', 'Page Name']], on='Page ID', how='left')
    except:
        pass

    # Add useful columns to DFs
    customVisualsDF['Used in Report'] = customVisualsDF['Custom Visual Name'].isin(visualDF['Type'])
    visualFiltersDF = pd.merge(visualFiltersDF, visualDF[['Visual ID', 'Title']], on='Visual ID', how='left')
    visualFiltersDF.rename(columns={'Title': 'Visual Title'}, inplace=True)
    reportDF['Report Filter Count'] = len(reportFiltersDF)

    filter_counts = pageFiltersDF.groupby('Page ID').size().reset_index(name='Page Filter Count')
    pageDF = pd.merge(pageDF, filter_counts, on='Page ID', how='left')
    pageDF['Page Filter Count'].fillna(0, inplace=True)
    pageDF['Page Filter Count'] = pageDF['Page Filter Count'].astype(int)
    visualDF['Tab Order'] = visualDF['Tab Order'].astype(int)
    visualDF['Custom Visual'] = visualDF['Custom Visual'].astype(bool)
    visualObjectsDF['Active'] = visualObjectsDF['Active'].astype(bool)
    visualObjectsDF['Sparkline'] = visualObjectsDF['Sparkline'].astype(bool)
    reportFiltersDF['Hidden'] = reportFiltersDF['Hidden'].astype(bool)
    reportFiltersDF['Locked'] = reportFiltersDF['Locked'].astype(bool)
    visualDF['Object Count'] = visualDF['Object Count'].astype(int)
    visualDF['Data Visual'] = visualDF['Data Visual'].astype(bool)
    visualDF['Z'] = visualDF['Z'].astype(int)

    print('Report')
    display(reportDF)
    print('Page')
    display(pageDF)
    print('Visuals')
    display(visualDF)
    print('Report Filters')
    display(reportFiltersDF)
    print('Page Filters')
    display(pageFiltersDF)
    print('Visual Filters')
    display(visualFiltersDF)
    print('Bookmarks')
    display(bookmarksDF)
    print('Custom Visuals')
    display(customVisualsDF)    
    print('Themes')
    display(themesDF)
    print('Images')
    display(imagesDF)
    print('Visual Objects')
    display(visualObjectsDF)

export_report_objects("") # Enter Report Name