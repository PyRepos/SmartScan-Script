'''
    File name: SmartScanParse.py
    Author: Hao Ye
    Date created: 1/10/2018
    Function: Process SMARTscan GPS data using Arcpy in ESRI geodatabase
    Python Version: 2.7
'''

import arcpy
import os
import gc
import csv
import pandas as pd

# Configure the geodatabase file path
SMARTSCAN_PATH_CONFIG = {
    'workspace' : 'C:\1-CarnellDeployment\SMARTscan\Data\SourceFolder',
    'geodatabase' : 'C:\1-CarnellDeployment\SMARTscan\DeveloperOnly\DRAINAGE_REPORTS.mdb', # empty sample geodatabase,
    'ssTrace' : 'C:\1-CarnellDeployment\SMARTscan\Data\SourceFolder\ssTrace.shp',
    'ProjectionFile' : 'C:\1-CarnellDeployment\SMARTscan\DeveloperOnly\CoordinateSystems\BritishNationalGrid.prj',
    'startEndtemp' : 'C:/1-CarnellDeployment/SMARTscan/Data/SourceFolder/StartEndTemp.shp'
    }

# Configure file configurations
FILE_CONFIG={
    'VBOX' : 'vbox.csv',
    'GISFOLDER':'1-GIS',
    'VBOXFOLDER' : '5-VBOX',
    'GIS' : '1-GIS',
    'GEODATABASE' : 'DRAINAGE_REPORTS.mdb',
    'SMLINES' : 'Sm_Lines',
    'Coordinatesystem' : 'C:/1-CarnellDeployment/SMARTscan/DeveloperOnly/CoordinateSystems/BritishNationalGrid.prj'
    }

# Configure the geodatabase data structure
GEODATABASE_GEODATABASE_CONFIG = {
    'StartEN' : '\StartEN',
    'EndEN' : '\EndEN',
    'StartEnd' : '\OSR_POINTS',
    'SsReport' : '\SMARTscan_report',
    'smPointLyName' : '\Sm_PointsXY',
    'smLineLyName' : '\smLineLyName'
    }

# Setting up workspace 
#arcpy.env.workspace = str(sys.argv[1]) 
arcpy.env.workspace = "C:/1-CarnellDeployment/SMARTscan/Data/SourceFolder/88608-M11-11-Hao" # FOR TESTING ONLY

# Setting working environment
smFileName = FILE_CONFIG['VBOX']
smLineLyName = FILE_CONFIG['SMLINES']
gdbDatabasePath = os.path.join(arcpy.env.workspace, FILE_CONFIG['GEODATABASE'])
smLinePath = os.path.join(arcpy.env.workspace,smLineLyName).replace("\\",'/')
smFilePath = os.path.join(arcpy.env.workspace, FILE_CONFIG['VBOXFOLDER'], smFileName).replace("\\",'/')
startEndtemp = SMARTSCAN_PATH_CONFIG['startEndtemp']

# Setting up geodatabase template for script use
ssTrace = SMARTSCAN_PATH_CONFIG['ssTrace']
startEN = gdbDatabasePath + GEODATABASE_GEODATABASE_CONFIG['StartEN']
endEN = gdbDatabasePath + GEODATABASE_GEODATABASE_CONFIG['EndEN']
startEnd = gdbDatabasePath + GEODATABASE_GEODATABASE_CONFIG['StartEnd']
ssReport = gdbDatabasePath + GEODATABASE_GEODATABASE_CONFIG['SsReport']
ssPointXY = gdbDatabasePath + GEODATABASE_GEODATABASE_CONFIG['smPointLyName']
ssLines = gdbDatabasePath + GEODATABASE_GEODATABASE_CONFIG['smLineLyName']

# Setting up input field variables for layer
smlatField = "Latitude"
smlonField = "Longitude"
smlineField = "Scan_num"  
smPointLyName = "Sm_PointXY"

# Setting up coordinate Systems
ukCoordSystem = FILE_CONFIG['Coordinatesystem']

print "\nInfo: Environment configuration is completed"

# Check if all database is clean and configure database
if arcpy.Exists(startEN) == True:
    arcpy.Delete_management(startEN)

if arcpy.Exists(endEN) == True:
    arcpy.Delete_management(endEN)

if arcpy.Exists(ssPointXY) == True:
    arcpy.Delete_management(ssPointXY)

if arcpy.Exists(ssLines) == True:
    arcpy.Delete_management(ssLines)

if arcpy.Exists(ssTrace) == True:
    arcpy.Delete_management(ssTrace)   

if arcpy.Exists(startEndtemp ) == True:
    arcpy.Delete_management(startEndtemp ) 

print "\nInfo: Data container is cleaned\n"

# Get geospatial geometries from Latitude and Longitude in data file, exported as point layer
arcpy.MakeXYEventLayer_management(smFilePath,smlatField,smlonField,smPointLyName,ukCoordSystem)

# Define the produced SMARTscan Input Features 
smInputPoint = os.path.join(gdbDatabasePath,smPointLyName)
smOutputLine = os.path.join(gdbDatabasePath,smLineLyName)

# Get line layers and start & end node coordinate
arcpy.FeatureClassToShapefile_conversion(smPointLyName,gdbDatabasePath)
arcpy.PointsToLine_management(smInputPoint, smOutputLine, smlineField)

print "\nInfo: SMARTscan geospatial points and lines are produced"

# Get start node and end note layer
arcpy.AddGeometryAttributes_management(smOutputLine,"LINE_START_MID_END")
startLayer = arcpy.MakeXYEventLayer_management(smOutputLine,"START_X","START_Y","startLayer",ukCoordSystem)
endLayer = arcpy.MakeXYEventLayer_management(smOutputLine,"END_X","END_Y","endLayer",ukCoordSystem)

# Convert layer into geodatabase datafile
arcpy.FeatureClassToFeatureClass_conversion(startLayer,gdbDatabasePath,"StartEN")
arcpy.FeatureClassToFeatureClass_conversion(endLayer,gdbDatabasePath,"EndEN")

# Process Pipe ref and then new Scan_Num values for start node and end nodes
with arcpy.da.UpdateCursor(startEN,["Scan_Num"]) as cursor:
    for row in cursor:
        row[0] = row[0] + "_1"
        cursor.updateRow(row)

with arcpy.da.UpdateCursor(endEN,["Scan_Num"]) as cursor:
    for row in cursor:
        row[0] = row[0] + "_2"
        cursor.updateRow(row)

del row
del cursor

print "\nInfo: Scan number fields are modified for following HADDMS"

## Create FieldMappings object to manage merge output fields
fieldMappingsStart = arcpy.FieldMappings()

## Add the target table to the field mappings class to set the schema
fieldMappingsStart.addTable(startEN)

## Add input fields for the startEN  field that matches the target dataset
fldMap_Ref = arcpy.FieldMap()
fldMap_Ref.addInputField(startEN,"Scan_Num")
suppRef = fldMap_Ref.outputField
suppRef.name = "SUPP_REF"
fldMap_Ref.outputField = suppRef

# Add output field to field mappings object
fieldMappingsStart.addFieldMap(fldMap_Ref)
fldMap_StartX = arcpy.FieldMap()
fldMap_StartX.addInputField(startEN,"START_X")
startX = fldMap_StartX.outputField
startX.name = "EASTING"
fldMap_StartX.outputField = startX

# Add output field to field mappings object
fieldMappingsStart.addFieldMap(fldMap_StartX)
fldMap_StartY = arcpy.FieldMap()
fldMap_StartY.addInputField(startEN,"START_Y")
startY = fldMap_StartY.outputField
startY.name = "NORTHING"
fldMap_StartY.outputField = startY

# Add output field to field mappings object
fieldMappingsStart.addFieldMap(fldMap_StartY)
fieldMappingsEnd = arcpy.FieldMappings()

# Add the target table to the field mappings class to set the schema
fieldMappingsEnd.addTable(endEN)

# Add input fields for the startEN  field that matches the target dataset
# since each input dataset has a different field name for this info
fldMap_Ref = arcpy.FieldMap()
fldMap_Ref.addInputField(endEN,"Scan_Num")
suppRef = fldMap_Ref.outputField
suppRef.name = "SUPP_REF"
fldMap_Ref.outputField = suppRef

# Add output field to field mappings object
fieldMappingsEnd.addFieldMap(fldMap_Ref)
fieldMappingsStart.addFieldMap(fldMap_Ref)
fldMap_EndX = arcpy.FieldMap()
fldMap_EndX.addInputField(endEN,"END_X")
endX = fldMap_EndX.outputField
endX.name = "EASTING"
fldMap_EndX.outputField = endX

# Add output field to field mappings object
fieldMappingsEnd.addFieldMap(fldMap_EndX)
fldMap_EndY = arcpy.FieldMap()
fldMap_EndY.addInputField(endEN,"END_Y")
endY = fldMap_EndY.outputField
endY.name = "NORTHING"
fldMap_EndY.outputField = endY

# Add output field to field mappings object
fieldMappingsEnd.addFieldMap(fldMap_EndY)

print "\nInfo: Field mappings are successfully added"

## Clear out nodes fc and append both start and End Positions then apply Item type GN to all items
arcpy.TruncateTable_management(startEnd)
arcpy.Append_management(startEN,startEnd,"NO_TEST",fieldMappingsStart)
arcpy.Append_management(endEN,startEnd,"NO_TEST",fieldMappingsEnd)

with arcpy.da.UpdateCursor(startEnd,["ITEM_TY_CO","SUPP_SCH","COVER_DUTY","SYSTEM_TYP","Unique_Ref","SUPP_REF"]) as cursor:
    for row in cursor:
        row[0] = "GN"
        #row[1] = contract
        row[2] = "OT"
        row[3] = "SW"
        #row[4] = row[5] + "_" + contract
        row[4] = row[5] + "_"
        cursor.updateRow(row)

# Add .X to Scan Number field smOutputLine
with arcpy.da.UpdateCursor(smOutputLine,["Scan_Num"]) as cursor:
    for row in cursor:
        row[0] = row[0] + ".X"
        cursor.updateRow(row)

print "\nInfo: Data Append Management processing is completed"

# Create FieldMappings object to manage merge output fields
fieldMappings = arcpy.FieldMappings()

# Add the target table to the field mappings class to set the schema
#fieldMappings.addTable(ssTrace)
fieldMappings.addTable(smOutputLine)

print "\nInfo: Field mappings are added to the table"

# Add input fields for the ssTrace  field that matches the target dataset
# since each input dataset has a different field name for this info
fldMap_Ref = arcpy.FieldMap()
fldMap_Ref.addInputField(smOutputLine,"Scan_Num")
suppRef = fldMap_Ref.outputField
suppRef.name = "SUPP_REF"
fldMap_Ref.outputField = suppRef

# Add output field to field mappings object
fieldMappings.addFieldMap(fldMap_Ref)

print "\nInfo: start to truncate the table, please wait!" 

# Append smartscan trace to smartscan report
arcpy.TruncateTable_management(ssReport)
arcpy.Append_management(smOutputLine,ssReport,"NO_TEST",fieldMappings)

# Apply HADDMS mandatory values
with arcpy.da.UpdateCursor(ssReport,["SUPP_REF","SUPP_REFUP","SUPP_REFDN","CERT_CONN","FLOW_DIR","ITEM_TY_CO","Scan_Number","SS","SUPP_SCH","Area","Unique_Ref","STRU_GRADE"]) as cursor:
    for row in cursor:
        row[1] = row[0][:-2] + "_1"
        row[2] = row[0][:-2] + "_2"
        row[3] = "N"
        row[4] = "N"
        row[5] = "FD"
        row[6] = row[0][:-2]
        row[7] = 1
        #row[8] = contract
        #row[9] = area
        row[10] = row[0] + "_"
        row[11] = 0
        cursor.updateRow(row)
# Validate SMARTscan Attribute information
templateHeader = ["Scan Date","Scan Number","Scan Link (office)"," Condition Score Graph Link","Overall Condition Score for Drain", "Road","Direction","PSD Link  (office)",
            "Height (inches)","Gain","Distance From edge of C/W (mm)", "Survey Distance (m)","Filter Drain Length (m)", "Filter Drain Visible (Yes/No)","Scheme Reference",
            "Start Ref","End Ref","Non-FD (m)","MP Start","MP End","GPS Start X","GPS Start Y","GPS End X","GPS End Y","Photo_One","Photo_Two","Photo_Three",
            "Video","Comments\n"]

print "\nInfo: Start to validate the attribute tables"

dir_path = os.path.abspath(__file__)
dirParent = os.path.dirname(os.path.dirname(os.path.dirname(dir_path)))
dir = os.path.normpath(dirParent) 

files = []
files = os.listdir(dir)
projfound = False
for item in files:
    if ".csv" in item and "VBOX" not in item and "CSV_Out_modified" not in item:
        print "Project csv is found!"
        projfound = True
        objectCSV = dir + "\\" + item
    else:
        projfound = False

rownum = 1
origData = []

print"\nInfo: Start to populate the attribute to a new csv file"

# convert the excel file active sheet to csv
excelName = "SMARTscan M11 J6 - J9 Consolidated Information.xlsm"
objectExl = os.path.join(arcpy.env.workspace,FILE_CONFIG['GISFOLDER'],excelName).replace("\\",'/')
df = pd.read_excel(objectExl, sheetname='Info').to_csv('newCSV.csv',encoding='utf-8')

#for value in origData:
#    if value not in templateHeader:
#        print "Warning"

# Populate SMARTscan Attribute information


#newCsv="C:\Carell Source\repos\Projects\SMARTscan DataProcessor\SMARTscan-DataProcessor\SmscanScript\newCSV.csv"
#newcsv = os.path.join(arcpy.env.Workspace, FILE_CONFIG['GIS'])
newCsv="newCsv.csv"
        
ssReportLayer = arcpy.MakeFeatureLayer_management(ssReport,"ssReportLayer")
arcpy.AddJoin_management(ssReportLayer,"Scan_Number",newCsv,"Scan Number")
arcpy.CalculateField_management(ssReportLayer,"SMARTscan_Report.Scan_Date","[newCSV.csv.Scan Date]","VB")
arcpy.CalculateField_management(ssReportLayer,"SMARTscan_Report.Scan_Link__Office_","[newCSV.csv.Scan Link (office)]","VB")
arcpy.CalculateField_management(ssReportLayer,"SMARTscan_Report.Condition_Score_Graph_Link","[newCSV.csv.Condition Score Graph Link]","VB")
#arcpy.CalculateField_management(ssReportLayer,"SMARTscan_Report.Overall_Condition_Score_for_Drain","[CSV_Out_modified.csv.Overall Condition Score for Drain]","VB")
#arcpy.CalculateField_management(ssReportLayer,"SMARTscan_Report.SERV_GRADE","[CSV_Out_modified.csv.Overall Condition Score for Drain]","VB")
#arcpy.CalculateField_management(ssReportLayer,"SMARTscan_Report.Road","[CSV_Out_modified.csv.Road]","VB")
#arcpy.CalculateField_management(ssReportLayer,"SMARTscan_Report.LOC_ST","[CSV_Out_modified.csv.Road]","VB")
arcpy.CalculateField_management(ssReportLayer,"SMARTscan_Report.Photo_One","[newCSV.csv.Photo\nOne]","VB")
#arcpy.CalculateField_management(ssReportLayer,"SMARTscan_Report.Photo_Two","[CSV_Out_modified.csv.Photo\nTwo]","VB")
#arcpy.CalculateField_management(ssReportLayer,"SMARTscan_Report.Photo_Three","[CSV_Out_modified.csv.Photo\nThree]","VB")

print "\nInfo: Field calculation is completed"

arcpy.RemoveJoin_management(ssReportLayer)

# Recalculate SMARTscan Extents
arcpy.RecalculateFeatureClassExtent_management(ssReport)

print "\nInfo: Recalculation for feature class extension is completed"

# Remove intermediate layers
if arcpy.Exists(startEndtemp) == True:
    arcpy.Delete_management(startEndtemp)

if arcpy.Exists(ssTrace) == True:
    arcpy.Delete_management(ssTrace)

del ssReportLayer
del startEnd
del startEN
del endEN
del arcpy

print("\nInfo: GIS Data succesfully appended to DRAINAGE_REPORTS.mdb")
print("\n")

       
