import pandas as pd
import numpy as np
from json import loads
import json

TYPE_LIST = ["String", "List", "Number", "bool"]
YES = ["Yes", "yes", "Y", "y"]
CAP_LIST = ["lower", "upper", "keep"]
ROW_OFFSET = 2
# Parameters use for verify csv records data
CONFIG_PARAMETERS = ["key_name", "record_unit", "type", "min", "max", "cap", "category", "blank_accepted"]

## export
## csvFileReaderOutput
## "baseUnitFileError", "configFileError" and "recordFileError" is in DataFrame format, easlier to convert to csv file,
## "recordOutput" is in list format
def csvFileReader(baseUnitFile, cfgFile, recFile):
  cfgFileResult = cfgFileInfo(baseUnitFile, cfgFile, recFile)
  cfgDF = cfgFileResult["configDF"]
  cfgDict = cfgFileResult["configDict"]
  cfgErr = cfgFileResult["errorCfg"]
  baseUnitErr = cfgFileResult["errorBaseUnit"]
  # configuration file error status
  if cfgErr.shape[0] == 0 and baseUnitErr.shape[0] == 0:
    cfgBaseUnitFileErr = False
  else:
    cfgBaseUnitFileErr = True
  # column names
  columnNames = cfgDF.index.to_list()
  # Mapping the csv column name and key_name to columnNameMap dictionary,
  # to be use for renaming the csv file column output
  keys = cfgDF.index
  values = cfgDF['key_name']
  columnNameMap = {}
  for key, value in zip(keys, values, strict = True):
    columnNameMap[key] = value
  # Do not process csv file if the configuration file has error
  if cfgBaseUnitFileErr:
    errorDF = pd.DataFrame()
    parsed = []
  else:
    # Read CSV file and select relevant columns
    recordDF = pd.read_csv(recFile, sep = cfgDict["csv_separator"])
    recordDF = recordDF[columnNames]
    #error check
    errorDF = errorCheck(cfgDF, recordDF, columnNames)
    # identify error records index
    errorRecords = errorDF.apply(lambda x: x["row"] - ROW_OFFSET, axis=1).unique().tolist()
    recordDF = recordDF.drop(index = errorRecords, axis= 0)
    # Clean csv records
    for columnName in columnNames:
      if (cfgDF.loc[columnName,"type"] == "String"):
        recordDF = trimClean(cfgDF, recordDF, columnName)
      elif (cfgDF.loc[columnName,"type"] == "List"):
        recordDF = trimClean(cfgDF, recordDF, columnName)
      elif (cfgDF.loc[columnName, "type"] == "Number"):
        recordDF = numberClean(recordDF, columnName)
    # rename column to key_name
    recordDF.rename(columns = columnNameMap, inplace = True)
    recordDFJSON = recordDF.to_json(orient = "records")
    parsed = loads(recordDFJSON) # make json more readable
  csvFileReaderOutput = {
    "configFileError": cfgErr,
    "baseUnitFileError": baseUnitErr,
    "recordFileError": errorDF,
    "recordOutput": parsed
  }
  return csvFileReaderOutput
## end of export

def cfgFileInfo(baseUnitFile, cfgFile, recFile):
  result = {}
  baseUnitDF = pd.read_json(baseUnitFile)
  baseUnitDF.set_index("display_name", inplace = True)
  baseKeyName = baseUnitDF["key_name"].values
  with open(cfgFile, "r") as f:
    conDict = json.load(f)
  if not "csv_separator" in conDict.keys():
    conDict["csv_separator"] = ","
  recordDF = pd.read_csv(recFile, sep = conDict["csv_separator"])
  recordColumns = recordDF.columns.to_list()
  configDF = pd.DataFrame.from_dict(conDict["elements"])
  configDF.set_index("column", inplace = True)
  configDF["blank_accepted"] = configDF["blank_accepted"].isin(YES) # clean "blank_accepted" column
  configDF = chkCfgCol(configDF) #check configDF that all requires parameter is in the configDF DataFrame
  # trim category
  configDF = trimList(configDF, "category")
  # assign configDF key_name with baseUnit key_name
  baseUnitDispName = baseUnitDF.index.values
  setKeyNameIdx = configDF[configDF.index.isin(baseUnitDispName)].index.values
  configDF.loc[setKeyNameIdx, "key_name"] = baseUnitDF["key_name"]
  # check for error in BaseUnit file
  errBaseDF = pd.DataFrame()
  test = chkCfgBase(baseUnitDF, "display_name", recordColumns )
  frames = [errBaseDF, test]
  errBaseDF = pd.concat(frames)
  # check for error in Config file
  errCfgDF = pd.DataFrame()
  chkLists = [("column", recordColumns), ("key_name", baseKeyName), ("type", TYPE_LIST), ("cap", CAP_LIST)]
  for chkList in chkLists:
    test = chkCfgBase(configDF, chkList[0], chkList[1])
    frames = [errCfgDF, test]
    errCfgDF = pd.concat(frames)
  result = {
    "configDict": conDict,
    "configDF": configDF,
    "errorCfg": errCfgDF,
    "errorBaseUnit": errBaseDF
  }
  return result

# all parameter required for csv records data must be included.
# if a parameter is not in the config DataFrame, it is added as a blank column
def chkCfgCol(configDF):
  for configParameter in CONFIG_PARAMETERS:
    if not configParameter in configDF.columns:
      # add as new column and fill with nan
      configDF[configParameter] = np.nan
  # assign null key_name with the column name
  keyNameNull = configDF["key_name"].isnull()
  configDF.loc[keyNameNull, "key_name"] = configDF[keyNameNull].index
  return configDF

# convert case
def convertCase(df, cap):
  if cap == "lower":
    df = df.str.lower()
  elif cap == "upper":
    df = df.str.upper()
  return df

# check error of config file
def chkCfgBase(tempDF, col, lists):
  errorDF = pd.DataFrame()
  tempList = []
  if col == "column" or col == "display_name":
    errColumns = tempDF[~tempDF.index.isin(lists)].index.values
    for errColumn in errColumns:
      tempList.append([errColumn, f"{errColumn} is not found in the CSV data file"])
    errorDF = pd.DataFrame(tempList, columns=["column / display_name","error"])
    # check for missing column name in configuration or baseUnit file
    df = pd.DataFrame(lists, columns = ["csvColName"])
    errColumns = df["csvColName"][~df["csvColName"].isin(tempDF.index.values)]
    for errColumn in errColumns:
      tempList.append(["", f"{errColumn} {col} is missing"])
  elif col == "key_name":
    # find duplicate key_name
    duplicate = tempDF.duplicated(subset = "key_name", keep = False)
    errColumns = tempDF[duplicate].index.values
    for errColumn in errColumns:
      tempList.append([errColumn, f"{tempDF.loc[errColumn, 'key_name']} key_name is duplicated"])
    # check for key_name that is not found in baseUnit file
    errColumns = tempDF[~tempDF[col].isin(lists)].index.values
    for errColumn in errColumns:
      tempList.append([errColumn, f"{tempDF.loc[errColumn, 'key_name']} key_name is not found in the baseUnit file"])
    # check for missing key_name in configuration file
    df = pd.DataFrame(lists, columns = ["unitKeyName"])
    errColumns = df["unitKeyName"][~df["unitKeyName"].isin(tempDF["key_name"].values)]
    for errColumn in errColumns:
      tempList.append(["",f"{errColumn} key_name is missing"])
  elif col == "type":
    errEntries = ~tempDF[col].isin(lists)
    errRecords = tempDF[errEntries].index.values
    for errRecord in errRecords:
      tempList.append([errRecord, f"There error in \'{col}\' key values"])
    errorDF = pd.DataFrame(tempList, columns=["column / display_name","error"])
  else:
    emptyEntries = tempDF[col].isnull()
    tempDF = tempDF[~emptyEntries] # remove empty col from tempDF
    errEntries = ~tempDF[col].isin(lists)
    errRecords = tempDF[errEntries].index.values
    for errRecord in errRecords:
      tempList.append([errRecord, f"There error in \'{col}\' key values"])
  errorDF = pd.DataFrame(tempList, columns=["column / display_name","error"])
  return errorDF

# Trim category list in configDF json file
def trimList(configDF, col):
  # Check for empty entries for given column 'col'.
  emptyEntries = configDF[col].isnull()
  # Get from dataframe 'configDF' all rows that contain
  # valid (non null) 'col' entries.
  nnRows = configDF[~emptyEntries].index.values
  # Loop through the valid row entries and trim category list
  for idx in nnRows:
    # Get cell content
    cellCont = configDF.loc[idx, col]
    # Convert cell content to Pandas Series String object and
    # then strip all white spaces
    cleanCellCont = pd.Series(cellCont).str.strip()
    if configDF.loc[idx, "cap"] is not None:
      if configDF.loc[idx, "cap"] == "lower":
        cleanCellCont = cleanCellCont.str.lower()
      elif configDF.loc[idx, "cap"] == "upper":
        cleanCellCont = cleanCellCont.str.upper()
    cleanCellCont = cleanCellCont.to_list()
    # Update dataframe cell with cleaned up content
    configDF.at[idx, col] = cleanCellCont
  return configDF

def errorCheck(configDF, inputDF, columnNames):
  ## Check for error
  list = []
  errorDF = pd.DataFrame()
  for columnName in columnNames:
    if (configDF.loc[columnName, "type"] == "String"):
      test = stringExec(configDF, inputDF, columnName)
    elif (configDF.loc[columnName, "type"] == "List"):
      test = listExec(configDF, inputDF, columnName)
    elif (configDF.loc[columnName, "type"] == "Number"):
      test = numberExec(configDF, inputDF, columnName)
    frames = [errorDF, test]
    errorDF = pd.concat(frames)
  errorDF.sort_values(by = ["row"], ignore_index = True, inplace = True)
  return errorDF

# For String
def stringExec(configDF, inputDF, col):
  tempList = []
  # Check for empty entries for given column 'col'.
  emptyEntries = inputDF[col].isnull()
  if configDF.loc[col,"blank_accepted"] == False:
    # Get from dataframe 'inputDF' all rows that contain
    # null 'col' entries.
    errRecords = inputDF[emptyEntries].index.values
    # For each errRecords append the error message in the tempList
    for errRecord in errRecords:
      tempList.append([errRecord + ROW_OFFSET, f"{col} is blank"])
  # Remove null 'col' entries in inputDF
  inputDF = inputDF[~emptyEntries]
  # convert case
  inputDF.loc[:,col] = convertCase(inputDF[col], configDF.loc[col, "cap"])
  # Check for length exceed the min & max
  if pd.notna(configDF.loc[col, "max"]):
    errEntries = inputDF[col].str.len() > configDF.loc[col, "max"]
    errRecords = inputDF[errEntries].index.values
    # Remove error entries in inputDF
    inputDF = inputDF[~errEntries]
    for errRecord in errRecords:
      tempList.append([errRecord + ROW_OFFSET , f"{col} is longer than max({configDF.loc[col,'max']})"])
  if pd.notna(configDF.loc[col, "min"]):
    errEntries = inputDF[col].str.len() < configDF.loc[col, "min"]
    errRecords = inputDF[errEntries].index.values
    # Remove error entries in inputDF
    inputDF = inputDF[~errEntries]
    for errRecord in errRecords:
      tempList.append([errRecord + ROW_OFFSET , f"{col} is shorter than min({configDF.loc[col, 'min']})"])
  tempErrorDF = pd.DataFrame(tempList, columns=["row", "error"])
  return tempErrorDF

# For List
def listExec(configDF, inputDF, col):
  tempList = []
  #check for null or na
  emptyEntries = inputDF[col].isnull()
  if configDF.loc[col, "blank_accepted"] == False:
      errRecords = inputDF[emptyEntries].index.values
      for errRecords in errRecords:
        tempList.append([errRecords + ROW_OFFSET, f"{col} is blank"])
  inputDF = inputDF[~emptyEntries] # Remove blank 'col' entries in inputDF
  # convert case
  inputDF.loc[:, col] = convertCase(inputDF[col], configDF.loc[col, "cap"])
  #check values is in the list category
  errEntries = ~inputDF[col].str.strip().isin(configDF.loc[col, "category"])
  errRecords = inputDF[errEntries].index.values
  inputDF = inputDF[~errEntries] # Remove error entries in inputDF
  for errRecords in errRecords:
    tempList.append([errRecords + ROW_OFFSET, f"{col} is not in the list"])
  tempErrorDF = pd.DataFrame(tempList, columns=["row", "error"])
  return tempErrorDF

# For Number
def numberExec(configDF, inputDF, col):
  tempList = []
  # convert numeric string to number
  inputDF.loc[:, col]  = pd.to_numeric(inputDF[col], errors = "coerce")
  # Check if null or na
  emptyEntries = inputDF[col].isna()
  if not configDF.loc[col, "blank_accepted"]:
    error_records = inputDF[emptyEntries].index.values
    for error_record in error_records:
      tempList.append([error_record + ROW_OFFSET, f"{col} is blank or not a number"])
  inputDF = inputDF[~emptyEntries] # Remove error entries in inputDF
  # check for max limit
  if pd.notna(configDF.loc[col, "max"]):
    errEntries = inputDF[col] > configDF.loc[col, "max"]
    error_records = inputDF[errEntries].index.values
    inputDF = inputDF[~(errEntries)]  # Remove error entries in inputDF
    for error_record in error_records:
      tempList.append([error_record + ROW_OFFSET, f"{col} is greater the max({configDF.loc[col, 'max']})"])
  # check for min limit
  if pd.notna(configDF.loc[col, "min"]):
    errEntries = inputDF[col] < configDF.loc[col, "min"]
    error_records = inputDF[errEntries].index.values
    inputDF = inputDF[~(errEntries)]  # Remove error entries in inputDF
    for error_record in error_records:
      tempList.append([error_record + ROW_OFFSET, f"{col} is less than the min({configDF.loc[col, 'min']})"])
  tempErrorDF = pd.DataFrame(tempList, columns=["row", "error"])
  return tempErrorDF

##Clean data function
def trimClean(configDF, inputDF, col):
  inputDF.loc[:, col]=inputDF[col].str.strip()
  # convert case
  inputDF.loc[:, col] = convertCase(inputDF[col], configDF.loc[col, "cap"])
  return inputDF

def numberClean(inputDF, col):
  inputDF.loc[:, col]  = pd.to_numeric(inputDF[col], errors = "coerce")
  return inputDF
