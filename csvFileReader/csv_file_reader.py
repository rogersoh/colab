import json
from csv_import_lib import csvFileReader

cfgFile = "fileconfig4_sample_data.json"
recFile = "sample_data.csv"
baseUnitFile = "baseunits.json"

csvFileReaderData = csvFileReader(baseUnitFile, cfgFile, recFile)
configurationFileErrorDF = csvFileReaderData["configFileError"]
baseUnitFileErrorDF = csvFileReaderData["baseUnitFileError"]
recordFileErrorMessageDF = csvFileReaderData["recordFileError"]
recordOutputList = csvFileReaderData["recordOutput"] #record output in List format
# save error message in csv file
if configurationFileErrorDF.shape[0] > 0:
  configurationFileErrorDF.to_csv(cfgFile.split(".")[0] + "_cfg_err_msg.csv", index = False)
if baseUnitFileErrorDF.shape[0] > 0:
  baseUnitFileErrorDF.to_csv(baseUnitFile.split(".")[0] + "_err_msg.csv", index = False)
if recordFileErrorMessageDF.shape[0] > 0:
  recordFileErrorMessageDF.to_csv(recFile.split(".")[0] + "_err_msg.csv", index = False)
# save record output (those records that have no error) in JSON format
if len(recordOutputList) > 0:
  recordOutputJSON = json.dumps(recordOutputList)
  with open(recFile.split(".")[0] + "_output.json", "w+") as f:
    f.write(recordOutputJSON)
