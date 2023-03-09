import pandas as pd
import os 
from os import walk

def prepareDataFile():
    pathDir = "C:\\Users\\andre\\OneDrive\\Andrenko.AO\\Family\\Данные\\"
    pathDirDestination = "C:\\Users\\andre\\OneDrive\\Andrenko.AO\\Family\\Данные\\Processed\\"
    totalData = pd.DataFrame()

    if not os.path.isdir("C:\\Users\\andre\\OneDrive\\Andrenko.AO\\Family\\Данные\\Processed"):
        os.mkdir("C:\\Users\\andre\\OneDrive\\Andrenko.AO\\Family\\Данные\\Processed")

    fileNameArr = []
    for (_, _, filenames) in walk(pathDir):
        fileNameArr.extend(filenames)
        break

    for nameFile in fileNameArr:
        if(nameFile != "Data.xlsx"):
            pathFile = pathDir + nameFile
            myFile = pd.read_excel(pathFile)
            totalData = pd.concat([totalData, myFile], ignore_index=True)

            counter = 1
            isSave = False

            while(not isSave and counter < 1000):
                if not os.path.isfile("C:\\Users\\andre\\OneDrive\\Andrenko.AO\\Family\\Данные\\Processed\\" + nameFile):
                    os.rename(pathDir + nameFile, pathDirDestination + nameFile)
                    isSave = True
                elif not os.path.isfile("C:\\Users\\andre\\OneDrive\\Andrenko.AO\\Family\\Данные\\Processed\\" + nameFile[:len(nameFile) - 5] + "(" + str(counter) + ").xlsx"):
                    os.rename(pathDir + nameFile, pathDirDestination + nameFile[:len(nameFile) - 5] + "(" + str(counter) + ").xlsx")
                    isSave = True
                else:
                    counter += 1
    
    return totalData
        
#totalData = prepareDataFile() #totalData[totalData['Номер счета/карты зачисления'].notnull()]
# accounts = pd.DataFrame(totalData["Номер счета/карты зачисления"].unique())


# # currentTransactions = pd.read_excel("C:\\Users\\andre\\OneDrive\\Andrenko.AO\\Family\\Данные\\Data.xlsx", sheet_name = "Transactions")
# currentAccounts = pd.read_excel("C:\\Users\\andre\\OneDrive\\Andrenko.AO\\Family\\Данные\\Data.xlsx", sheet_name = "Accounts")

# newAccounts = pd.concat([currentAccounts, accounts], ignore_index=False)

# print(newAccounts)



# with pd.ExcelWriter("C:\\Users\\andre\\OneDrive\\Andrenko.AO\\Family\\Данные\\Data.xlsx") as writer:
#     totalData.to_excel(writer, sheet_name="Transactions")
#     accounts.to_excel(writer, sheet_name="Accounts")
