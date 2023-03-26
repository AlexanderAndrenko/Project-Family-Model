import pandas as pd
import os 
from os import walk
import Model.Model
from sqlalchemy.orm import sessionmaker

def prepareDataFile(OwnerName):

    totalData = pd.DataFrame()

    
        # pathDir = "C:\\Users\\andre\\OneDrive\\Andrenko.AO\\Family\\Данные Саша\\"
        # pathDirDestination = "C:\\Users\\andre\\OneDrive\\Andrenko.AO\\Family\\Данные Саша\\Processed\\"        

        # if not os.path.isdir("C:\\Users\\andre\\OneDrive\\Andrenko.AO\\Family\\Данные Саша\\Processed"):
        #     os.mkdir("C:\\Users\\andre\\OneDrive\\Andrenko.AO\\Family\\Данные Саша\\Processed")

        # fileNameArr = []
        # for (_, _, filenames) in walk(pathDir):
        #     fileNameArr.extend(filenames)
        #     break

        # for nameFile in fileNameArr:
        #     if(nameFile != "Data.xlsx"):
        #         pathFile = pathDir + nameFile
        #         myFile = pd.read_excel(pathFile)
        #         totalData = pd.concat([totalData, myFile], ignore_index=True)

        #         counter = 1
        #         isSave = False

        #         while(not isSave and counter < 1000):
        #             if not os.path.isfile("C:\\Users\\andre\\OneDrive\\Andrenko.AO\\Family\\Данные Саша\\Processed\\" + nameFile):
        #                 os.rename(pathDir + nameFile, pathDirDestination + nameFile)
        #                 isSave = True
        #             elif not os.path.isfile("C:\\Users\\andre\\OneDrive\\Andrenko.AO\\Family\\Данные Саша\\Processed\\" + nameFile[:len(nameFile) - 5] + "(" + str(counter) + ").xlsx"):
        #                 os.rename(pathDir + nameFile, pathDirDestination + nameFile[:len(nameFile) - 5] + "(" + str(counter) + ").xlsx")
        #                 isSave = True
        #             else:
        #             counter += 1
    if OwnerName == "Alexander":    
        pathDir = "C:\\Users\\andre\\OneDrive\\Andrenko.AO\\Family\\Данные Саша\\"
        pathDirDestination = "C:\\Users\\andre\\OneDrive\\Andrenko.AO\\Family\\Данные Саша\\Processed"
    elif OwnerName == "Daria":
        pathDir = "C:\\Users\\andre\\OneDrive\\Andrenko.AO\\Family\\Данные Даша\\"
        pathDirDestination = "C:\\Users\\andre\\OneDrive\\Andrenko.AO\\Family\\Данные Даша\\Processed"

    if not os.path.isdir(pathDirDestination):
        os.mkdir(pathDirDestination)

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
                if not os.path.isfile(pathDirDestination + "\\" + nameFile):
                    os.rename(pathDir + nameFile, pathDirDestination + nameFile)
                    isSave = True
                elif not os.path.isfile(pathDirDestination + "\\" + nameFile[:len(nameFile) - 5] + "(" + str(counter) + ").xlsx"):
                    os.rename(pathDir + nameFile, pathDirDestination + nameFile[:len(nameFile) - 5] + "(" + str(counter) + ").xlsx")
                    isSave = True
                else:
                    counter += 1
    
    
    return totalData
        
def loadSberDataAlexander():
    Session = sessionmaker(Model.engine)
    session = Session()

    total = prepareDataFile("Alexander")

    if not total.empty:
        Model.TypeOperationInsert(total['Тип операции'].unique())
        Model.CurrencyInsert(total['Валюта'].unique())
        Model.DescriptionInsert(total['Описание'].unique())
        Model.AccountInsert(total['Номер счета/карты зачисления'].unique())
        Model.CategoryInsert(total['Категория'].unique())