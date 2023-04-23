import numpy as np
import pandas as pd
import os 
from os import walk
import Model.Model
from sqlalchemy.orm import sessionmaker

def prepareDataFile(OwnerName):

    totalData = pd.DataFrame()
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

            # while(not isSave and counter < 1000):
            #     if not os.path.isfile(pathDirDestination + "\\" + nameFile):
            #         os.rename(pathDir + nameFile, pathDirDestination + nameFile)
            #         isSave = True
            #     elif not os.path.isfile(pathDirDestination + "\\" + nameFile[:len(nameFile) - 5] + "(" + str(counter) + ").xlsx"):
            #         os.rename(pathDir + nameFile, pathDirDestination + nameFile[:len(nameFile) - 5] + "(" + str(counter) + ").xlsx")
            #         isSave = True
            #     else:
            #         counter += 1
    
    
    return totalData
        
def loadSberDataAlexander():
    Session = sessionmaker(Model.Model.engine)
    session = Session()

    total = prepareDataFile("Alexander")

    if not total.empty:
        Model.Model.SetTypeOperation(ExcludeNaN(total['Тип операции'].unique()))
        Model.Model.SetCurrency(ExcludeNaN(total['Валюта'].unique()))
        Model.Model.SetDescription(ExcludeNaN(total['Описание'].unique()))
        Model.Model.SetCategory(ExcludeNaN(total['Категория'].unique()))

        accounts = pd.DataFrame(total['Номер счета/карты зачисления'].unique())
        listBank = Model.Model.GetBank()
        # BankID = listBank.
        listBank = [(bank.BankID, bank.Name) for bank in listBank]
        listBank = pd.DataFrame(listBank, columns=['BankID', 'Name'])

        print(listBank)
        bankID = listBank.loc[listBank['Name'] == "СБЕР"].iloc[0]
        total['BankID'] = bankID['BankID']
        print(total.head(10))

        # Model.Model.SetAccount(ExcludeNaN())

def ExcludeNaN(insertArray):
    return insertArray[~pd.isna(insertArray)]