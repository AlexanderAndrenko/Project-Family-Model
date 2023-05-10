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

        accountsIncome = pd.DataFrame(ExcludeNaN(total['Номер счета/карты зачисления'].unique()))
        accountsIncome.rename(columns={accountsIncome.columns[0] : 'Number'}, inplace=True)
        accountsOutcome = pd.DataFrame(ExcludeNaN(total['Номер счета/карты списания'].unique()))
        accountsOutcome.rename(columns={accountsOutcome.columns[0] : 'Number'}, inplace=True)
        accounts = pd.concat([accountsIncome, accountsOutcome]).drop_duplicates()
        LoadAccounts(accounts)

def ExcludeNaN(insertArray):
    return insertArray[~pd.isna(insertArray)]

def LoadAccounts(insertAccounts):    

    accountsDWH = Model.Model.GetAccount()
    listAccount = [(account.AccountID, account.Number, account.PersonID, account.TypeAccountID, account.BankID) for account in accountsDWH]
    listAccount = pd.DataFrame(listAccount, columns=['AccountID', 'Number','PersonID','TypeAccountID','BankID'])
    
    insertAccounts = insertAccounts.merge(listAccount, left_on='Number', right_on='Number', how='left')

    listBank = Model.Model.GetBank()
    listBank = [(bank.BankID, bank.Name) for bank in listBank]
    listBank = pd.DataFrame(listBank, columns=['BankID', 'Name'])
    bankID = listBank.loc[listBank['Name'] == "СБЕР"].iloc[0]

    listPerson = Model.Model.GetPerson()
    listPerson = [(person.PersonID, person.Firstname, person.Lastname) for person in listPerson]
    listPerson = pd.DataFrame(listPerson, columns=['PersonID', 'Firstname', 'Lastname'])
    personID = listPerson.loc[(listPerson['Firstname'] == "Александр") & (listPerson['Lastname'] == "Андренко")].iloc[0]

    insertAccounts['BankID'] = np.where(pd.isna(insertAccounts['BankID']), bankID['BankID'], insertAccounts['BankID'])
    insertAccounts['PersonID'] = personID['PersonID']

    # Ты остановился на том, что тебе необходимо написать метод загрузки аккаунтов в хранилище.
    # Необходимо сопоставить значения из базы, которые уже существуют и проставить им значения, которые уже есть
    # Чтобы избежать затирания даннах. Для новых счетов нужно проставлять, то что известно. Остальное руками в БД 

    print(insertAccounts)
