import numpy as np
import pandas as pd
import os 
from os import walk
import Model.Model as mm
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
    Session = sessionmaker(mm.engine)
    session = Session()

    total = prepareDataFile("Alexander")

    if not total.empty:
        # mm.SetTypeOperation(ExcludeNaN(total['Тип операции'].unique()))
        # mm.SetCurrency(ExcludeNaN(total['Валюта'].unique()))
        # mm.SetDescription(ExcludeNaN(total['Описание'].unique()))
        # mm.SetCategory(ExcludeNaN(total['Категория'].unique()))
        
        # Loading types of opreations
        listOfTypeOperations = []

        for value in ExcludeNaN(total['Тип операции'].unique()):
            type_op = session.query(mm.TypeOperation).filter(mm.TypeOperation.Name == value).first()
            if not type_op:
                type_op = mm.TypeOperation(Name=value)
                listOfTypeOperations.append(type_op)

        mm.SetEntites(listOfTypeOperations)

        # Loading currencies 
        listOfCurrencies = []

        for value in ExcludeNaN(total['Валюта'].unique()):
            type_op = session.query(mm.Currency).filter(mm.Currency.Code == value).first()
            if not type_op:
                type_op = mm.Currency(Code=value)
                listOfCurrencies.append(type_op)

        mm.SetEntites(listOfCurrencies)

        # Loading descriptions 
        listOfDescriptions = []

        for value in ExcludeNaN(total['Описание'].unique()):
            type_op = session.query(mm.Description).filter(mm.Description.Description == value).first()
            if not type_op:
                type_op = mm.Description(Description=value)
                listOfDescriptions.append(type_op)

        mm.SetEntites(listOfDescriptions)

        # Loading categories 
        listOfCategories = []

        for value in ExcludeNaN(total['Описание'].unique()):
            type_op = session.query(mm.Category).filter(mm.Category.Name == value).first()
            if not type_op:
                type_op = mm.Category(Name=value)
                listOfCategories.append(type_op)

        mm.SetEntites(listOfCategories)

        # Preparation a list of accounts
        # accountsIncome = pd.DataFrame(ExcludeNaN(total['Номер счета/карты зачисления'].unique()))
        # accountsIncome.rename(columns={accountsIncome.columns[0] : 'Number'}, inplace=True)
        # accountsOutcome = pd.DataFrame(ExcludeNaN(total['Номер счета/карты списания'].unique()))
        # accountsOutcome.rename(columns={accountsOutcome.columns[0] : 'Number'}, inplace=True)
        # accounts = pd.concat([accountsIncome, accountsOutcome]).drop_duplicates()

        accountsIncome = ExcludeNaN(total['Номер счета/карты зачисления'].unique())
        accountsOutcome = ExcludeNaN(total['Номер счета/карты списания'].unique())
        accounts = [j for i in [accountsIncome, accountsOutcome] for j in i]

        LoadAccounts(accounts)

def ExcludeNaN(insertArray):
    return insertArray[~pd.isna(insertArray)]

def LoadAccounts(insertAccounts):    

    Session = sessionmaker(mm.engine)
    session = Session()

    # accountsDWH = mm.GetAccount()

    # listAccount = [(account.AccountID, account.Number, account.PersonID, account.TypeAccountID, account.BankID) for account in accountsDWH]
    # listAccount = pd.DataFrame(listAccount, columns=['AccountID', 'Number','PersonID','TypeAccountID','BankID'])
    # print(insertAccounts)
    # insertAccounts = insertAccounts.merge(listAccount, left_on='Number', right_on='Number', how='left')

    

    listBank = mm.GetBank()
    listBank = [(bank.BankID, bank.Name) for bank in listBank]
    listBank = pd.DataFrame(listBank, columns=['BankID', 'Name'])
    bankID = listBank.loc[listBank['Name'] == "СБЕР"].iloc[0]

    listPerson = mm.GetPerson()
    listPerson = [(person.PersonID, person.Firstname, person.Lastname) for person in listPerson]
    listPerson = pd.DataFrame(listPerson, columns=['PersonID', 'Firstname', 'Lastname'])
    personID = listPerson.loc[(listPerson['Firstname'] == "Александр") & (listPerson['Lastname'] == "Андренко")].iloc[0]

    # insertAccounts['BankID'] = np.where(pd.isna(insertAccounts['BankID']), bankID['BankID'], insertAccounts['BankID'])
    # insertAccounts['PersonID'] = np.where(pd.isna(insertAccounts['PersonID']), personID['PersonID'], insertAccounts['PersonID'])

    # Нужно брать только те которых нет в КХД
    # insertAccounts = insertAccounts[pd.isna(insertAccounts['AccountID'])]

    # insertAccounts['BankID'] = bankID['BankID']
    # insertAccounts['PersonID'] = personID['PersonID']
    # insertAccounts['TypeAccountID'] = 1

    # Loading categories 
    listOfAccounts = []

    for value in set(insertAccounts):
        type_op = session.query(mm.Account).filter(mm.Account.Number == value).first()
        if not type_op:
            type_op = mm.Account(
                Number=value,
                BankID=int(bankID['BankID']),
                TypeAccountID=1,
                PersonID=int(personID['PersonID'])
                )
            listOfAccounts.append(type_op)

    mm.SetEntites(listOfAccounts)


    # listAccount = []

    # mm.SetAccount(insertAccounts)
