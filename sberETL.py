import locale
import numpy as np
import pandas as pd
import os 
from os import walk
import Model.Model as mm
from sqlalchemy.orm import sessionmaker
import dateparser

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

        for value in ExcludeNaN(total['Категория'].unique()):
            type_op = session.query(mm.Category).filter(mm.Category.Name == value).first()
            if not type_op:
                type_op = mm.Category(Name=value)
                listOfCategories.append(type_op)

        mm.SetEntites(listOfCategories)

        accountsIncome = ExcludeNaN(total['Номер счета/карты зачисления'].unique())
        accountsOutcome = ExcludeNaN(total['Номер счета/карты списания'].unique())
        accounts = [j for i in [accountsIncome, accountsOutcome] for j in i]

        LoadAccounts(accounts)

        total['AccountName'] = np.where( pd.isnull(total['Номер счета/карты зачисления']), total['Номер счета/карты списания'], total['Номер счета/карты зачисления'])
        LoadTransactions(total)

def ExcludeNaN(insertArray):
    return insertArray[~pd.isna(insertArray)]

def LoadAccounts(insertAccounts):    
    
    Session = sessionmaker(mm.engine)
    session = Session()

    listBank = mm.GetBank()
    listBank = [(bank.BankID, bank.Name) for bank in listBank]
    listBank = pd.DataFrame(listBank, columns=['BankID', 'Name'])
    bankID = listBank.loc[listBank['Name'] == "СБЕР"].iloc[0]

    listPerson = mm.GetPerson()
    listPerson = [(person.PersonID, person.Firstname, person.Lastname) for person in listPerson]
    listPerson = pd.DataFrame(listPerson, columns=['PersonID', 'Firstname', 'Lastname'])
    personID = listPerson.loc[(listPerson['Firstname'] == "Александр") & (listPerson['Lastname'] == "Андренко")].iloc[0]

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

def LoadTransactions(insertTransactions):

    Session = sessionmaker(mm.engine)
    session = Session()

    typeOperationDWH = mm.GetTypeOperation()
    listTypesOperations = [(typeOperation.TypeOperationID, typeOperation.Name) for typeOperation in typeOperationDWH]
    listTypesOperations = pd.DataFrame(listTypesOperations, columns=['TypeOperationID', 'Name'])
    insertTransactions = insertTransactions.merge(listTypesOperations, left_on='Тип операции', right_on='Name', how='left')

    categoriesDWH = mm.GetCategory()
    listCategories = [(category.CategoryID, category.Name) for category in categoriesDWH]
    listCategories = pd.DataFrame(listCategories, columns=['CategoryID', 'Name'])
    insertTransactions = insertTransactions.merge(listCategories, left_on='Категория', right_on='Name', how='left')

    currenciesDWH = mm.GetCurrency()
    listCurrencies = [(currency.CurrencyID, currency.Code) for currency in currenciesDWH]
    listCurrencies = pd.DataFrame(listCurrencies, columns=['CurrencyID', 'Code'])
    insertTransactions = insertTransactions.merge(listCurrencies, left_on='Валюта', right_on='Code', how='left')

    descriptionsDWH = mm.GetDescription()
    listDescriptions = [(description.DescriptionID, description.Description) for description in descriptionsDWH]
    listDescriptions = pd.DataFrame(listDescriptions, columns=['DescriptionID', 'Description'])
    insertTransactions = insertTransactions.merge(listDescriptions, left_on='Описание', right_on='Description', how='left')

    accountsDWH = mm.GetAccount()
    listAccounts = [(account.AccountID, account.Number) for account in accountsDWH]
    listAccounts = pd.DataFrame(listAccounts, columns=['AccountID', 'Number'])
    insertTransactions = insertTransactions.merge(listAccounts, left_on='AccountName', right_on='Number', how='left')

    
    
    listOfTransactions = []

    for i in range(insertTransactions.shape[0]):
        type_op = session.query(mm.Transaction).filter(
            mm.Transaction.DateCreated == dateparser.parse(insertTransactions.loc[i]['Дата'], languages=['ru']),
            mm.Transaction.TypeOperationID == insertTransactions.loc[i]['TypeOperationID'],
            mm.Transaction.Sum == insertTransactions.loc[i]['Сумма'],
            mm.Transaction.CurrencyID == insertTransactions.loc[i]['CurrencyID'],
            mm.Transaction.DescriptionID == insertTransactions.loc[i]['DescriptionID'],
            mm.Transaction.CategoryID == insertTransactions.loc[i]['CategoryID'],
            mm.Transaction.AccountID == insertTransactions.loc[i]['AccountID']
            ).first()
        if not type_op:
            type_op = mm.Transaction(
                SourceID = 1,
                DateCreated = dateparser.parse(insertTransactions.loc[i]['Дата'], languages=['ru']),
                TypeOperationID = int(insertTransactions.loc[i]['TypeOperationID']),
                Sum = insertTransactions.loc[i]['Сумма'],
                CurrencyID = int(insertTransactions.loc[i]['CurrencyID']),
                DescriptionID = int(insertTransactions.loc[i]['DescriptionID']),
                PlaceID = 1,
                CategoryID = int(insertTransactions.loc[i]['CategoryID']),
                AccountID = int(insertTransactions.loc[i]['AccountID'])
                )
            listOfTransactions.append(type_op)


    mm.SetEntites(listOfTransactions)

    # print(insertTransactions)
    # print(insertTransactions[['Номер','Дата','TypeOperationID', 'Сумма','CurrencyID', 'DescriptionID', 'CategoryID', 'AccountID']])
