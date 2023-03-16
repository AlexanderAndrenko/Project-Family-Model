#from Model.Model import CreateDataBase, engine, Transaction
import Model.Model as Model
from sqlalchemy.orm import sessionmaker
import datetime
import sberETL
import pandas as pd
import numpy as np

Model.__init__()

Session = sessionmaker(Model.engine)
session = Session()

total = sberETL.prepareDataFile()

if not total.empty:
    Model.TypeOperationInsert(total['Тип операции'].unique())
    Model.CurrencyInsert(total['Валюта'].unique())
    Model.DescriptionInsert(total['Описание'].unique())

    accountNumber = total['Номер счета/карты зачисления'].unique()
    accountNumber = accountNumber[~np.isnan(accountNumber) & (accountNumber != None)]
    Model.AccountInsert(accountNumber)

print(total['Номер счета/карты зачисления'].unique())

session.close()