#from Model.Model import CreateDataBase, engine, Transaction
import Model.Model as Model
from sqlalchemy.orm import sessionmaker
import datetime
import sberETL
import pandas as pd

Model.__init__()

Session = sessionmaker(Model.engine)
session = Session()

# new_transaction = Model.Transaction(
#         SourceID = 1,
#         DateCreated = datetime.datetime.now(), 
#         TypeOperationID = 1, 
#         Sum = 100,
#         CurrencyTypeID = 1,
#         DescriptionID = 1,
#         PlaceID = 1,
#         AccountID = 1
#     )

total = sberETL.prepareDataFile()


Model.TypeOperationInsert(total['Тип операции'].unique())

print(total.head(100))


session.close()