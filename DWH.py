#from Model.Model import CreateDataBase, engine, Transaction
import Model.Model as Model
from sqlalchemy.orm import sessionmaker
import datetime

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

session.close()