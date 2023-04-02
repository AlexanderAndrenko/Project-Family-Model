#from Model.Model import CreateDataBase, engine, Transaction
import Model.Model as Model
from sqlalchemy.orm import sessionmaker
import datetime
import sberETL
import pandas as pd
import numpy as np

Model.__init__()
sberETL.loadSberDataAlexander()

