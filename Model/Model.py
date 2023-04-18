from sqlalchemy import Boolean, ForeignKey, create_engine, Column, Integer, String, Float, DateTime, null
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine.reflection import Inspector
import pandas as pd

engine = create_engine('sqlite:///DWH.db')
Base = declarative_base()

#region Entites classes

class Transaction(Base):
    __tablename__ = 'Transactions'
    TransactionID = Column('TransactionID', Integer, primary_key=True, autoincrement=True)
    SourceID = Column('SourceID', Integer, ForeignKey('Sources.SourceID'), nullable=False, default=1)
    DateCreated = Column('DateCreated', DateTime)
    TypeOperationID = Column('TypeOperationID', Integer, ForeignKey('TypeOperations.TypeOperationID'), nullable=False, default=1)
    Sum = Column('Sum', Float)
    CurrencyTypeID = Column('CurrencyTypeID', Integer, ForeignKey('Currencies.CurrencyID'), nullable=False, default=1)
    DescriptionID = Column('DescriptionID', Integer, ForeignKey('Descriptions.DescriptionID'), nullable=False, default=1)
    PlaceID = Column('PlaceID', Integer, ForeignKey('Places.PlaceID'), nullable=False, default=1)
    CategoryID = Column('CategoryID', Integer, ForeignKey('Categories.CategoryID'), nullable=False, default=1)
    AccountID = Column('AccountID', Integer, ForeignKey('Accounts.AccountID'), nullable=False, default=1)

class Source(Base):
    __tablename__ = 'Sources'
    SourceID = Column('SourceID', Integer, primary_key=True, autoincrement=True)
    Name = Column('Name', String)

class Place(Base):
    __tablename__ = 'Places'
    PlaceID = Column('PlaceID', Integer, primary_key=True, autoincrement=True)    
    Name = Column('Name', String, nullable=False)
    Address = Column('Address', String)
    Latitude = Column('Latitude', Float)
    Longitude = Column('Longitude', Float)

class Currency(Base):
    __tablename__ = 'Currencies'
    CurrencyID = Column('CurrencyID', Integer, primary_key=True, autoincrement=True)
    Code = Column('Code', String, nullable=False)
    Name = Column('Name', String)

class TypeOperation(Base):
    __tablename__ = 'TypeOperations'
    TypeOperationID = Column('TypeOperationID', Integer, primary_key=True, autoincrement=True)
    Name = Column('Name', String)
    Direction = Column('Direction', Integer, nullable=False, default=0)

class Description(Base):
    __tablename__ = 'Descriptions'
    DescriptionID = Column('DescriptionID', Integer, primary_key=True, autoincrement=True)
    Description = Column('Description', String)

class Category(Base):
    __tablename__ = 'Categories'
    CategoryID = Column('CategoryID', Integer, primary_key=True, autoincrement=True)
    Name = Column('Name', String)
    Group = Column('Group', String)

class TypeAccount(Base):
    __tablename__ = 'TypeAccounts'
    TypeAccountID = Column('TypeAccountID', Integer, primary_key=True, autoincrement=True, nullable=False)
    Name = Column('Name', String)

class Person(Base):
    __tablename__ = 'Persons'
    PersonID = Column('PersonID', Integer, primary_key=True, autoincrement=True)
    Firstname = Column('Firstname', String)
    Middlename = Column('Middlename', String)
    Lastname = Column('Lastname', String)
    Family = Column('Family', String)

class Bank(Base):
    __tablename__ = 'Banks'
    BankID = Column('BankID', Integer, primary_key=True, autoincrement=True)
    Name = Column('Name', String)

class Account(Base):
    __tablename__ = 'Accounts'
    AccountID = Column('AccountID', Integer, primary_key=True, autoincrement=True)
    Number = Column('Number', String)
    BankID = Column('BankID', Integer, ForeignKey('Banks.BankID'), nullable=False, default=1)
    TypeAccountID = Column('TypeAccountID', Integer, ForeignKey('TypeAccounts.TypeAccountID'), nullable=False, default=1)
    PersonID = Column('PersonID', Integer, ForeignKey('Persons.PersonID'), nullable=False, default=1)

#endregion

#region Initialize methods

def __init__():
    inspector = Inspector.from_engine(engine)

    if(len(inspector.get_table_names()) == 0):
        CreateDataBase()

def CreateDataBase():
    Base.metadata.create_all(engine)
    InitializeUndefined()
    InitializeBank()
    InitializePerson()
    InitializeTypeAccount()

def InitializeUndefined():
    Session = sessionmaker(engine)
    session = Session()
    arrayUndefined = []

    arrayUndefined.append(Source(Name='Undefined'))
    arrayUndefined.append(Place(Name='Undefined', Address='', Latitude=0.0, Longitude=0.0))
    arrayUndefined.append(Currency(Code='Undefined', Name='Undefined'))
    arrayUndefined.append(TypeOperation(Name='Undefined', Direction=0))
    arrayUndefined.append(Description(Description='Undefined'))
    arrayUndefined.append(Category(Name='Undefined', Group='Undefined'))
    arrayUndefined.append(TypeAccount(Name='Undefined'))
    arrayUndefined.append(Person(Firstname='Undefined', Middlename='Undefined', Lastname='Undefined', Family='Undefined'))
    arrayUndefined.append(Bank(Name='Undefined'))
    arrayUndefined.append(Account(Number='Undefined', BankID=1, TypeAccountID=1, PersonID=1))

    session.add_all(arrayUndefined)
    session.commit()

def InitializePerson():
    Session = sessionmaker(engine)
    session = Session()
    arrayPerson = []

    arrayPerson.append(
        Person(
            Firstname = 'Александр',
            Middlename = 'Олегович',
            Lastname = 'Андренко',
            Family = 'Андренко')
        )
    
    arrayPerson.append(
        Person(
            Firstname = 'Дарья',
            Middlename = 'Андреевна',
            Lastname = 'Андренко',
            Family = 'Андренко')
        )
    
    session.add_all(arrayPerson)
    session.commit()

def InitializeBank():
    Session = sessionmaker(engine)
    session = Session()
    arrayBank = []

    arrayBank.append(
        Bank(
            Name = 'СБЕР'
        )
    )
    
    arrayBank.append(
        Bank(
            Name = 'Росбанк'
        )
    )

    arrayBank.append(
        Bank(
            Name = 'Тинькофф'
        )
    )

    arrayBank.append(
        Bank(
            Name = 'ВТБ'
        )
    )

    arrayBank.append(
        Bank(
            Name = 'Альфа'
        )
    )

    session.add_all(arrayBank)
    session.commit()

def InitializeTypeAccount():
    Session = sessionmaker(engine)
    session = Session()
    arrayTypeAccount = []

    arrayTypeAccount.append(
        TypeAccount(
            Name = 'Дебетовая карта'
        )
    )

    arrayTypeAccount.append(
        TypeAccount(
            Name = 'Кредитная карта'
        )
    )

    arrayTypeAccount.append(
        TypeAccount(
            Name = 'Накопительный счет'
        )
    )

    session.add_all(arrayTypeAccount)
    session.commit()

#endregion

#region SET operations

def SetSource(unique_values):
    Session = sessionmaker(engine)
    session = Session()

    for value in unique_values:
        name = session.query(Source).filter(Source.Name == value).first()
        if not name:
            name = Source(Name=value)
            session.add(name)

    session.commit()
    session.close()

def SetPlace(unique_values):
    Session = sessionmaker(engine)
    session = Session() 

    for value in unique_values:
        place = session.query(Place).filter_by(Name=value['Name'], Address=value['Address'], Latitude=value['Latitude'], Longitude=value['Longitude']).first()
        if not place:
            place = Place(Name=value['Name'], Address=value['Address'], Latitude=value['Latitude'], Longitude=value['Longitude'])
            session.add(place)

    session.commit()
    session.close()

def SetTypeOperation(unique_values):
    Session = sessionmaker(engine)
    session = Session()

    for value in unique_values:
        type_op = session.query(TypeOperation).filter(TypeOperation.Name == value).first()
        if not type_op:
            type_op = TypeOperation(Name=value)
            session.add(type_op)

    session.commit()
    session.close()

def SetCurrency(unique_values):
    Session = sessionmaker(engine)
    session = Session() 

    for value in unique_values:
        codeCurrency = session.query(Currency).filter(Currency.Code == value).first()
        if not codeCurrency:
            codeCurrency = Currency(Code=value)
            session.add(codeCurrency)

    session.commit()
    session.close()

def SetDescription(unique_values):
    Session = sessionmaker(engine)
    session = Session()  

    for value in unique_values:
        description = session.query(Description).filter(Description.Description == value).first()
        if not description:
            description = Description(Description=value)
            session.add(description)

    session.commit()
    session.close()

def SetAccount(unique_values):
    Session = sessionmaker(engine)
    session = Session()

    for value in unique_values:
        number = session.query(Account).filter(Account.Number == value).first()
        if not number:
            number = Account(Number=value)
            session.add(number)

    session.commit()
    session.close()

def SetCategory(unique_values):

    Session = sessionmaker(engine)
    session = Session()

    for value in unique_values:
        name = session.query(Category).filter(Category.Name == value).first()
        if not name:
            name = Category(Name=value)
            session.add(name)

    session.commit()
    session.close()

def SetTransaction(list_of_transaction):
    return 1
#endregion

#region GET operations

def GetSource():
    Session = sessionmaker(engine)
    session = Session()

    source = session.query(Source).all()

    return source

def GetPlace():
    Session = sessionmaker(engine)
    session = Session()

    place = session.query(Place).all()

    return place

def GetTypeOperation():
    Session = sessionmaker(engine)
    session = Session()

    typeOperation = session.query(TypeOperation).all()

    return typeOperation

def GetCurrency():
    Session = sessionmaker(engine)
    session = Session()

    currency = session.query(Currency).all()

    return currency

def GetDescription():
    Session = sessionmaker(engine)
    session = Session()

    description = session.query(Description).all()

    return description

def GetAccount():
    Session = sessionmaker(engine)
    session = Session()

    account = session.query(Account).all()

    return account

def GetCategory():
    Session = sessionmaker(engine)
    session = Session()

    category = session.query(Category).all()

    return category

def GetBank():
    Session = sessionmaker(engine)
    session = Session()

    bank = pd.read_sql('SELECT * FROM Bank', engine) #session.query(Bank).all()
    return bank

#endregion



