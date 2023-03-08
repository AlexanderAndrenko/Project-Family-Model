from sqlalchemy import Boolean, ForeignKey, create_engine, Column, Integer, String, Float, DateTime, null
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine.reflection import Inspector

engine = create_engine('sqlite:///DWH.db')
Base = declarative_base()

class Transaction(Base):
    __tablename__ = 'Transactions'
    TransactionID = Column('TransactionID', Integer, primary_key=True, autoincrement=True)
    SourceID = Column('SourceID', Integer, ForeignKey('Sources.SourceID'))
    DateCreated = Column('DateCreated', DateTime)
    TypeOperationID = Column('TypeOperationID', Integer, ForeignKey('TypeOperations.TypeOperationID'))
    Sum = Column('Sum', Float)
    CurrencyTypeID = Column('CurrencyTypeID', Integer, ForeignKey('Currencies.CurrencyID'))
    DescriptionID = Column('DescriptionID', Integer, ForeignKey('Descriptions.DescriptionID'))
    PlaceID = Column('PlaceID', Integer, ForeignKey('Places.PlaceID'))
    CategoryID = Column('CategoryID', Integer, ForeignKey('Categories.CategoryID'))
    AccountID = Column('AccountID', Integer, ForeignKey('Accounts.AccountID'))

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
    Direction = Column('Direction', Integer, nullable=False)

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
    TypeAccountID = Column('TypeAccountID', Integer, primary_key=True, autoincrement=True)
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
    BankID = Column('BankID', Integer, ForeignKey('Banks.BankID'))
    TypeAccountID = Column('TypeAccountID', Integer, ForeignKey('TypeAccounts.TypeAccountID'))
    PersonID = Column('PersonID', Integer, ForeignKey('Persons.PersonID'))

def __init__():
    inspector = Inspector.from_engine(engine)

    if(len(inspector.get_table_names()) == 0):
        CreateDataBase()

def CreateDataBase():
    Base.metadata.create_all(engine)
    InitializeUndefined()

def InitializeUndefined():
    Session = sessionmaker(engine)
    session = Session()
    arrayUndefined = []

    arrayUndefined.append(Source(Name='Undefined'))
    arrayUndefined.append(Place(Name='Undefined', CategoryID=0, Address='', Latitude=0.0, Longitude=0.0))
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
