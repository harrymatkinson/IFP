# sqlalchemy classes for each table in our DB

from sqlalchemy import Column, Integer, String, Float, DateTime, Identity, JSON, ForeignKey
from sqlalchemy.ext.declarative import declarative_base

# if we use declarative_base() as a class for everything else to inherit,
# we can create all tables at the same time using Base.metadata.create_all()
Base = declarative_base()

class Bars(Base):
    __tablename__ = "bars"
    BAR_NAME = Column(String(), primary_key=True)
    BAR_STOCK = Column(JSON()) # this will store dict type values, so JSON is appropriate

class Transactions(Base):
    __tablename__ = "transactions"
    # Identity() means the field will autoincrement
    TRANS_ID = Column(Integer(), Identity(start=1, cycle=True), primary_key=True)
    BAR_NAME = Column(String(), ForeignKey("bars.BAR_NAME"))
    DRINK_NAME = Column(String(), ForeignKey("drinks.DRINK_NAME"))
    VALUE = Column(Float())
    TRANS_TIME = Column(DateTime(timezone=True))

class Glasses(Base):
    __tablename__ = "glasses"
    GLASS_NAME = Column(String(), primary_key=True)
    STOCK = Column(Integer())

class Drinks(Base):
    __tablename__ = "drinks"
    DRINK_NAME = Column(String(), primary_key=True)
    GLASS_NAME = Column(String(), ForeignKey("glasses.GLASS_NAME"))