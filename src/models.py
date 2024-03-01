from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, Float
from sqlalchemy.orm import relationship


class Base(DeclarativeBase):
    __abstract__ = True


class CongressPerson(Base):
    __tablename__ = "CongressPerson"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    state = Column(String)
    education = Column(String)
    birth_date = Column(DateTime)
    sex = Column(String)
    cpf = Column(String)
    social_network = Column(String)
    occupation = Column(String)
    ethnicity = Column(String)


class Network(Base):
    __tablename__ = "Network"
    id = Column(Integer, primary_key=True)
    type = Column(String)


class Term(Base):
    __tablename__ = "Term"
    id = Column(Integer, primary_key=True)
    congressperson_id = Column(Integer, ForeignKey("CongressPerson.id"))
    network_id = Column(Integer, ForeignKey("Network.id"))

    state = Column(String)
    party = Column(String)
    region = Column(String)

    # congressperson = relationship("CongressPerson", backref="Terms")
    # network = relationship("Network", backref="Terms")


class Bill(Base):
    __tablename__ = "Bill"
    id = Column(Integer, primary_key=True)
    status_id = Column(Integer)
    type = Column(String)
    year = Column(Integer)
    description = Column(String)
    keywords = Column(String)


class Authorship(Base):
    __tablename__ = "Authorship"
    id = Column(Integer, primary_key=True)
    congressperson_id = Column(Integer, ForeignKey("CongressPerson.id"))
    bill_id = Column(Integer, ForeignKey("Bill.id"))
    # congressperson = relationship("CongressPerson", backref="Authorships")
    # bill = relationship("Bill", backref="Authors")


class Statistics(Base):
    __tablename__ = "Statistics"
    id = Column(Integer, primary_key=True)
    type = Column(String)
    value = Column(Float)
    label = Column(String)
    network_id = Column(Integer, ForeignKey("Network.id"))
    congressperson_id = Column(Integer, ForeignKey("CongressPerson.id"), nullable=True)

    # network = relationship("Network", backref="Statistics")
    # congressperson_statistics = relationship(
    #     "CongressPersonStatistics", backref="Statistics"
    # )
