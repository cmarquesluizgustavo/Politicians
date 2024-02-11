from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, Boolean
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


class Term(Base):
    __tablename__ = "Term"
    id = Column(Integer, primary_key=True)
    congressperson_id = Column(Integer, ForeignKey("CongressPerson.id"))
    congressperson = relationship("CongressPerson", backref="Terms")
    state = Column(String)
    party = Column(String)
    legislature_id = Column(Integer)
    election_year = Column(Integer)


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
    main_author = Column(
        Boolean,
        nullable=True,
    )
    congressperson = relationship("CongressPerson", backref="Authorships")
    bill = relationship("Bill", backref="Authors")
