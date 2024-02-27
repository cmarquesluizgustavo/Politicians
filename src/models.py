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


class Term(Base):
    __tablename__ = "Term"
    id = Column(Integer, primary_key=True)
    congressperson_id = Column(Integer, ForeignKey("CongressPerson.id"))

    state = Column(String)
    party = Column(String)

    network_id = Column(Integer)
    congressperson = relationship("CongressPerson", backref="Terms")


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
    congressperson = relationship("CongressPerson", backref="Authorships")
    bill = relationship("Bill", backref="Authors")


class Network(Base):
    id = Column(Integer, primary_key=True)
    type = Column(String)


class CongressPersonStatistics(Base):
    id = Column(Integer, primary_key=True)
    congressperson_id = Column(Integer, ForeignKey("CongressPerson.id"))
    network_id = Column(Integer, ForeignKey("Network.id"))

    total_bills = Column(Integer)

    party = Column(String)
    state = Column(String)
    education = Column(String)
    gender = Column(String)
    region = Column(String)
    occupation = Column(String)
    ethnicity = Column(String)

    degree = Column(Integer)
    pagerank = Column(Float)
    betweenness = Column(Float)
    closeness = Column(Float)

    congressperson = relationship("CongressPerson", backref="Statistics")
    network = relationship("Network", backref="Statistics")


class NetworkStatistics(Base):
    id = Column(Integer, primary_key=True)
    network_id = Column(Integer, ForeignKey("Network.id"))

    total_bills = Column(Integer)

    number_of_nodes = Column(Integer)
    number_of_edges = Column(Integer)
    average_degree = Column(Float)
    density = Column(Float)
    connected_components = Column(Integer)
    largest_cc_rel_size = Column(Float)
    global_clustering = Column(Float)
    avg_clustering = Column(Float)
    diameter = Column(Integer)

    party = Column(String)
    state = Column(String)
    education = Column(String)
    gender = Column(String)
    region = Column(String)
    occupation = Column(String)
    ethnicity = Column(String)

    network = relationship("Network", backref="Statistics")
