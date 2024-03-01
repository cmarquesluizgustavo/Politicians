import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from src.models import (
    CongressPerson,
    Term,
    Bill,
    Authorship,
    Network,
    Statistics,
)


def add_congresspeople_to_db(congresspeople_df: pd.DataFrame, session: Session):
    already_added_ids = []
    for index, row in congresspeople_df.iterrows():
        if not row["id"] in already_added_ids:
            already_added_ids.append(row["id"])
            congressperson = CongressPerson(
                id=row["id"],
                name=row["nomeCivil"],
                state=row["ufNascimento"],
                education=row["education"],
                birth_date=row["dataNascimento"],
                sex=row["sexo"],
                cpf=row["cpf"],
                social_network=row["redeSocial"],
                occupation=row["occupation"],
                ethnicity=row["ethnicity"],
            )
            session.add(congressperson)
    session.commit()


def add_networks_to_db(networks_df: pd.DataFrame, session: Session):
    for index, row in networks_df.iterrows():
        network = Network(id=row["period"], type=row["type"])
        session.add(network)
    session.commit()


def add_terms_to_db(congresspeople_df: pd.DataFrame, session: Session):
    for index, row in congresspeople_df.iterrows():
        term = Term(
            congressperson_id=row["id"],
            network_id=row["idLegislatura"],
            state=row["siglaUf"],
            party=row["siglaPartido"],
            region=row["region"],
            age_group=row["age_group"],
        )
        session.add(term)
        session.commit()


def add_bills_to_db(proposals_df: pd.DataFrame, session: Session):
    for index, row in proposals_df.iterrows():
        bill = Bill(
            id=row["id"],
            status_id=row["ultimoStatus_idSituacao"],
            type=row["siglaTipo"],
            year=row["ano"],
            description=row["ementa"],
            keywords=row["keywords"],
        )
        session.add(bill)
    session.commit()


def add_authorship_to_db(authors_df: pd.DataFrame, session: Session):
    for index, row in authors_df.iterrows():
        authorship = Authorship(
            bill_id=row["idProposicao"], congressperson_id=row["id"]
        )
        session.add(authorship)
    session.commit()


def add_statistics_to_db(statistics_df: pd.DataFrame, session: Session):
    for index, row in statistics_df.iterrows():
        statistics = Statistics(
            type=row["type"],
            value=row["value"],
            label=row["label"],
            network_id=row["network_id"],
            congressperson_id=row["congressperson_id"],
        )
        session.add(statistics)
    session.commit()


def connect_to_db(DATABASE_URL: str):
    # Create a connection to the database
    engine = create_engine(DATABASE_URL)

    # Create a session to interact with the database
    db_session = sessionmaker(bind=engine)
    session = db_session()
    return session
