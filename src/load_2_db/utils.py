import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from src.models import (
    CongressPerson,
    Term,
    Bill,
    Authorship,
    Network,
    CongressPersonStatistics,
    NetworkStatistics,
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
                education=row["education_level"],
                birth_date=row["dataNascimento"],
                sex=row["sexo"],
                cpf=row["cpf"],
                social_network=row["redeSocial"],
                occupation=row["occupation"],
                ethnicity=row["ethnicity"],
            )
            session.add(congressperson)
    session.commit()


def add_terms_to_db(congresspeople_df: pd.DataFrame, session: Session):
    for index, row in congresspeople_df.iterrows():
        term = Term(
            congressperson_id=row["id"],
            state=row["siglaUf"],
            party=row["siglaPartido"],
            network_id=row["idLegislatura"],
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


def add_networks_to_db(networks: dict, session: Session):
    for network_id, network_type in networks.items():
        network = Network(id=network_id, type=network_type)
        session.add(network)
    session.commit()


def add_congressperson_statistics_to_db(
    congressperson_statistics_df: pd.DataFrame, session: Session
):
    for index, row in congressperson_statistics_df.iterrows():
        congressperson_statistics = CongressPersonStatistics(
            congressperson_id=row["id"],
            network_id=row["idLegislatura"],
            total_bills=row["total_bills"],
            party=row["siglaPartido"],
            state=row["state"],
            education=row["education"],
            gender=row["gender"],
            region=row["region"],
            occupation=row["occupation"],
            ethnicity=row["ethnicity"],
            degree=row["degree"],
            pagerank=row["pagerank"],
            betweenness=row["betweenness"],
            closeness=row["closeness"],
        )
        session.add(congressperson_statistics)
    session.commit()


def add_network_statistics_to_db(network_statistics_df: pd.DataFrame, session: Session):
    for index, row in network_statistics_df.iterrows():
        network_statistics = NetworkStatistics(
            network_id=row["idLegislatura"],
            total_bills=row["total_bills"],
            number_of_nodes=row["number_of_nodes"],
            number_of_edges=row["number_of_edges"],
            average_degree=row["average_degree"],
            density=row["density"],
            connected_components=row["connected_components"],
            largest_cc_rel_size=row["largest_cc_rel_size"],
            global_clustering=row["global_clustering"],
            avg_clustering=row["avg_clustering"],
            diameter=row["diameter"],
            party=row["party"],
            state=row["state"],
            education=row["education"],
            gender=row["gender"],
            region=row["region"],
            occupation=row["occupation"],
            ethnicity=row["ethnicity"],
        )
        session.add(network_statistics)
    session.commit()


def connect_to_db(DATABASE_URL: str):
    if DATABASE_URL is None:
        raise ValueError("DATABASE_URL not found in .env file.")

    # Create a connection to the database
    engine = create_engine(DATABASE_URL)

    # Create a session to interact with the database
    db_session = sessionmaker(bind=engine)
    session = db_session()
    return session
