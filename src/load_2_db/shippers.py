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
    StatisticsTypeLabel,
)


def add_congresspeople_to_db(congresspeople_df: pd.DataFrame, session: Session):
    already_added_ids = []
    counter = 0
    for index, row in congresspeople_df.iterrows():
        if not row["id"] in already_added_ids:
            counter += 1
            already_added_ids.append(row["id"])
            congressperson = CongressPerson(
                id=row["id"],
                name=row["nomeCivil"],
                birth_state=row["ufNascimento"],
                education=row["education"],
                birth_date=row["dataNascimento"],
                sex=row["gender"],
                cpf=row["cpf"],
                social_network=row["redeSocial"],
                occupation=row["occupation"],
                ethnicity=row["ethnicity"],
            )
            session.add(congressperson)
            if counter % 100 == 0:
                session.commit()            
    session.commit()

def add_networks_to_db(networks_df: pd.DataFrame, session: Session):
    for index, row in networks_df[["period", "type"]].drop_duplicates().iterrows():
        network = Network(id=row["period"], type=row["type"])
        session.add(network)
    session.commit()


def add_terms_to_db(congresspeople_df: pd.DataFrame, session: Session):
    counter = 0
    for index, row in congresspeople_df.iterrows():
        counter += 1
        term = Term(
            congressperson_id=row["id"],
            network_id=row["idLegislatura"],
            state=row["siglaUf"],
            party=row["siglaPartido"],
            region=row["region"],
            age_group=row["age_group"],
        )
        session.add(term)
        if counter % 100 == 0:
            session.commit()
    session.commit()


def add_bills_to_db(proposals_df: pd.DataFrame, session: Session):
    counter = 0
    for index, row in proposals_df.iterrows():
        counter += 1
        bill = Bill(
            id=row["id"],
            status_id=row["ultimoStatus_idSituacao"],
            type=row["siglaTipo"],
            year=row["ano"],
            description=row["ementa"],
            keywords=row["keywords"],
        )
        session.add(bill)
        if counter % 100 == 0:
            session.commit()
    session.commit()


def add_authorship_to_db(authors_df: pd.DataFrame, session: Session):
    for index, row in authors_df.iterrows():
        authorship = Authorship(
            bill_id=row["idProposicao"], congressperson_id=row["id"]
        )
        session.add(authorship)
    session.commit()


def add_type_and_label_to_db(statistics_df: pd.DataFrame, session: Session):
    counter = 0
    for index, row in (
        statistics_df[["type", "label", "feature"]].drop_duplicates().iterrows()
    ):
        counter += 1
        type_label = StatisticsTypeLabel(
            type=row["type"], label=row["label"], feature=row["feature"]
        )
        session.add(type_label)
        if counter % 100 == 0:
            session.commit()
    session.commit()


def get_all_type_labels(session: Session):
    return session.query(StatisticsTypeLabel).all()


def add_statistics_to_db(statistics_df: pd.DataFrame, session: Session):
    types_labels = get_all_type_labels(session)
    types_labels_df = pd.DataFrame(
        [
            (type_label.id, type_label.type, type_label.label)
            for type_label in types_labels
        ],
        columns=["type_label_id", "type", "label"],
    )
    statistics_df = statistics_df.merge(types_labels_df, on=["type", "label"])
    counter = 0
    for index, row in statistics_df.iterrows():
        counter += 1
        statistics = Statistics(
            network_id=row["network_id"],
            congressperson_id=row["congressperson_id"],
            type_label_id=row["type_label_id"],
            value=row["value"],
        )
        session.add(statistics)
        if counter % 100 == 0:
            session.commit()
    session.commit()


def connect_to_db(DATABASE_URL: str):
    # Create a connection to the database
    engine = create_engine(DATABASE_URL)

    # Create a session to interact with the database
    db_session = sessionmaker(bind=engine)
    session = db_session()
    return session
