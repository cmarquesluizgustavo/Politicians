import pandas as pd
import dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.models import CongressPerson, Term, Bill, Authorship
from src.network_builder.pre_processing import pre_processing


CONGRESSPEOPLE_PATH = "data/miners/enriched_congresspeople.csv"
PROPOSALS_PATH = "data/miners/proposals/proposals.csv"
AUTHORS_PATH = "data/miners/authors/authors.csv"

congresspeople_df = pd.read_csv(CONGRESSPEOPLE_PATH)
proposlas_df = pd.read_csv(PROPOSALS_PATH)
authors_df = pd.read_csv(AUTHORS_PATH)

congresspeople_df = pre_processing(congresspeople_df)
authors_df = authors_df[authors_df["type"] == "deputados"]
authors_df = authors_df[authors_df["idProposicao"].isin(proposlas_df["id"])]
authors_df = authors_df[authors_df["id"].isin(congresspeople_df["id"])]


DATABASE_URL = dotenv.get_key(dotenv.find_dotenv(), "DATABASE_URL")
if DATABASE_URL is None:
    raise ValueError("DATABASE_URL not found in .env file.")

# Create a connection to the database
engine = create_engine(DATABASE_URL)

# Create a session to interact with the database
DBSession = sessionmaker(bind=engine)
session = DBSession()


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


for index, row in congresspeople_df.iterrows():
    term = Term(
        congressperson_id=row["id"],
        state=row["siglaUf"],
        party=row["siglaPartido"],
        legislature_id=row["idLegislatura"],
        election_year=row["election_year"],
    )
    session.add(term)
    session.commit()


for index, row in proposlas_df.iterrows():
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


for index, row in authors_df.iterrows():
    authorship = Authorship(bill_id=row["idProposicao"], congressperson_id=row["id"])
    session.add(authorship)
session.commit()

# Close the session
session.close()