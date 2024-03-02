import datetime
import logging
import pandas as pd


def pre_processing(data: pd.DataFrame) -> pd.DataFrame:
    """
    This function is used to treat the data before it is used to create the network.

    Args:
        data (pd.DataFrame): The data that will be treated.

    Returns:
        pd.DataFrame: The treated data.
    """
    data["occupation"] = data["occupation"].apply(get_occupation)
    data["education"] = data["education"].apply(get_eduction_level)
    data["marital_status"] = data["marital_status"].apply(get_marital_status)
    data["age"] = data.apply(
        lambda x: get_age(x["dataNascimento"], x["election_year"]), axis=1
    )
    data["age_group"] = pd.cut(
        data["age"],
        bins=[0, 30, 40, 50, 60, 70, 80, 90, 100],
        labels=["0-30", "30-40", "40-50", "50-60", "60-70", "70-80", "80-90", "90-100"],
    )
    data["region"] = data["siglaUf"].apply(get_uf_region)
    data["gender"] = data["sexo"]

    data["ethnicity"] = data["ethnicity"].apply(
        lambda x: (
            pd.NA
            if (pd.isna(x) or x == "NÃO INFORMADO" or x == "NÃO DIVULGÁVEL")
            else x
        )
    )

    data.sort_values(by=["id", "election_year"], inplace=True)
    data["ethnicity"] = data.groupby("id").ethnicity.bfill()
    data["ethnicity"] = data.groupby("id").ethnicity.ffill()

    data["education"] = data.groupby("id").education.bfill()
    data["education"] = data.groupby("id").education.ffill()

    return data[
        [
            "id",
            "idLegislatura",
            "election_year",
            "nomeEleitoral",
            "education",
            "gender",
            "siglaUf",
            "siglaPartido",
            "region",
            "occupation",
            "ethnicity",
            "age_group",
        ]
    ]


def get_uf_region(uf):
    regions_dict = {
        "AM": "Norte",
        "RR": "Norte",
        "AP": "Norte",
        "PA": "Norte",
        "TO": "Norte",
        "RO": "Norte",
        "AC": "Norte",
        "MA": "Nordeste",
        "PI": "Nordeste",
        "CE": "Nordeste",
        "RN": "Nordeste",
        "PE": "Nordeste",
        "PB": "Nordeste",
        "SE": "Nordeste",
        "AL": "Nordeste",
        "BA": "Nordeste",
        "MT": "Centro-Oeste",
        "MS": "Centro-Oeste",
        "GO": "Centro-Oeste",
        "DF": "Distrito Federal",
        "SP": "Sudeste",
        "RJ": "Sudeste",
        "ES": "Sudeste",
        "MG": "Sudeste",
        "PR": "Sul",
        "RS": "Sul",
        "SC": "Sul",
    }
    return regions_dict[uf]


def get_occupation(occupation):
    if pd.isna(occupation):
        return "other"
    occupation = occupation.upper()
    translation_table = {
        "DEPUTADO": "politician",
        "SENADOR": "politician",
        "VEREADOR": "politician",
        "SENADOR, DEPUTADO E VEREADOR": "politician",
        "PRESIDENTE DA REPUBLICA, MINISTRO DE ESTADO, GOVERNADOR E PREFEITO": "politician",
        "OCUPANTE DE CARGO EM COMISSÃO": "politician",
        "ENGENHEIRO": "engineer/architect",
        "ARQUITETO": "engineer/architect",
        "SERVIDOR PUBLICO FEDERAL": "public servant",
        "SERVIDOR PÚBLICO FEDERAL": "public servant",
        "SERVIDOR PUBLICO ESTADUAL": "public servant",
        "SERVIDOR PÚBLICO ESTADUAL": "public servant",
        "SERVIDOR PUBLICO MUNICIPAL": "public servant",
        "SERVIDOR PÚBLICO MUNICIPAL": "public servant",
        "SERVIDOR PUBLICO CIVIL APOSENTADO": "public servant",
        "SERVIDOR PÚBLICO CIVIL APOSENTADO": "public servant",
        "TABELIÃO": "public servant",
        "FISCAL": "public servant",
        "MEMBRO DAS FORÇAS ARMADAS": "military",
        "MILITAR REFORMADO": "military",
        "BOMBEIRO MILITAR": "military",
        "POLICIAL MILITAR": "military",
        "POLICIAL CIVIL": "military",
        "PROFESSOR DE ENSINO SUPERIOR": "university professor",
        "PROFESSOR E INSTRUTOR DE FORMAÇÃO PROFISSIONAL": "teacher",
        "PROFESSOR E INSTRUTOR DE FORMACAO PROFISSIONAL": "teacher",
        "PROFESSOR DE ENSINO DE PRIMEIRO E SEGUNDO GRAUS": "teacher",
        "PROFESSOR DE ENSINO FUNDAMENTAL": "teacher",
        "PROFESSOR DE ENSINO MÉDIO": "teacher",
        "PEDAGOGO": "teacher",
        "ESTUDANTE, BOLSISTA, ESTAGIÁRIO E ASSEMELHADOS": "student",
        "ESTUDANTE, BOLSISTA, ESTAGIARIO E ASSEMELHADOS": "student",
        "ESTUDANTE,BOLSISTA, ESTAGIARIO E ASSEMELHADOS": "student",
        "MOTORISTA PARTICULAR": "driver",
        "MOTORISTA DE VEÍCULOS DE TRANSPORTE COLETIVO DE PASSAGEIROS": "driver",
        "OPERADOR DE EQUIPAMENTO DE RÁDIO, TELEVISÃO, SOM E CINEMA": "media worker",
        "COMUNICÓLOGO": "media worker",
        "COMUNICOLOGO": "media worker",
        "LOCUTOR E COMENTARISTA DE RÁDIO E TELEVISÃO E RADIALISTA": "media worker",
        "LOCUTOR E COMENTARISTA DE RADIO E TELEVISÃO E RADIALISTA": "media worker",
        "LOCUTOR E COMENTARISTA DE RADIO E TELEVISAO E RADIALISTA": "media worker",
        "FOTÓGRAFO E ASSEMELHADOS": "media worker",
        "TRABALHADOR DE ARTES GRAFICAS": "media worker",
        "TRABALHADOR DE ARTES GRÁFICAS": "media worker",
        "JORNALISTA E REDATOR": "journalist",
        "PUBLICITÁRIO": "publicist",
        "ATOR E DIRETOR DE ESPETÁCULOS PÚBLICOS": "actor/musician",
        "MÚSICO": "actor/musician",
        "CANTOR E COMPOSITOR": "actor/musician",
        "PECUARISTA": "farmer",
        "AGRICULTOR": "farmer",
        "PRODUTOR AGROPECUARIO": "farmer",
        "PRODUTOR AGROPECUÁRIO": "farmer",
        "PESCADOR": "farmer",
        "TECNICO CONTABILIDADE, ESTATISTICA, ECONOMIA DOMESTICA E ADMINISTRACAO": "accountant",
        "TÉCNICO CONTABILIDADE, ESTATÍSTICA, ECONOMIA DOMÉSTICA E ADMINISTRAÇÃO": "accountant",
        "CONTADOR": "accountant",
        "VENDEDOR DE COMÉRCIO VAREJISTA E ATACADISTA": "salesman",
        "COMERCIANTE": "salesman",
        "REPRESENTANTE COMERCIAL": "salesman",
        "COMERCIÁRIO": "salesman",
        "INDUSTRIAL": "blue collar",
        "OPERADOR DE APARELHOS DE PRODUÇÃO INDUSTRIAL": "blue collar",
        "FERROVIÁRIO": "blue collar",
        "FERROVIARIO": "blue collar",
        "TRABALHADOR METALÚRGICO E SIDERÚRGICO": "blue collar",
        "TRABALHADOR METALURGICO E SIDERURGICO": "blue collar",
        "TECNICO DE ELETRICIDADE, ELETRONICA E TELECOMUNICACOES": "blue collar",
        "ENCANADOR,SOLDADOR,CHAPEADOR,CALDEIREIRO,MONTADOR DE ESTRUTURA METALIC": "blue collar",
        "TORNEIRO MECÂNICO": "blue collar",
        "TÉCNICO EM EDIFICAÇÕES": "blue collar",
        "VETERINÁRIO": "veterinarian/agronomist",
        "VETERINARIO": "veterinarian/agronomist",
        "ZOOTECNISTA": "veterinarian/agronomist",
        "AGRONOMO": "veterinarian/agronomist",
        "AGRÔNOMO": "veterinarian/agronomist",
        "TÉCNICO EM AGRONOMIA E AGRIMENSURA": "veterinarian/agronomist/",
        "OPERADOR DE IMPLEMENTO DE AGRICULTURA, PECUARIA E EXPLORACAO FLORESTAL": "veterinarian/agronomist",
        "ODONTOLOGO": "dentist/medic",
        "ODONTÓLOGO": "dentist/medic",
        "MÉDICO": "dentist/medic",
        "MEDICO": "dentist/medic",
        "ADVOGADO": "judge/lawyer/prosecutor",
        "BIOMÉDICO": "biologist",
        "BIOLOGO E BIOMEDICO": "biologist",
        "BIÓLOGO": "biologist",
        "PSICOLOGO": "health professional",
        "PSICÓLOGO": "health professional",
        "ASSISTENTE SOCIAL": "health professional",
        "ENFERMEIRO": "health professional",
        "FISIOTERAPEUTA E TERAPEUTA OCUPACIONAL": "health professional",
        "FARMACEUTICO": "health professional",
        "FARMACÊUTICO": "health professional",
        "SOCIOLOGO": "social scientist/historian",
        "SOCIÓLOGO": "social scientist/historian",
        "CIENTISTA POLÍTICO": "social scientist/historian",
        "HISTORIADOR": "social scientist/historian",
        "CORRETOR DE IMÓVEIS, SEGUROS, TÍTULOS E VALORES": "broker/economist/banker",
        "ECONOMISTA": "broker/economist/banker",
        "BANCARIO E ECONOMIARIO": "broker/economist/banker",
        "BANCÁRIO E ECONOMIÁRIO": "broker/economist/banker",
        "LEILOEIRO, AVALIADOR E ASSEMELHADOS": "broker/economist/banker",
        "SECURITARIO": "broker/economist/banker",
        "ADMINISTRADOR": "businessperson/entrepreneur",
        "EMPRESARIO": "businessperson/entrepreneur",
        "EMPRESÁRIO": "businessperson/entrepreneur",
        "GERENTE": "businessperson/entrepreneur",
        "DIRETOR DE EMPRESAS": "businessperson/entrepreneur",
        "ATLETA PROFISSIONAL E TECNICO EM DESPORTOS": "athlete",
        "ATLETA PROFISSIONAL E TÉCNICO EM DESPORTOS": "athlete",
        "SACERDOTE OU MEMBRO DE ORDEM OU SEITA RELIGIOSA": "religious",
        "DIPLOMATA": "diplomat",
        "RELAÇÕES-PÚBLICAS": "public relations",
        "MAGISTRADO": "judge/lawyer/prosecutor",
        "MEMBRO DO MINISTERIO PUBLICO": "judge/lawyer/prosecutor",
        "MEMBRO DO MINISTÉRIO PÚBLICO": "judge/lawyer/prosecutor",
        "GEÓLOGO": "geologist",
        "GEÓGRAFO": "geologist",
        "DONA DE CASA": "houseperson/retired",
        "APOSENTADO (EXCETO SERVIDOR PUBLICO)": "houseperson/retired",
        "APOSENTADO (EXCETO SERVIDOR PÚBLICO)": "houseperson/retired",
        "ESCRITOR E CRÍTICO": "writer",
        "ANALISTA DE SISTEMAS": "computer scientist",
        "OTHER": "other",
        "OUTROS": "other",
        "NÃO INFORMADA": "other",
    }

    if occupation in translation_table:
        return translation_table[occupation].lower()
    else:
        return "other"


def get_eduction_level(education):
    if pd.isna(education):
        return "other"
    education = education.upper()
    translation_table = {
        "DOUTORADO": "graduate",
        "MESTRADO": "graduate",
        "DOUTORADO INCOMPLETO": "graduate",
        "MESTRADO INCOMPLETO": "undergraduate",
        "PÓS-GRADUAÇÃO": "graduate",
        "SUPERIOR COMPLETO": "undergraduate",
        "SUPERIOR": "undergraduate",
        "SUPERIOR INCOMPLETO": "high school",
        "2º GRAU COMPLETO": "high school",
        "ENSINO MÉDIO": "high school",
        "ENSINO MÉDIO COMPLETO": "high school",
        "MÉDIO COMPLETO": "high school",
        "SECUNDÁRIO": "high school",
        "ENSINO FUNDAMENTAL": "elementary school",
        "ENSINO FUNDAMENTAL COMPLETO": "elementary school",
        "1º GRAU COMPLETO": "elementary school",
        "FUNDAMENTAL COMPLETO": "elementary school",
        "PRIMÁRIO": "elementary school",
        "SECUNDÁRIO INCOMPLETO": "elementary school",
        "ENSINO TÉCNICO": "high school",
        "GINASIAL": "high school",
        "2º GRAU INCOMPLETO": "elementary school",
        "ENSINO MÉDIO INCOMPLETO": "elementary school",
        "MÉDIO INCOMPLETO": "elementary school",
        "PRIMÁRIO INCOMPLETO": "no education",
        "ENSINO FUNDAMENTAL INCOMPLETO": "no education",
        "1º GRAU INCOMPLETO": "no education",
        "FUNDAMENTAL INCOMPLETO": "no education",
        "LÊ E ESCREVE": "no education",
        "NÃO INFORMADO": "other",
        "OTHER": "other",
    }
    if education in translation_table:
        return translation_table[education]
    else:
        logging.warning("Education not found: %s", education)
        return education


def get_marital_status(marital_status):
    if pd.isna(marital_status):
        return "other"
    marital_status = marital_status.upper()
    translation_table = {
        "CASADO(A)": "married",
        "SOLTEIRO(A)": "single",
        "DIVORCIADO(A)": "divorced",
        "SEPARADO(A) JUDICIALMENTE": "divorced",
        "VIÚVO(A)": "widowed",
        "OUTROS": "other",
        "NÃO INFORMADO": "other",
        "OTHER": "other",
    }
    if marital_status in translation_table:
        return translation_table[marital_status]
    else:
        logging.warning("Marital status not found: %s", marital_status)
        return marital_status


def get_age(birthdate, election_year):
    birthdate = datetime.datetime.strptime(birthdate, "%Y-%m-%d")
    election_year = int(election_year)
    age = election_year - birthdate.year
    return age


if __name__ == "__main__":
    congresspeople = pd.read_csv("data/enriched_congresspeople.csv")
    congresspeople_processed = pre_processing(congresspeople)
    congresspeople_processed.to_csv("data/data_preprocessed.csv", index=False)
