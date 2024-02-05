import logging
import datetime


def getUfRegion(uf):
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


def getoccupation(occupation):
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


def getEductionLevel(education):
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
        "ENSINO MÉDIO": "high school",
        "ENSINO MÉDIO COMPLETO": "high school",
        "MÉDIO COMPLETO": "high school",
        "SECUNDÁRIO": "high school",
        "ENSINO FUNDAMENTAL": "elementary school",
        "ENSINO FUNDAMENTAL COMPLETO": "elementary school",
        "FUNDAMENTAL COMPLETO": "elementary school",
        "PRIMÁRIO": "elementary school",
        "SECUNDÁRIO INCOMPLETO": "elementary school",
        "ENSINO TÉCNICO": "high school",
        "GINASIAL": "high school",
        "ENSINO MÉDIO INCOMPLETO": "elementary school",
        "MÉDIO INCOMPLETO": "elementary school",
        "PRIMÁRIO INCOMPLETO": "no education",
        "ENSINO FUNDAMENTAL INCOMPLETO": "no education",
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


def getMaritalStatus(marital_status):
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


def getAge(birthdate):
    birthdate = datetime.datetime.strptime(birthdate, "%Y-%m-%d")
    age = datetime.datetime.now() - birthdate
    age = int(age.days / 365)
    return age
