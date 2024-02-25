import pickle
from similarity_statistics import SimilarityAndGainsStatistics


if __name__ == "__main__":
    g = pickle.load(open("data/networks_builder/2023.pkl", "rb"))
    g.name = "2023"
    sgs = SimilarityAndGainsStatistics(
        g,
        target_features=[
            "siglaPartido",
            "siglaUf",
            "education",
            "gender",
            "region",
            "occupation",
            "ethnicity",
        ],
        similarity_algorithm="jaccard",
    )
