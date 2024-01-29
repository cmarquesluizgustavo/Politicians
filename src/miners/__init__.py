from congresspeople import CongressPeople
from authors import AuthorsMiner
from bills import BillsMiner
from TSE import TSEReportsMiner

congresspeople = CongressPeople()
authors = AuthorsMiner()
proposals = BillsMiner()
tse = TSEReportsMiner()

congresspeople.mine()
authors.mine()
proposals.mine()
tse.mine()