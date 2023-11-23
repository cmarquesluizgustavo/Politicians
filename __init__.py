from src.miners.propositions import ProposalsMiner
import asyncio

proposicoes = ProposalsMiner()

a = asyncio.run(proposicoes.get_proposals())
