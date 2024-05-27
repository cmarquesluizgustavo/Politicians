import pandas
import networkx as nx
import pickle
import matplotlib.pyplot as plt
import numpy as np


# read network-sauce/data/network_builder/56.pkl into a networkx graph
with open('network-sauce/data/network_builder/56.pkl', 'rb') as f:
    G = pickle.load(f)

# make a visualization of the network. The thickness of the edges is proportional to the number of interactions between the two nodes
# the size of the nodes is proportional to the number of interactions that node has
# the color is the party of the node
interactions_edges = np.array(list(nx.get_edge_attributes(G, 'weight').values()))

# Get the number of interactions for each node
interactions_nodes = np.array([d for n, d in G.degree()])

# Get the party of each node (converted to a numeric scale for color mapping)
party = np.array(list(nx.get_node_attributes(G, 'siglaPartido').values()))

# Get the position of each node
pos = nx.spring_layout(G)

# Normalize edge widths for better visualization
interactions_edges_normalized = 1 + 4 * (interactions_edges - interactions_edges.min()) / (interactions_edges.max() - interactions_edges.min())

# Normalize node sizes for better visualization
interactions_nodes_normalized = 10 + 90 * (interactions_nodes - interactions_nodes.min()) / (interactions_nodes.max() - interactions_nodes.min())

# Draw the network
plt.figure(figsize=(10, 10))
nx.draw_networkx_nodes(G, pos, node_size=interactions_nodes_normalized, node_color='white', edgecolors='black', linewidths=0.5, node_shape="2"
)
nx.draw_networkx_edges(G, pos, width=interactions_edges_normalized, edge_color='black', alpha=0.7)

# Remove axis
plt.axis('off')

# Show the plot
plt.show()