# Description: This file contains the implementation of weighted similarity algorithms.
import networkx as nx
from networkx.utils import not_implemented_for
from math import log


def add_weight_log(g, weight):
    """
    Add the weight log to the graph
    """
    weight_log = "1_over_weight_log"
    for node in g.nodes:
        degree_or_n_neighbors = g.degree(node, weight=weight)
        # If the node has no neighbors, we skip it
        # it will always be more than 2 neighbors for adamic adar
        # as it considers common neighbors only
        if degree_or_n_neighbors < 2:
            continue
        g.nodes[node][weight_log] = 1 / log(degree_or_n_neighbors)

    return g, weight_log

def _apply_prediction(G, func, weight, ebunch=None):
    """Applies the given function to each edge in the specified iterable
    of edges.

    `G` is an instance of :class:`networkx.Graph`.

    `func` is a function on two inputs, each of which is a node in the
    graph. The function can return anything, but it should return a
    value representing a prediction of the likelihood of a "link"
    joining the two nodes.

    `ebunch` is an iterable of pairs of nodes. If not specified, all
    non-edges in the graph `G` will be used.

    """
    if ebunch is None:
        ebunch = nx.non_edges(G)
    return ((u, v, func(u, v, weight)) for u, v in ebunch)

def common_neighbors(G, u, v):
    return set(G[u]) & set(G[v])

@not_implemented_for("directed")
@not_implemented_for("multigraph")
def weighted_adamic_adar_index(G, weight="weight", ebunch=None):
    r"""Compute the weighted Adamic-Adar index of all node pairs in ebunch.

    Adamic-Adar index of `u` and `v` is defined as

    .. math::

        \sum_{w \in \Gamma(u) \cap \Gamma(v)} \frac{1}{\log |\Gamma(w)|}

    where $\Gamma(u)$ denotes the degree of node `u`.
    This index leads to zero-division for nodes only connected via self-loops or
    degree-zero nodes.
    It is intended to be used when no self-loops are present.

    Parameters
    ----------
    G : graph
        NetworkX undirected graph.
    weight : string
        The edge attribute that holds the numerical value used as weight.

    ebunch : iterable of node pairs, optional (default = None)
        Adamic-Adar index will be computed for each pair of nodes given
        in the iterable. The pairs must be given as 2-tuples (u, v)
        where u and v are nodes in the graph. If ebunch is None then all
        nonexistent edges in the graph will be used.
        Default value: None.

    Returns
    -------
    piter : iterator
        An iterator of 3-tuples in the form (u, v, p) where (u, v) is a
        pair of nodes and p is their Adamic-Adar index.
    """

    def predict(u, v, weight_log):
        return sum(
            G.nodes[w][weight_log] for w in common_neighbors(G, u, v)
        )

    G, weight_log = add_weight_log(G, weight)
    return _apply_prediction(G, predict, weight_log, ebunch)


@not_implemented_for("directed")
@not_implemented_for("multigraph")
def weighted_jaccard_coefficient(G, weight="weight", ebunch=None):
    r"""Compute the weighted Jaccard coefficient of all node pairs in ebunch.

    The weighted Jaccard coefficient of nodes `u` and `v` is defined as:

    .. math::

        \frac{\sum_{w \in \Gamma(u) \cap \Gamma(v)} \min(\text{weight}(u, w), \text{weight}(v, w))}{
              \sum_{w \in \Gamma(u) \cup \Gamma(v)} \max(\text{weight}(u, w), \text{weight}(v, w))}

    where $\Gamma(u)$ denotes the set of neighbors of $u$.

    Parameters
    ----------
    G : graph
        A NetworkX undirected graph.

    weight : string, optional (default="weight")
        The edge attribute that holds the numerical value used as a weight.

    ebunch : iterable of node pairs, optional (default=None)
        Weighted Jaccard coefficient will be computed for each pair of nodes
        given in the iterable. The pairs must be given as 2-tuples
        (u, v) where u and v are nodes in the graph. If ebunch is None
        then all nonexistent edges in the graph will be used.
        Default value: None.

    Returns
    -------
    piter : iterator
        An iterator of 3-tuples in the form (u, v, p) where (u, v) is a
        pair of nodes and p is their weighted Jaccard coefficient.

    Examples
    --------
    >>> G = nx.Graph()
    >>> G.add_edge(0, 1, weight=0.5)
    >>> G.add_edge(1, 2, weight=0.7)
    >>> G.add_edge(2, 3, weight=1.0)
    >>> preds = nx.weighted_jaccard_coefficient(G, [(0, 1), (1, 2)])
    >>> for u, v, p in preds:
    ...     print(f"({u}, {v}) -> {p:.8f}")
    (0, 1) -> 0.50000000
    (1, 2) -> 0.41176471
    """

    def predict(u, v, weight):
        neighbors_u = set(G[u])
        neighbors_v = set(G[v])
        common_neighbors = neighbors_u & neighbors_v
        all_neighbors = neighbors_u | neighbors_v

        if not all_neighbors:
            return 0
        if not common_neighbors:
            return 0

        numerator = sum(min(G[v][w][weight], G[u][w][weight]) for w in common_neighbors)
        denominator = sum(
            max(G[v].get(w, {weight: 0})[weight], G[u].get(w, {weight: 0})[weight])
            for w in all_neighbors
        )

        return numerator / denominator

    return _apply_prediction(G, predict, weight, ebunch)


def adamic_adar_index(G, ebunch=None):
    return weighted_adamic_adar_index(G, weight=None, ebunch=ebunch)

def jaccard_coefficient(G, ebunch=None):
    def predict(u, v, weight):
        neighbors_u = set(G[u])
        neighbors_v = set(G[v])
        common_neighbors = neighbors_u & neighbors_v
        all_neighbors = neighbors_u | neighbors_v

        if not all_neighbors:
            return 0
        if not common_neighbors:
            return 0

        numerator = len(common_neighbors)
        denominator = len(all_neighbors)

        return numerator / denominator

    return _apply_prediction(G, predict, None, ebunch)