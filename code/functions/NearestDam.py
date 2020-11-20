import heapq
"""Tested"""


class Dijkstra:
    """Dijkstra's algorithm.
    The algorithm stops if one lake has been visited (nearest lake found)."""
    def __init__(self, graph, vertices, COMID_dam, LAGOSName):
        self.graph = graph
        self.V = vertices
        self.LAGOSName = LAGOSName
        self.COMID_dam = COMID_dam

    def traversal(self, source):
        queue = []
        dist = {k: float("inf") for k in self.V}
        dist[source] = 0.0
        processedSet = set()#{k: False for k in self.V}
        heapq.heappush(queue, (dist[source], source))

        while queue:
            # Pick the minimum distance vertex from
            # the set of vertices not yet processed.
            # u is always equal to source in first iteration
            (distant, u) = heapq.heappop(queue)
            if distant == float("inf"):
                print('Future nodes are not connected.')
                return None
            if u not in processedSet:
                # Put the minimum distance vertex in the
                # shotest path tree
                processedSet.add(u)
                if u in self.COMID_dam:  # if u is a stream with dam, return
                    return dist[u], u
                if not u.find(self.LAGOSName) != -1 or u == source: # if a node is not a lake,
                                                       # traversal its neighbors
                    for neighbor in self.graph[u]:
                        if dist[neighbor] > dist[u] + self.graph[u][neighbor][0]:
                                dist[neighbor] = dist[u] + self.graph[u][neighbor][0]
                                heapq.heappush(queue, (dist[neighbor], neighbor))
        return None
