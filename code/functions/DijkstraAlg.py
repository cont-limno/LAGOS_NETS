import heapq
import warnings

class Dijkstra:
    """Dijkstra's algorithm.
    The algorithm stops if one lake has been visited (nearest lake found)."""
    def __init__(self, graph, vertices, LAGOSName, savepath):
        self.graph = graph
        self.V = vertices
        self.LAGOSName = LAGOSName
        self.savepath = savepath

    def CalPath(self, parent, target):
        curr = target
        path = []
        FLAGLS = []
        while curr in parent:
            path = [curr] + path
            FLAGLS.append(int(curr.find(self.LAGOSName) != -1))
            curr = parent[curr]
        FLAGLS.append(int(curr.find(self.LAGOSName) != -1))
        path = [curr] + path
        distUp = 0.0
        distDown = 0.0
        UpDown = [None, 0]#  {k: [None, 0] for k in path}
        for i in range(1, len(path)):  # neighbor = i, u = i-1
            if self.graph[path[i-1]][path[i]][1] == 'Upstream':
                distUp += self.graph[path[i-1]][path[i]][0]
                if UpDown[0] == 'Down':
                    distUp += UpDown[1]
                UpDown = ['Up', self.graph[path[i-1]][path[i]][0]]
            elif self.graph[path[i-1]][path[i]][1] == 'Downstream':
                distDown += self.graph[path[i-1]][path[i]][0]
                if UpDown[0] == 'Up':
                    distDown += UpDown[1]
                UpDown = ['Down', self.graph[path[i-1]][path[i]][0]]
        if sum(FLAGLS) > 2:
            warnings.warn('More than two lakes included')
            print(FLAGLS)
        return path, distUp, distDown

    def traversal(self, source):
        connections = []
        queue = []
        dist = {k: float("inf") for k in self.V}
        parent = {}#{k: None for k in self.V}
        dist[source] = 0.0
        processedSet = set()#{k: False for k in self.V}
        heapq.heappush(queue, (dist[source], source))

        while queue:
            # Pick the minimum distance vertex from
            # the set of vertices not yet processed.
            # u is always equal to source in first iteration
            (distant, u) = heapq.heappop(queue)
            if distant == float("inf"):
                print('Distance is Inf. '
                      'All connected vertices has been visited.')
                break
            if u not in processedSet:
                # Put the minimum distance vertex in the
                # shotest path tree
                processedSet.add(u)
                if u.find(self.LAGOSName) != -1 and u != source:
                    path, distUp, distDown = self.CalPath(parent, u)
                    if self.savepath:
                        connections.append([source.replace(self.LAGOSName, ''),
                                      u.replace(self.LAGOSName, ''), distant,
                                            distUp, distDown, path])
                    else:
                        connections.append([source.replace(self.LAGOSName, ''),
                                            u.replace(self.LAGOSName, ''),
                                            distant, distUp, distDown])
                # Update dist value of the adjacent vertices
                # of the picked vertex only if the current
                # distance is greater than new distance and
                # the vertex in not in the shotest path tree
                else:
                    # a flag to check if two lakes are connected through other
                    # lakes. If yes, remove this connection.
                    for neighbor in self.graph[u]:
                        if self.graph[u][neighbor][0] < 0:
                            print("Warning: edge weight is not positive. "
                                          "Weight is %4f" % self.graph[u][neighbor][0])
                        else:
                            if dist[neighbor] > dist[u] + self.graph[u][neighbor][0]:
                                dist[neighbor] = dist[u] + self.graph[u][neighbor][0]
                                parent[neighbor] = u
                                heapq.heappush(queue, (dist[neighbor], neighbor))
        return connections
