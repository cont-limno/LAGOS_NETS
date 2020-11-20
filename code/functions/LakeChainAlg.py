class LakeChain:
    def __init__(self, pairs, vertices):
        self.pairs = pairs
        self.vertices = vertices
        self.visited = {k: False for k in self.vertices}

    def DFS(self, v):
        chain = set()
        queue = []
        queue.append(v)
        while queue:
            curr = queue.pop()
            chain.add(curr)
            if curr in self.pairs:
                for i in self.pairs[curr]:
                    if i not in chain:
                        queue.append(i)
        # set all nodes to be visted and check if the nodes are in other chains or not
        chain_list = []
        for i in chain:
            if self.visited[i]:
                print('%s has been visted in other chain!' % i)
            else:
                self.visited[i] = True
                chain_list.append(i)
        return chain_list

    def FindChain(self):
        ChainAll = []
        for v in self.vertices:
            if not self.visited[v]:
                ChainAll.append(self.DFS(v))
        return ChainAll
