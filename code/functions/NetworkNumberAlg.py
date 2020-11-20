#  checked by test cases
class Network:
    def __init__(self, pairs, lakes_start):
        self.pairs = pairs
        self.lakes_start = lakes_start  # lakes that do not have upstream lakes

    def CalNumber(self):
        """
        start from the lakes that do not have upstream lakes and do BFS search
        """
        vertices = set(list(self.pairs.keys()))
        for i in self.pairs:
            vertices = vertices | set(self.pairs[i])
        # network number intialization
        NetNumber = {}
        for i in self.lakes_start:
            NetNumber[i] = 1
        #BFS
        for i in self.lakes_start:
            queue = [i]
            while queue:
                item = queue.pop(0)
                if item in self.pairs:  #  leaf lakes are not keys in pairs
                    for neighbor in self.pairs[item]:
                        if neighbor in NetNumber:
                            if NetNumber[neighbor] < 1 + NetNumber[item]:
                                NetNumber[neighbor] = 1 + NetNumber[item]
                                queue.append(neighbor)
                        else:
                            NetNumber[neighbor] = 1 + NetNumber[item]
                            queue.append(neighbor)
        #  check is all lakes have number
        if len(NetNumber) != len(vertices):
            print('NetNumber length is %d. Total vertices is %d.'
                  %(len(NetNumber), len(vertices)))
            raise ValueError('Some lakes do not have network number.')
        return NetNumber






