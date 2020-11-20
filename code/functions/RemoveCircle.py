# checked by test cases
class RmCircle:
    def __init__(self, pairs):
        self.pairs = pairs  # pairs:  {A:[1, 2], B: [A, 1]....} adgencency list
        self.vertices = set(list(pairs.keys()))

    def check(self, pairs):
        circle = {} #  a dictionary to store duplicated pairs
        visted = set()
        for v in self.vertices:
            if v not in visted:
                ancester = set()
                stack = [v]
                while stack:
                    curr = stack[-1]
                    ancester.add(curr)
                    visted.add(curr)
                    if curr in pairs: # need to remove item from ancester
                        neighbors = []
                        for ni in pairs[curr]:
                            if ni in ancester:
                                if curr not in circle:
                                    circle[curr] = set()
                                circle[curr].add(ni)
                            elif ni not in visted:
                                neighbors.append(ni)
                        if len(neighbors) == 0:
                            ancester.remove(curr)
                            stack.pop()
                        else:
                            stack.extend(neighbors)
                    else:
                        ancester.remove(curr)
                        stack.pop()
        return circle

    def remove(self):
        # check circles
        circles = self.check(self.pairs)
        # remove values from the dictionary
        for key in circles:
            for value in circles[key]:
                self.pairs[key].remove(value)
        # check if there is any circles
        circles_check = self.check(self.pairs)
        if len(circles_check) != 0:
            print('Error! Not all circles been removed.')
        return self.pairs, circles














