class BKTree:
    def __init__(self, distance_func):
        self.distance_func = distance_func
        self.root = None

    class Node:
        __slots__ = ("item", "children")

        def __init__(self, item):
            self.item = item
            self.children = {}

    def add(self, item):
        if self.root is None:
            self.root = self.Node(item)
            return

        current = self.root
        while True:
            distance = self.distance_func(item, current.item)
            if distance == 0:
                return  # Already exists
            if distance not in current.children:
                current.children[distance] = self.Node(item)
                break
            current = current.children[distance]

    def query(self, item, max_distance):
        if self.root is None:
            return []

        candidates = [self.root]
        results = []

        while candidates:
            node = candidates.pop(0)
            distance = self.distance_func(item, node.item)

            if distance <= max_distance:
                results.append((distance, node.item))

            for d in node.children:
                if distance - max_distance <= d <= distance + max_distance:
                    candidates.append(node.children[d])

        return results
