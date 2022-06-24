class TreeNode:
    def __init__(self, item, parent, idx, level_id):
        self.item = item
        self.idx = idx
        self.level_id = level_id
        self.parent = parent
        self.children = []

    def get_parent(self):
        return self.parent

    def get_item(self):
        return self.item

    def get_children(self):
        return self.children

    def add_child(self, child):
        self.children.append(child)

    def get_idx(self):
        return self.idx

    def __str__(self):
        return f'level_id:{self.level_id}, idx:{self.idx}'


def recover_tree(vec, parent, start_idx):
    if len(vec) == 0:
        return vec, start_idx
    if vec[0] is None:
        return vec[1:], start_idx+1
    node = TreeNode(vec[0], parent, start_idx, -1)
    while True:
        vec, start_idx = recover_tree(vec[1:], node, start_idx+1)
        parent.add_child(node)
        if len(vec) == 0:
            return vec, start_idx
        if vec[0] is None:
            return vec[1:], start_idx+1
        node = TreeNode(vec[0], parent, start_idx, -1)


# if the input plan is Join1(Scan1, Join2(Scan2, Scan3)), then nodes_by_level will be a list like:
# [
#   [Join1]         <- level0
#   [Scan1, Join2]  <- level1
#   [Scan2, Scan3]  <- level2
# ]
def dfs_tree_to_level(root, level_id, nodes_by_level):
    root.level_id = level_id
    if len(nodes_by_level) <= level_id:
        nodes_by_level.append([])
    nodes_by_level[level_id].append(root)
    root.idx = len(nodes_by_level[level_id])
    for c in root.get_children():
        dfs_tree_to_level(c, level_id+1, nodes_by_level)


def parse_plan_tree(vec):
    nodes_by_level = []
    node = TreeNode(vec[0], None, 0, -1)
    recover_tree(vec[1:], node, 1)
    dfs_tree_to_level(node, 0, nodes_by_level)
    return nodes_by_level


def debug_nodes_by_level(nodes_by_level):
    for nodes in nodes_by_level:
        for node in nodes:
            whitespace = ' ' * node.level_id
            print(f'{whitespace}level_id:{node.level_id}')
            print(f'{whitespace}idx:{node.idx}')
