import ast

with open("app/core/database.py", "r", encoding="utf-8") as f:
    tree = ast.parse(f.read())

for node in ast.walk(tree):
    if isinstance(node, ast.Call) and getattr(node.func, "id", "") == "_safe_create":
        col_name = ast.unparse(node.args[0])
        print(f"Collection: {col_name}")
        list_node = node.args[1]
        seen = {}
        for elt in list_node.elts:
            if isinstance(elt, ast.Call) and getattr(elt.func, "id", "") == "IndexModel":
                keys = ast.unparse(elt.args[0])
                print(f"  Index: {keys}")
                if keys in seen:
                    print(f"    *** DUPLICATE KEYS: {keys} ***")
                seen[keys] = True
