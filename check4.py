import ast

with open("app/core/database.py", "r", encoding="utf-8") as f:
    tree = ast.parse(f.read())

for node in ast.walk(tree):
    if isinstance(node, ast.Call) and getattr(node.func, "id", "") == "_safe_create":
        col_name = ast.unparse(node.args[0])
        list_node = node.args[1]
        for elt in list_node.elts:
            if isinstance(elt, ast.Call) and getattr(elt.func, "id", "") == "IndexModel":
                name = None
                for kw in elt.keywords:
                    if kw.arg == "name":
                        name = ast.unparse(kw.value)
                print(f"Col: {col_name}, Name: {name}")
