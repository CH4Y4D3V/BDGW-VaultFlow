import re
with open("app/core/database.py", "r", encoding="utf-8") as f:
    text = f.read()
block = text.split('await _safe_create(settings.VAULT_COLLECTION')[1].split('await _safe_create')[0]
print(block)
