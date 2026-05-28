import re
with open("app/core/database.py", "r", encoding="utf-8") as f:
    text = f.read()
matches = re.findall(r'_safe_create\((.*?)\,', text)
print("Collections registered:")
for m in matches:
    print(m.strip())
