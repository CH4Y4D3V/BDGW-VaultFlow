import pathlib, re, sys

ROOT = pathlib.Path(".")
FIXED = []

def fix_bare_logger(path):
    src = path.read_text(encoding="utf-8", errors="replace")
    lines = src.splitlines(keepends=True)
    changed = False
    new_lines = []
    for line in lines:
        stripped = line.lstrip()
        if stripped.startswith("#"):
            new_lines.append(line)
            continue
        m = re.match(r'^(\s*)(logger\.\w+)\("([^"]+)"((?:,\s*[\w]+\s*=[^)\n]+)*)\)', line)
        if not m:
            new_lines.append(line)
            continue
        indent, method, msg, kwargs_str = m.groups()
        kwarg_pairs = re.findall(r'(\w+)\s*=\s*([^,)]+)', kwargs_str)
        safe = {"extra", "exc_info", "stack_info"}
        bad = [(k, v.strip()) for k, v in kwarg_pairs if k not in safe]
        good = [(k, v.strip()) for k, v in kwarg_pairs if k in safe]
        if not bad:
            new_lines.append(line)
            continue
        extra = "extra={" + ", ".join(f'"ctx_{k}": {v}' for k, v in bad) + "}"
        tail = ", " + extra
        for k, v in good:
            tail += f", {k}={v}"
        new_lines.append(f'{indent}{method}("{msg}"{tail})\n')
        changed = True
    if changed:
        path.write_text("".join(new_lines), encoding="utf-8")
        FIXED.append(str(path))

def fix_redis(path):
    src = path.read_text(encoding="utf-8", errors="replace")
    new_src = re.sub(r'(?<!\w)_redis(?!\w)', 'redis', src)
    if new_src != src:
        path.write_text(new_src, encoding="utf-8")
        FIXED.append(str(path) + " [_redis fixed]")

for p in ["app/core/lifecycle.py","app/referral/handlers.py","app/referral/scheduler.py","app/referral/service.py"]:
    pp = ROOT / p
    if pp.exists(): fix_bare_logger(pp)
    else: print(f"NOT FOUND: {p}")

for p in ["app/handlers/submission_handler.py","app/handlers/support_handler.py"]:
    pp = ROOT / p
    if pp.exists(): fix_redis(pp)
    else: print(f"NOT FOUND: {p}")

print()
if FIXED:
    print(f"Fixed {len(FIXED)} file(s):")
    [print(f"  * {f}") for f in FIXED]
else:
    print("Nothing changed.")
