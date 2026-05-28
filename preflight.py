import ast, sys, pathlib, re

ROOT = pathlib.Path(".")
ERRORS = []

BAD_LOGGER = ["logger.info(", "logger.debug(", "logger.warning(", "logger.error(", "logger.critical("]

def check_bare_kwargs(path):
    src = path.read_text(errors="replace")
    for i, line in enumerate(src.splitlines(), 1):
        stripped = line.strip()
        if stripped.startswith("#"):
            continue
        for prefix in BAD_LOGGER:
            if prefix in stripped:
                kwargs = re.findall(r",\s*(\w+)\s*=", stripped)
                bad = [k for k in kwargs if k not in ("extra", "exc_info", "stack_info")]
                if bad:
                    ERRORS.append(f"BARE KWARG LOGGER: {path}:{i} — {bad} — {stripped[:100]}")
def check_syntax(path):
    try:
        ast.parse(path.read_text(errors="replace"))
    except SyntaxError as e:
        ERRORS.append(f"SYNTAX ERROR: {path}:{e.lineno} — {e.msg}")

def check_patterns():
    checks = [
        ("app/handlers/submission_handler.py", "_redis", "FIX 9 — _redis NameError still present"),
        ("app/handlers/support_handler.py",    "_redis", "FIX 10 — _redis NameError still present"),
        ("app/distribution/dispatcher.py",     "import asyncio", "FIX 8 — missing import asyncio"),
        ("app/scheduler/scheduler.py",         "import asyncio", "FIX 4 — missing import asyncio"),
    ]
    for filepath, pattern, msg in checks:
        p = ROOT / filepath
        if not p.exists():
            ERRORS.append(f"FILE MISSING: {filepath}")
            continue
        src = p.read_text(errors="replace")
        if filepath.endswith("submission_handler.py") or filepath.endswith("support_handler.py"):
            bad = re.findall(r'\b_redis\b', src)
            if bad:
                ERRORS.append(f"{msg} — found {len(bad)} occurrence(s) in {filepath}")
        else:
            if pattern not in src:
                ERRORS.append(f"{msg} — pattern not found in {filepath}")

def check_lifecycle():
    p = ROOT / "app/core/lifecycle.py"
    if not p.exists():
        ERRORS.append("FILE MISSING: app/core/lifecycle.py")
        return
    src = p.read_text(errors="replace")
    if "sys.exit" in src and "client.start" in src:
        exit_pos = src.index("sys.exit")
        start_pos = src.index("client.start")
        if start_pos < exit_pos:
            ERRORS.append("LIFECYCLE: client.start() appears BEFORE sys.exit() — may be unreachable")

py_files = list(ROOT.rglob("app/**/*.py"))
print(f"Scanning {len(py_files)} files...")

for f in py_files:
    check_syntax(f)
    check_bare_kwargs(f)

check_patterns()
check_lifecycle()

print()
if ERRORS:
    print(f"ISSUES FOUND: {len(ERRORS)}\n")
    for e in ERRORS:
        print(f"  * {e}")
    sys.exit(1)
else:
    print("All checks passed — safe to deploy.")


