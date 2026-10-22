#!/opt/kronos/venv/bin/python
import sqlite3
db = sqlite3.connect("/opt/kronos/predictions.db")
for tf, n, ok, pend in db.execute("SELECT timeframe, SUM(correct IS NOT NULL), SUM(correct=1), SUM(correct IS NULL) FROM predictions GROUP BY timeframe"):
    print(f"{tf}: {ok or 0}/{n or 0} directions justes" + (f" = {100*ok/n:.1f}%" if n else "") + f" | en attente: {pend}")
