"""Scans the Intel Lab dataset and recommends a start date/motes that show real sensor faults.
Usage: python find_faults.py
"""
import pandas as pd

cols = ["date", "time", "epoch", "moteid", "temperature", "humidity", "light", "voltage"]
print("Scanning dataset (about 30-60 seconds)...")
df = pd.read_csv("data/data.txt.gz", sep=r"\s+", names=cols, header=None, on_bad_lines="skip")
for c in cols[3:]:
    df[c] = pd.to_numeric(df[c], errors="coerce")
df = df.dropna(subset=["moteid", "temperature", "humidity"])
df["moteid"] = df["moteid"].astype(int)
df["fault"] = (df.temperature > 45) | (df.temperature < 0) | (df.humidity < 0) | (df.humidity > 100)

print("\nReadings per mote (top 10, most complete data):")
print(df.groupby("moteid").size().sort_values(ascending=False).head(10).to_string())

per_day = df.groupby("date")["fault"].sum().sort_values(ascending=False)
print("\nDays with the most faulty readings:")
print(per_day.head(5).to_string())

day = per_day.index[0]
d = df[df.date == day]
bad_motes = d.groupby("moteid")["fault"].sum().sort_values(ascending=False)
bad_motes = [int(m) for m in bad_motes[bad_motes > 0].index[:3]]
good = df[~df.moteid.isin(bad_motes)].groupby("moteid").size().sort_values(ascending=False)
motes = bad_motes + [int(m) for m in good.index[:3]]
first = d[d.fault & d.moteid.isin(bad_motes)]
t0 = pd.to_datetime(first.date + " " + first.time, errors="coerce").min() - pd.Timedelta(hours=1)
print(f"\nFaulty motes that day: {bad_motes}")
print("\n=== RECOMMENDED COMMAND ===")
print(f'python replay_producer.py --motes {",".join(map(str, motes))} --start-date "{t0:%Y-%m-%d %H:%M}" --loop')