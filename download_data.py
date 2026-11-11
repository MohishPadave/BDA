"""Downloads the Intel Berkeley Research Lab sensor dataset (34 MB) into ./data/."""
import os, sys, urllib.request

URLS = ["https://db.csail.mit.edu/labdata/data.txt.gz", "https://db.csail.mit.edu/labdata//data.txt.gz",
        "http://db.csail.mit.edu/labdata/data.txt.gz"]
DEST = os.path.join("data", "data.txt.gz")
os.makedirs("data", exist_ok=True)

def progress(blocks, size, total):
    if total > 0:
        pct = min(100, blocks * size * 100 // total)
        sys.stdout.write(f"\rDownloading... {pct}%")
        sys.stdout.flush()

for url in URLS:
    try:
        print("Trying", url)
        urllib.request.urlretrieve(url, DEST, progress)
        print(f"\nSaved to {DEST} ({os.path.getsize(DEST) / 1e6:.1f} MB)")
        break
    except Exception as e:
        print("\nFailed:", e)
else:
    print("\nCould not download automatically. Open https://db.csail.mit.edu/labdata/labdata.html in a browser,")
    print("download data.txt.gz and place it at data/data.txt.gz")
    sys.exit(1)

# Mote coordinates (used by the floor-plan dashboard)
for url in ["https://db.csail.mit.edu/labdata/mote_locs.txt", "https://db.csail.mit.edu/labdata//mote_locs.txt"]:
    try:
        urllib.request.urlretrieve(url, os.path.join("data", "mote_locs.txt"))
        print("Saved data/mote_locs.txt")
        break
    except Exception as e:
        print("Could not download mote_locs.txt:", e)
else:
    print("Download mote_locs.txt manually from the dataset page and put it in data/ (3 columns: id x y)")