import sqlite3
import json
conn = sqlite3.connect('/home/therayyanawaz/Documents/Github/TeleUserBot-main/data/teleuserbot.db')
cur = conn.cursor()
cur.execute("SELECT current_facts_json FROM breaking_story_clusters WHERE cluster_id = '5e3550474e01401fb77b13027454394f';")
r = cur.fetchone()
if r:
    print("Found cluster!")
    print(json.dumps(json.loads(r[0]), indent=2))
else:
    print("Not found")
