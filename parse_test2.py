import sqlite3
import json
conn = sqlite3.connect('/home/therayyanawaz/Documents/Github/TeleUserBot-main/data/teleuserbot.db')
cur = conn.cursor()
cur.execute("SELECT payload_json FROM inbound_jobs WHERE message_id = 50559;")
r = cur.fetchone()
if r and r[0]:
    payload = json.loads(r[0])
    raw_text = payload.get('combined_text', '')
    print(f"Original Text:\n{raw_text}")
