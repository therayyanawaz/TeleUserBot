import sqlite3
import json

# we want to fetch the exact AI response from the pipeline that processed msg 50559.
# Since it might have been cached, or retried, we can search the most recent messages in inbound_jobs.
conn = sqlite3.connect('/home/therayyanawaz/Documents/Github/TeleUserBot-main/data/teleuserbot.db')
cur = conn.cursor()
cur.execute("SELECT payload_json FROM inbound_jobs WHERE message_id = 50559;")
r = cur.fetchone()
if r and r[0]:
    payload = json.loads(r[0])
    print(json.dumps(payload.get('filter_decision', {}), indent=2))
