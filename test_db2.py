import sqlite3
import json

conn = sqlite3.connect('/home/therayyanawaz/Documents/Github/TeleUserBot-main/data/teleuserbot.db')
cur = conn.cursor()
cur.execute("SELECT payload_json FROM inbound_jobs WHERE message_id = 50559;")
r = cur.fetchone()
if r and r[0]:
    payload = json.loads(r[0])
    fd = payload.get('filter_decision', {})
    print(f"fallback_reason: {fd.get('fallback_reason')}")
    print(f"reason_code: {fd.get('reason_code')}")
    print(f"ai_attempt_count: {fd.get('ai_attempt_count')}")

