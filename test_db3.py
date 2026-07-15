import sqlite3
import json
import sys
sys.path.append("/home/therayyanawaz/Documents/Github/TeleUserBot-main")

conn = sqlite3.connect('/home/therayyanawaz/Documents/Github/TeleUserBot-main/data/teleuserbot.db')
cur = conn.cursor()
cur.execute("SELECT payload_json FROM inbound_jobs WHERE message_id = 50559;")
r = cur.fetchone()
if r and r[0]:
    payload = json.loads(r[0])
    raw_text = payload.get('combined_text', '')
    from ai_filter import resolve_breaking_headline_for_delivery
    
    candidate = "🤍⚡🇮🇷Tonight's targets of the American terrorist attacks: 1"
    
    # Is it failing resolve_breaking_headline_for_delivery?
    print(f"resolved headline: {resolve_breaking_headline_for_delivery(raw_text, candidate, allow_fallback=False)}")
    print(f"resolved headline with fallback: {resolve_breaking_headline_for_delivery(raw_text, candidate, allow_fallback=True)}")

