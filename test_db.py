import sqlite3

conn = sqlite3.connect('/home/therayyanawaz/Documents/Github/TeleUserBot-main/data/teleuserbot.db')
cur = conn.cursor()

# Check what filter decision the bot actually made
cur.execute("SELECT filter_decision_json FROM recent_breaking WHERE message_id = 50559;")
r = cur.fetchone()
print(f"Decision: {r[0]}")
