import sqlite3
import time
conn = sqlite3.connect('/home/therayyanawaz/Documents/Github/TeleUserBot-main/data/teleuserbot.db')
cur = conn.cursor()
cur.execute("SELECT * FROM seen_messages WHERE message_id = 50559;")
rows = cur.fetchall()
for r in rows:
    print(r)
