import sqlite3
conn = sqlite3.connect('/home/therayyanawaz/Documents/Github/TeleUserBot-main/data/teleuserbot.db')
cur = conn.cursor()
cur.execute("SELECT raw_text FROM seen_messages WHERE message_id = 50559;")
r = cur.fetchone()
print(r)
