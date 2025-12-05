import os
import time
import sqlite3
from openai import OpenAI

# --------- Config ---------
MODEL = "gpt-5.1"  # or another chat-capable model
DB_FILE = "chat_history.db"

# One chat/session id per run (similar to your timestamped JSON file)
CHAT_ID = int(time.time())

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

SYSTEM_PROMPT = (
    "Ich schreibe an einem Kurs über Geschichte. "
    "Du bist mein Assistent. "
    "Gib mir das folgende Kapitel als zusammenhängenden Prosatext "
    "auf Englisch, ohne Gedankenstriche, ohne '---' zwischen Kapiteln, "
    "und mit Zwischenüberschriften auf der obersten Ebene mit '#'."
)

# --------- DB setup & helpers ---------
def init_db():
    conn = sqlite3.connect(DB_FILE)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,
            role TEXT NOT NULL,
            content TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.commit()
    conn.close()

def save_message(chat_id: int, role: str, content: str):
    conn = sqlite3.connect(DB_FILE)
    conn.execute(
        "INSERT INTO messages (chat_id, role, content) VALUES (?, ?, ?)",
        (chat_id, role, content),
    )
    conn.commit()
    conn.close()

def load_history(chat_id: int):
    """If you ever want to send past context, you can use this."""
    conn = sqlite3.connect(DB_FILE)
    cur = conn.execute(
        "SELECT role, content FROM messages WHERE chat_id=? ORDER BY id",
        (chat_id,),
    )
    rows = cur.fetchall()
    conn.close()
    return [{"role": role, "content": content} for role, content in rows]

# --------- OpenAI interaction ---------
def send_to_existing_chat(user_message: str) -> str:
    # Log the user message
    save_message(CHAT_ID, "user", user_message)

    # If you *want* previous messages as context, uncomment:
    history = load_history(CHAT_ID)
    messages = [{"role": "system", "content": SYSTEM_PROMPT}] + history + [
         {"role": "user", "content": user_message}
    ]

    # If you want each chapter independent (no context bleed), do this:
    # messages = [
    #    {"role": "system", "content": SYSTEM_PROMPT},
    #    {"role": "user", "content": user_message},
    #]

    response = client.chat.completions.create(
        model=MODEL,
        messages=messages,
    )

    assistant_message = response.choices[0].message.content

    # Log assistant reply
    save_message(CHAT_ID, "assistant", assistant_message)

    return assistant_message

# --------- Main ---------
if __name__ == "__main__":
    init_db()

    with open("chapters.txt", encoding="utf-8") as file:
        for line in file:
            chapter_text = line.rstrip()
            if not chapter_text:
                continue  # skip empty lines

            print("Input chapter line:")
            print(chapter_text)

            reply = send_to_existing_chat(chapter_text)

            print("\nAssistant:\n", reply)
            print("\n" + "=" * 80 + "\n")

