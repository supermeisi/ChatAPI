import os
import time
import sqlite3
from openai import OpenAI

# --------- Config ---------
MODEL = "gpt-5.1"
DB_FILE = "chat_history.db"
CHAT_ID = int(time.time())   # Identifies this run

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

SYSTEM_PROMPT = (
    "Ich schreibe an einem Kurs über Geschichte. "
    "Gib mir das folgende Kapitel als zusammenhängenden Prosatext "
    "auf Englisch, ohne Gedankenstriche, keine '---', "
    "und mit Zwischenüberschriften in der obersten Ebene mit '#'."
)

# --------- DB setup ---------
def init_db():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS chapters (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id INTEGER NOT NULL,
        title TEXT NOT NULL,
        level INTEGER NOT NULL,
        parent_id INTEGER,
        position INTEGER NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(parent_id) REFERENCES chapters(id)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id INTEGER NOT NULL,
        chapter_id INTEGER,
        role TEXT NOT NULL,
        content TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(chapter_id) REFERENCES chapters(id)
    )
    """)

    conn.commit()
    conn.close()


def create_chapter(chat_id, title, level, parent_id, position):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO chapters (chat_id, title, level, parent_id, position)
        VALUES (?, ?, ?, ?, ?)
        """,
        (chat_id, title, level, parent_id, position),
    )
    chapter_id = cur.lastrowid
    conn.commit()
    conn.close()
    return chapter_id


def save_message(chat_id, chapter_id, role, content):
    conn = sqlite3.connect(DB_FILE)
    conn.execute(
        "INSERT INTO messages (chat_id, chapter_id, role, content) VALUES (?, ?, ?, ?)",
        (chat_id, chapter_id, role, content),
    )
    conn.commit()
    conn.close()


# --------- OpenAI interaction ---------
def send_to_existing_chat(chapter_id, user_message: str) -> str:
    save_message(CHAT_ID, chapter_id, "user", user_message)

    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_message},
    ]

    response = client.chat.completions.create(
        model=MODEL,
        messages=messages,
    )

    assistant_message = response.choices[0].message.content

    save_message(CHAT_ID, chapter_id, "assistant", assistant_message)

    return assistant_message


# --------- Parse chapters.txt by '#' hierarchy ---------
def parse_chapters_file(filename: str):
    """
    Lines must look like:
    # Title
    ## Subtitle
    ### Sub-subtitle
    """
    with open(filename, encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line:
                continue

            # Count # at start
            if not line.startswith("#"):
                raise ValueError(f"Invalid line (must start with #): {line}")

            level = 0
            while level < len(line) and line[level] == "#":
                level += 1

            title = line[level:].strip()

            yield level - 1, title   # level 0 = one '#'


# --------- Main ---------
if __name__ == "__main__":
    init_db()

    level_stack = []  # at index L → chapter_id at level L
    position_counter = 0

    for level, title in parse_chapters_file("chapters.txt"):

        # Shrink stack when moving up in hierarchy
        while len(level_stack) > level + 1:
            level_stack.pop()

        # Determine parent ID
        parent_id = None
        if level > 0:
            parent_id = level_stack[level - 1]

        # Create new chapter record
        chapter_id = create_chapter(
            CHAT_ID,
            title,
            level,
            parent_id,
            position_counter
        )
        position_counter += 1

        # Update stack
        if len(level_stack) == level:
            level_stack.append(chapter_id)
        else:
            level_stack[level] = chapter_id

        print(f"Processing L{level}: {title} (id={chapter_id}, parent={parent_id})")

        # Send chapter title to ChatGPT
        reply = send_to_existing_chat(chapter_id, title)

        print("\nAssistant:\n", reply)
        print("\n" + "=" * 80 + "\n")

