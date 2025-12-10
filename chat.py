import os
import time
import json
import sqlite3
from typing import Dict, Any, Tuple, Optional

import requests
from openai import OpenAI

# ------------ Config ------------
MODEL = "gpt-5.1"
SQLITE_DB_FILE = "chat_history_linux.db"
CHAT_ID = int(time.time())

# OpenAI
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# System prompt: just produce chapter text from a given title
SYSTEM_PROMPT = (
    "You are helping to write a Linux course with sections and subsections.\n"
    "For each request you receive a section or subsection title.\n"
    # "- Write a continuous prose chapter in English.\n"
    "- Use top-level headings with '#' not including the section title.\n"
    "- Do not use '---' separators.\n"
    "- Write equations and formulas as Latex equations encapsulated in $$ for inline math and $$$$ for math mode.\n"
    "- Write inline code encapsulated with ` and code blocks within :::code :::.\n"
    "Answer only with the chapter text, no explanations around it."
)

# Remote PHP API (your endpoint)
API_URL = "https://kahibaro.com/api_insert_chapter.php"  # <-- adjust if needed
API_TOKEN = "Sonne121#"                      # <-- same as in PHP
COURSE_ID = 25  # set this to your real course id in `courses`

# ------------ SQLite helpers ------------
def init_sqlite_db():
    conn = sqlite3.connect(SQLITE_DB_FILE)
    cur = conn.cursor()

    # Chapters table (local tracking, plus remote_id)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS chapters (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id INTEGER NOT NULL,
        original_title TEXT NOT NULL,   -- here: same as title
        title TEXT NOT NULL,            -- chapter title (already in correct language)
        level INTEGER NOT NULL,
        parent_id INTEGER,
        position INTEGER NOT NULL,
        remote_id INTEGER,              -- MySQL id (from PHP API)
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(parent_id) REFERENCES chapters(id)
    )
    """)

    # Try to add remote_id column if table already exists without it
    try:
        cur.execute("ALTER TABLE chapters ADD COLUMN remote_id INTEGER")
    except sqlite3.OperationalError:
        # Column already exists or other non-fatal error
        pass

    # Messages log
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


def get_existing_sqlite_chapters() -> Dict[str, Dict[str, Any]]:
    """
    Map: original_title -> info dict (incl. remote_id).
    We use original_title as the key to detect already processed chapters.
    """
    conn = sqlite3.connect(SQLITE_DB_FILE)
    cur = conn.cursor()
    cur.execute(
        "SELECT id, chat_id, original_title, title, level, parent_id, position, remote_id FROM chapters"
    )
    rows = cur.fetchall()
    conn.close()

    result: Dict[str, Dict[str, Any]] = {}
    for (chapter_id, chat_id, original_title, title,
         level, parent_id, position, remote_id) in rows:
        result[original_title] = {
            "id": chapter_id,
            "chat_id": chat_id,
            "original_title": original_title,
            "title": title,
            "level": level,
            "parent_id": parent_id,
            "position": position,
            "remote_id": remote_id,
        }
    return result


def get_next_sqlite_position_start() -> int:
    conn = sqlite3.connect(SQLITE_DB_FILE)
    cur = conn.cursor()
    cur.execute("SELECT COALESCE(MAX(position) + 1, 0) FROM chapters")
    (start_pos,) = cur.fetchone()
    conn.close()
    return int(start_pos)


def create_sqlite_chapter(
    chat_id: int,
    chapter_title: str,
    level: int,
    parent_id: Optional[int],
    position: int,
) -> int:
    """
    Store a chapter locally in SQLite.
    original_title and title are the same now.
    """
    conn = sqlite3.connect(SQLITE_DB_FILE)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO chapters
            (chat_id, original_title, title, level, parent_id, position, remote_id)
        VALUES (?, ?, ?, ?, ?, ?, NULL)
        """,
        (chat_id, chapter_title, chapter_title, level, parent_id, position),
    )
    chapter_id = cur.lastrowid
    conn.commit()
    conn.close()
    return chapter_id


def update_sqlite_chapter_remote_id(chapter_id: int, remote_id: int):
    conn = sqlite3.connect(SQLITE_DB_FILE)
    cur = conn.cursor()
    cur.execute(
        "UPDATE chapters SET remote_id = ? WHERE id = ?",
        (remote_id, chapter_id),
    )
    conn.commit()
    conn.close()


def save_sqlite_message(chat_id: int, chapter_id: Optional[int], role: str, content: str):
    conn = sqlite3.connect(SQLITE_DB_FILE)
    conn.execute(
        "INSERT INTO messages (chat_id, chapter_id, role, content) VALUES (?, ?, ?, ?)",
        (chat_id, chapter_id, role, content),
    )
    conn.commit()
    conn.close()


# ------------ Remote PHP API helper ------------
def create_remote_chapter(
    title: str,
    parent_remote_id: Optional[int],
    position: int,
    content: str,
    description: Optional[str] = None,
    is_active: bool = True,
) -> int:
    """
    Call your PHP API to create a chapter in the MySQL database.
    We use the same string for `name` and `title` in the MySQL schema.
    """
    payload = {
        "name": title,            # previously German; now just the chapter title
        "course_id": COURSE_ID,
        "parent_id": parent_remote_id,
        "position": position,
        "title": title,           # English (or whatever language you now use)
        "description": description,
        "content": content,
        "is_active": is_active,
    }

    headers = {
        "X-API-TOKEN": API_TOKEN,
        "Content-Type": "application/json",
    }

    resp = requests.post(API_URL, json=payload, headers=headers, timeout=30)

    # Debug: show any server errors
    print("API status:", resp.status_code)
    print("API response text:", resp.text)

    resp.raise_for_status()
    data = resp.json()

    if not data.get("success"):
        raise RuntimeError(f"API error: {data}")

    return int(data["chapter_id"])


# ------------ OpenAI interaction ------------
def generate_chapter_text(chapter_title: str) -> str:
    """
    Send the chapter title to the model and get back plain chapter text (no JSON).
    """
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {
            "role": "user",
            "content": f"Chapter title: \"{chapter_title}\"",
        },
    ]

    response = client.chat.completions.create(
        model=MODEL,
        messages=messages,
    )

    chapter_text = response.choices[0].message.content
    return chapter_text.strip()


# ------------ Parse chapters.txt (# hierarchy) ------------
def parse_chapters_file(filename: str):
    """
    Lines like:
    # Chapter 1
    ## Subchapter 1.1
    ### Sub-subchapter 1.1.1

    Level mapping:
    - '# '   -> level 0
    - '## '  -> level 1
    - '### ' -> level 2
    """
    with open(filename, encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line:
                continue
            if not line.startswith("#"):
                raise ValueError(f"Invalid line (must start with #): {line}")

            level = 0
            while level < len(line) and line[level] == "#":
                level += 1

            title = line[level:].strip()  # chapter title
            yield level - 1, title  # level 0 for one '#'


# ------------ Main ------------
if __name__ == "__main__":
    init_sqlite_db()

    existing_sqlite_chapters = get_existing_sqlite_chapters()
    position_counter = get_next_sqlite_position_start()

    # Hierarchy stack for *this* run
    # Each entry: {"title": str, "sqlite_id": int, "remote_id": Optional[int]}
    level_stack: list[Dict[str, Any]] = []

    for level, chapter_title in parse_chapters_file("chapters_linux.txt"):
        # Adjust stack size based on current level
        while len(level_stack) > level + 1:
            level_stack.pop()

        parent_sqlite_id: Optional[int] = None
        parent_remote_id: Optional[int] = None
        if level > 0 and len(level_stack) >= level:
            parent_entry = level_stack[level - 1]
            parent_sqlite_id = parent_entry.get("sqlite_id")
            parent_remote_id = parent_entry.get("remote_id")

        # Check if this title was already processed
        if chapter_title in existing_sqlite_chapters:
            existing = existing_sqlite_chapters[chapter_title]
            sqlite_id = existing["id"]
            remote_id = existing["remote_id"]

            print(
                f"Skipping already processed chapter: '{chapter_title}' "
                f"(sqlite_id={sqlite_id}, remote_id={remote_id}, level={level})"
            )

            entry = {
                "title": chapter_title,
                "sqlite_id": sqlite_id,
                "remote_id": remote_id,
            }
            if len(level_stack) == level:
                level_stack.append(entry)
            else:
                level_stack[level] = entry

            continue

        # --- New chapter ---
        print(
            f"\n=== Processing NEW chapter L{level}: {chapter_title} "
            f"(parent_sqlite={parent_sqlite_id}, parent_remote={parent_remote_id}) ==="
        )

        # 1) Generate chapter text from the title
        chapter_text = generate_chapter_text(chapter_title)

        # 2) Insert into SQLite
        sqlite_chapter_id = create_sqlite_chapter(
            CHAT_ID,
            chapter_title=chapter_title,
            level=level,
            parent_id=parent_sqlite_id,
            position=position_counter,
        )

        # 3) Insert into remote MySQL via PHP API
        remote_chapter_id = create_remote_chapter(
            title=chapter_title,
            parent_remote_id=parent_remote_id,
            position=position_counter,
            content=chapter_text,
            description=None,
            is_active=True,
        )

        # 4) Store remote_id in SQLite
        update_sqlite_chapter_remote_id(sqlite_chapter_id, remote_chapter_id)

        # 5) Update in-memory map and stack
        existing_sqlite_chapters[chapter_title] = {
            "id": sqlite_chapter_id,
            "chat_id": CHAT_ID,
            "original_title": chapter_title,
            "title": chapter_title,
            "level": level,
            "parent_id": parent_sqlite_id,
            "position": position_counter,
            "remote_id": remote_chapter_id,
        }

        position_counter += 1

        new_entry = {
            "title": chapter_title,
            "sqlite_id": sqlite_chapter_id,
            "remote_id": remote_chapter_id,
        }
        if len(level_stack) == level:
            level_stack.append(new_entry)
        else:
            level_stack[level] = new_entry

        # 6) Save message log
        save_sqlite_message(CHAT_ID, sqlite_chapter_id, "user", chapter_title)
        save_sqlite_message(CHAT_ID, sqlite_chapter_id, "assistant", chapter_text)

        # 7) Show output
        print("\n--- Chapter text (assistant) ---\n")
        print(chapter_text)
        print("\n" + "=" * 80 + "\n")

