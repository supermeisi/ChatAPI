import os
import time
import json
import sqlite3
from typing import Dict, Any, Tuple, Optional

import requests
from openai import OpenAI

# ------------ Config ------------
MODEL = "gpt-5.1"
SQLITE_DB_FILE = "chat_history.db"
CHAT_ID = int(time.time())

# OpenAI
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

SYSTEM_PROMPT = (
    "You are helping to write a history course.\n"
    "For each request you receive a chapter title in German.\n"
    "You MUST answer ONLY with valid JSON of the form:\n"
    "{\n"
    '  \"english_title\": \"<short English chapter title>\",\n'
    '  \"chapter_text\": \"<full chapter text in English>\"\n'
    "}\n"
    "- english_title: a concise English chapter heading.\n"
    "- chapter_text: a continuous prose chapter in English, "
    "with top-level headings using '#', no bullet lists, "
    "no '---' separators.\n"
    "Do not write any text outside the JSON."
)

# Remote PHP API
API_URL = "https://kahibaro.com/api_insert_chapter.php"  # <-- adjust
API_TOKEN = "Sonne121#"                         # <-- same as in PHP
COURSE_ID = 20  # set this to your real course id in `courses`

# ------------ SQLite helpers ------------
def init_sqlite_db():
    conn = sqlite3.connect(SQLITE_DB_FILE)
    cur = conn.cursor()

    # Chapters table (local tracking, plus remote_id)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS chapters (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id INTEGER NOT NULL,
        original_title TEXT NOT NULL,   -- German
        title TEXT NOT NULL,            -- English
        level INTEGER NOT NULL,
        parent_id INTEGER,
        position INTEGER NOT NULL,
        remote_id INTEGER,              -- MySQL id
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
    Map: original_title (German) -> info dict (incl. remote_id).
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
    original_title: str,
    english_title: str,
    level: int,
    parent_id: Optional[int],
    position: int,
) -> int:
    conn = sqlite3.connect(SQLITE_DB_FILE)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO chapters
            (chat_id, original_title, title, level, parent_id, position, remote_id)
        VALUES (?, ?, ?, ?, ?, ?, NULL)
        """,
        (chat_id, original_title, english_title, level, parent_id, position),
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
    german_name: str,
    parent_remote_id: Optional[int],
    position: int,
    english_title: str,
    content: str,
    description: Optional[str] = None,
    is_active: bool = True,
) -> int:
    payload = {
        "name": german_name,
        "course_id": COURSE_ID,
        "parent_id": parent_remote_id,
        "position": position,
        "title": english_title,
        "description": description,
        "content": content,
        "is_active": is_active,
    }

    headers = {
        "X-API-TOKEN": API_TOKEN,
        "Content-Type": "application/json",
    }

    resp = requests.post(API_URL, json=payload, headers=headers, timeout=30)

    # Debug output
    print("API status:", resp.status_code)
    print("API response text:", resp.text)

    # This will still raise for 4xx/5xx, but now you'll see the message
    resp.raise_for_status()

    data = resp.json()

    if not data.get("success"):
        raise RuntimeError(f"API error: {data}")

    return int(data["chapter_id"])


# ------------ OpenAI interaction ------------
def generate_chapter_from_german_title(german_title: str) -> Tuple[str, str]:
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {
            "role": "user",
            "content": f"Kapitelüberschrift (Deutsch): \"{german_title}\"",
        },
    ]

    response = client.chat.completions.create(
        model=MODEL,
        messages=messages,
    )

    raw_content = response.choices[0].message.content

    try:
        data = json.loads(raw_content)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Model did not return valid JSON: {raw_content}") from e

    english_title = data.get("english_title", "").strip()
    chapter_text = data.get("chapter_text", "").strip()

    if not english_title:
        raise RuntimeError(f"No 'english_title' in model response: {raw_content}")
    if not chapter_text:
        raise RuntimeError(f"No 'chapter_text' in model response: {raw_content}")

    return english_title, chapter_text


# ------------ Parse chapters.txt (# hierarchy) ------------
def parse_chapters_file(filename: str):
    """
    Lines like:
    # Kapitel 1
    ## Unterkapitel 1.1
    ### Unter-Unterkapitel 1.1.1
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

            title = line[level:].strip()  # German title
            yield level - 1, title  # level 0 for one '#'


# ------------ Main ------------
if __name__ == "__main__":
    init_sqlite_db()

    existing_sqlite_chapters = get_existing_sqlite_chapters()
    position_counter = get_next_sqlite_position_start()

    # Hierarchy stack for *this* run
    # Each entry: {"title": str, "sqlite_id": int, "remote_id": Optional[int]}
    level_stack: list[Dict[str, Any]] = []

    for level, german_title in parse_chapters_file("chapters.txt"):
        # Adjust stack size
        while len(level_stack) > level + 1:
            level_stack.pop()

        parent_sqlite_id: Optional[int] = None
        parent_remote_id: Optional[int] = None
        if level > 0 and len(level_stack) >= level:
            parent_entry = level_stack[level - 1]
            parent_sqlite_id = parent_entry.get("sqlite_id")
            parent_remote_id = parent_entry.get("remote_id")

        # Already processed locally?
        if german_title in existing_sqlite_chapters:
            existing = existing_sqlite_chapters[german_title]
            sqlite_id = existing["id"]
            remote_id = existing["remote_id"]

            print(
                f"Skipping already processed chapter: '{german_title}' "
                f"(sqlite_id={sqlite_id}, remote_id={remote_id}, level={level})"
            )

            entry = {
                "title": german_title,
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
            f"\n=== Processing NEW chapter L{level}: {german_title} "
            f"(parent_sqlite={parent_sqlite_id}, parent_remote={parent_remote_id}) ==="
        )

        english_title, chapter_text = generate_chapter_from_german_title(german_title)
        print(f"  → English title: {english_title}")

        # Insert into SQLite
        sqlite_chapter_id = create_sqlite_chapter(
            CHAT_ID,
            original_title=german_title,
            english_title=english_title,
            level=level,
            parent_id=parent_sqlite_id,
            position=position_counter,
        )

        # Insert into remote MySQL via PHP API
        remote_chapter_id = create_remote_chapter(
            german_name=german_title,
            parent_remote_id=parent_remote_id,
            position=position_counter,
            english_title=english_title,
            content=chapter_text,
            description=None,
            is_active=False,
        )

        # Store remote_id in SQLite
        update_sqlite_chapter_remote_id(sqlite_chapter_id, remote_chapter_id)

        # Update in-memory map and stack
        existing_sqlite_chapters[german_title] = {
            "id": sqlite_chapter_id,
            "chat_id": CHAT_ID,
            "original_title": german_title,
            "title": english_title,
            "level": level,
            "parent_id": parent_sqlite_id,
            "position": position_counter,
            "remote_id": remote_chapter_id,
        }

        position_counter += 1

        new_entry = {
            "title": german_title,
            "sqlite_id": sqlite_chapter_id,
            "remote_id": remote_chapter_id,
        }
        if len(level_stack) == level:
            level_stack.append(new_entry)
        else:
            level_stack[level] = new_entry

        # Save message log
        save_sqlite_message(CHAT_ID, sqlite_chapter_id, "user", german_title)
        save_sqlite_message(CHAT_ID, sqlite_chapter_id, "assistant", chapter_text)

        print("\n--- Chapter text (assistant) ---\n")
        print(chapter_text)
        print("\n" + "=" * 80 + "\n")

