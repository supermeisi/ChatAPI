import os
import time
import json
import sqlite3
from typing import Dict, Any, Tuple, Optional

from openai import OpenAI
import mysql.connector

# --------- OpenAI / general config ---------
MODEL = "gpt-5.1"
SQLITE_DB_FILE = "chat_history.db"
CHAT_ID = int(time.time())   # Identifies this run

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

# --------- MySQL config (Strato) ---------
MYSQL_CONFIG = {
    "host": "rdbms.strato.de",
    "port": 3306,
    "user": "dbu1211409",
    "password": "8m4SCF6)F/zS",
    "database": "dbs14310204",
}

COURSE_ID = 100  # <-- Set this to your actual course id from `courses` table

_mysql_conn: Optional[mysql.connector.connection.MySQLConnection] = None

def get_mysql_conn():
    """Lazy-connect to MySQL and reuse the connection."""
    global _mysql_conn
    if _mysql_conn is None or not _mysql_conn.is_connected():
        _mysql_conn = mysql.connector.connect(**MYSQL_CONFIG)
    return _mysql_conn


# --------- SQLite setup ---------
def init_sqlite_db():
    conn = sqlite3.connect(SQLITE_DB_FILE)
    cur = conn.cursor()

    # Chapters in SQLite (local tracking)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS chapters (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id INTEGER NOT NULL,
        original_title TEXT NOT NULL,   -- German
        title TEXT NOT NULL,            -- English
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


def get_existing_sqlite_chapters() -> Dict[str, Dict[str, Any]]:
    """
    Load all chapters from the local SQLite DB.
    Map: original_title (German) -> info dict
    """
    conn = sqlite3.connect(SQLITE_DB_FILE)
    cur = conn.cursor()
    cur.execute(
        "SELECT id, chat_id, original_title, title, level, parent_id, position FROM chapters"
    )
    rows = cur.fetchall()
    conn.close()

    result: Dict[str, Dict[str, Any]] = {}
    for chapter_id, chat_id, original_title, title, level, parent_id, position in rows:
        result[original_title] = {
            "id": chapter_id,
            "chat_id": chat_id,
            "original_title": original_title,
            "title": title,
            "level": level,
            "parent_id": parent_id,
            "position": position,
        }
    return result


def get_next_sqlite_position_start() -> int:
    """Get the next local position index (max(position)+1) for new chapters."""
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
        INSERT INTO chapters (chat_id, original_title, title, level, parent_id, position)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (chat_id, original_title, english_title, level, parent_id, position),
    )
    chapter_id = cur.lastrowid
    conn.commit()
    conn.close()
    return chapter_id


def save_sqlite_message(chat_id: int, chapter_id: Optional[int], role: str, content: str):
    conn = sqlite3.connect(SQLITE_DB_FILE)
    conn.execute(
        "INSERT INTO messages (chat_id, chapter_id, role, content) VALUES (?, ?, ?, ?)",
        (chat_id, chapter_id, role, content),
    )
    conn.commit()
    conn.close()


# --------- MySQL helpers ---------
def load_mysql_chapters_by_name() -> Dict[str, int]:
    """
    Load existing chapters from MySQL for this course, keyed by `name` (German title).
    """
    conn = get_mysql_conn()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT id, name FROM chapters WHERE course_id = %s", (COURSE_ID,))
    rows = cur.fetchall()
    cur.close()
    return {row["name"]: row["id"] for row in rows}


def create_mysql_chapter(
    german_name: str,
    parent_mysql_id: Optional[int],
    position: int,
    english_title: str,
    content: str,
    description: Optional[str] = None,
    is_active: bool = False,
) -> int:
    """
    Insert a chapter row into your Strato MySQL `chapters` table.
    """
    conn = get_mysql_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO chapters
            (name, course_id, parent_id, position, title, description, content, is_active)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            german_name,
            COURSE_ID,
            parent_mysql_id,
            position,
            english_title,
            description,
            content,
            is_active,
        ),
    )
    conn.commit()
    chapter_id = cur.lastrowid
    cur.close()
    return chapter_id


# --------- OpenAI interaction ---------
def generate_chapter_from_german_title(german_title: str) -> Tuple[str, str]:
    """
    Sends the German chapter title and gets back:
      - english_title
      - chapter_text (English prose)
    """
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

    # Parse the JSON returned by the model
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


# --------- Parse chapters.txt by '#' hierarchy ---------
def parse_chapters_file(filename: str):
    """
    Lines should look like:
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

            title = line[level:].strip()   # German title

            yield level - 1, title   # level 0 for one '#'


# --------- Main ---------
if __name__ == "__main__":
    # Initialize SQLite
    init_sqlite_db()

    # Load existing chapters from SQLite (for "already processed" detection)
    existing_sqlite_chapters = get_existing_sqlite_chapters()

    # Next local position index
    position_counter = get_next_sqlite_position_start()

    # Load existing chapters from MySQL to know their IDs by name (German)
    mysql_chapters_by_name = load_mysql_chapters_by_name()

    # Hierarchy stack: one entry per level
    # Each entry: {"title": german_title, "sqlite_id": int, "mysql_id": Optional[int]}
    level_stack: list[Dict[str, Any]] = []

    for level, german_title in parse_chapters_file("chapters.txt"):
        # Adjust stack size according to the current level
        while len(level_stack) > level + 1:
            level_stack.pop()

        # Get parent info from stack (if any)
        parent_sqlite_id: Optional[int] = None
        parent_mysql_id: Optional[int] = None
        if level > 0 and len(level_stack) >= level:
            parent_entry = level_stack[level - 1]
            parent_sqlite_id = parent_entry.get("sqlite_id")
            parent_mysql_id = parent_entry.get("mysql_id")

        # If this chapter already exists in SQLite, skip generation
        if german_title in existing_sqlite_chapters:
            existing = existing_sqlite_chapters[german_title]
            sqlite_id = existing["id"]
            mysql_id = mysql_chapters_by_name.get(german_title)  # may be None if not yet in MySQL

            print(f"Skipping already processed chapter: '{german_title}' "
                  f"(sqlite_id={sqlite_id}, mysql_id={mysql_id}, level={level})")

            # Ensure it participates in the hierarchy for children
            entry = {
                "title": german_title,
                "sqlite_id": sqlite_id,
                "mysql_id": mysql_id,
            }
            if len(level_stack) == level:
                level_stack.append(entry)
            else:
                level_stack[level] = entry

            continue

        # --- New chapter: process with model and store in both DBs ---
        print(f"\n=== Processing NEW chapter L{level}: {german_title} (parent_sqlite={parent_sqlite_id}, parent_mysql={parent_mysql_id}) ===")

        # 1) Get English title + chapter text from the model
        english_title, chapter_text = generate_chapter_from_german_title(german_title)
        print(f"  → English title: {english_title}")

        # 2) Insert into SQLite
        sqlite_chapter_id = create_sqlite_chapter(
            CHAT_ID,
            original_title=german_title,
            english_title=english_title,
            level=level,
            parent_id=parent_sqlite_id,
            position=position_counter,
        )

        # 3) Insert into MySQL
        mysql_chapter_id = create_mysql_chapter(
            german_name=german_title,
            parent_mysql_id=parent_mysql_id,
            position=position_counter,
            english_title=english_title,
            content=chapter_text,
            description=None,
            is_active=False,  # or True if you want it live immediately
        )

        # 4) Update in-memory maps so later children & runs know these chapters
        existing_sqlite_chapters[german_title] = {
            "id": sqlite_chapter_id,
            "chat_id": CHAT_ID,
            "original_title": german_title,
            "title": english_title,
            "level": level,
            "parent_id": parent_sqlite_id,
            "position": position_counter,
        }
        mysql_chapters_by_name[german_title] = mysql_chapter_id

        position_counter += 1

        # 5) Update hierarchy stack
        new_entry = {
            "title": german_title,
            "sqlite_id": sqlite_chapter_id,
            "mysql_id": mysql_chapter_id,
        }
        if len(level_stack) == level:
            level_stack.append(new_entry)
        else:
            level_stack[level] = new_entry

        # 6) Save local message log in SQLite
        save_sqlite_message(CHAT_ID, sqlite_chapter_id, "user", german_title)
        save_sqlite_message(CHAT_ID, sqlite_chapter_id, "assistant", chapter_text)

        # 7) Print result
        print("\n--- Chapter text (assistant) ---\n")
        print(chapter_text)
        print("\n" + "=" * 80 + "\n")

