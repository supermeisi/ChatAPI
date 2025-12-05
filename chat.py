import os
import time
import json
import sqlite3
from typing import Dict, Any, Tuple, Optional
from openai import OpenAI

# --------- Config ---------
MODEL = "gpt-5.1"
DB_FILE = "chat_history.db"
CHAT_ID = int(time.time())   # Identifies this run

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

SYSTEM_PROMPT = (
    "You are helping to write a history course.\n"
    "For each request you receive a chapter title in German.\n"
    "You MUST answer ONLY with valid JSON of the form:\n"
    "{\n"
    '  "english_title": "<short English chapter title>",\n'
    '  "chapter_text": "<full chapter text in English>"\n'
    "}\n"
    "- english_title: a concise English chapter heading.\n"
    "- chapter_text: a continuous prose chapter in English, "
    "with top-level headings using '#', no bullet lists, "
    "no '---' separators.\n"
    "Do not write any text outside the JSON."
)

# --------- DB setup ---------
def init_db():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    # Chapters: keep both German and English titles
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


def get_existing_chapters() -> Dict[str, Dict[str, Any]]:
    """
    Load all chapters from the DB and return a mapping:
        original_title (German) -> {id, level, parent_id, position, title, chat_id}
    If there are duplicates, the last one read wins.
    """
    conn = sqlite3.connect(DB_FILE)
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


def get_next_position_start() -> int:
    """Get the next position index (max(position)+1) for new chapters."""
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("SELECT COALESCE(MAX(position) + 1, 0) FROM chapters")
    (start_pos,) = cur.fetchone()
    conn.close()
    return int(start_pos)


def create_chapter(
    chat_id: int,
    original_title: str,
    english_title: str,
    level: int,
    parent_id: Optional[int],
    position: int,
) -> int:
    conn = sqlite3.connect(DB_FILE)
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


def save_message(chat_id: int, chapter_id: Optional[int], role: str, content: str):
    conn = sqlite3.connect(DB_FILE)
    conn.execute(
        "INSERT INTO messages (chat_id, chapter_id, role, content) VALUES (?, ?, ?, ?)",
        (chat_id, chapter_id, role, content),
    )
    conn.commit()
    conn.close()


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
    init_db()

    # Load already known chapters (by original_title)
    existing_chapters = get_existing_chapters()

    # Start position after the last existing chapter
    position_counter = get_next_position_start()

    # This stack will track the current hierarchy in *this run*
    # (we will push both old and new chapter IDs onto it)
    level_stack: list[int] = []  # at index L → chapter_id at level L

    for level, german_title in parse_chapters_file("chapters.txt"):
        # Adjust stack size according to the level
        while len(level_stack) > level + 1:
            level_stack.pop()

        # If this chapter already exists (by German title), skip re-processing
        if german_title in existing_chapters:
            existing = existing_chapters[german_title]
            chapter_id = existing["id"]

            # Make sure it participates in the hierarchy for its children
            if len(level_stack) <= level:
                level_stack.append(chapter_id)
            else:
                level_stack[level] = chapter_id

            print(f"Skipping already processed chapter: '{german_title}' "
                  f"(id={chapter_id}, level={level})")
            continue

        # Determine parent_id: use stack entry at level-1 if available
        parent_id: Optional[int] = None
        if level > 0 and len(level_stack) >= level:
            parent_id = level_stack[level - 1]

        print(f"\n=== Processing NEW chapter L{level}: {german_title} (parent={parent_id}) ===")

        # 1) Get English title + chapter text from the model
        english_title, chapter_text = generate_chapter_from_german_title(german_title)
        print(f"  → English title: {english_title}")

        # 2) Insert new chapter into DB
        chapter_id = create_chapter(
            CHAT_ID,
            original_title=german_title,
            english_title=english_title,
            level=level,
            parent_id=parent_id,
            position=position_counter,
        )
        position_counter += 1

        # 3) Update local cache so future runs also know this chapter
        existing_chapters[german_title] = {
            "id": chapter_id,
            "chat_id": CHAT_ID,
            "original_title": german_title,
            "title": english_title,
            "level": level,
            "parent_id": parent_id,
            "position": position_counter - 1,
        }

        # 4) Update hierarchy stack
        if len(level_stack) == level:
            level_stack.append(chapter_id)
        else:
            level_stack[level] = chapter_id

        # 5) Save messages (German input + English chapter text)
        save_message(CHAT_ID, chapter_id, "user", german_title)
        save_message(CHAT_ID, chapter_id, "assistant", chapter_text)

        # 6) Show result
        print("\n--- Chapter text (assistant) ---\n")
        print(chapter_text)
        print("\n" + "=" * 80 + "\n")

