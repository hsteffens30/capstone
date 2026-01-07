# imports
import os
import glob
import json
import sqlite3
import argparse
from typing import Optional

# opencreate database
def create_connection(db_path: str):
    # The database file will be created if it does not exist.
    conn = sqlite3.connect(db_path)
    return conn


def create_tables(conn: sqlite3.Connection):
    cur = conn.cursor()

    # posts: one row per unique topicID (first occurrence of each topicID)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS posts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        topic_id INTEGER UNIQUE,
        topic_title TEXT,
        post_id INTEGER,
        author TEXT,
        timestamp TEXT,
        content TEXT,
        category TEXT
    )
    """)

    # comments: duplicate occurrences of a topicID (after the first)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS comments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        topic_id INTEGER,
        post_id INTEGER,
        author TEXT,
        timestamp TEXT,
        content TEXT,
        FOREIGN KEY(topic_id) REFERENCES posts(topic_id)
    )
    """)

    # Helpful indexes for faster lookups
    cur.execute("CREATE INDEX IF NOT EXISTS idx_posts_id ON posts(topic_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_comments_topic ON comments(topic_id)")

    conn.commit()


# load jsonl files from given directory path
def load_jsonl_files(data_dir: str):
    # make a pattern to match all .jsonl files in the directory
    pattern = os.path.join(data_dir, "*.jsonl")

    # find all matching files
    for path in glob.glob(pattern):
        # open each file
        with open(path, "r", encoding="utf-8") as fh:
            # read each line
            for line in fh:
                line = line.strip()  # remove whitespace
                if not line:  # skip empty lines
                    continue
                try:
                    # give back parsed JSON object
                    yield json.loads(line)  # pause here and return the dict of string line
                except json.JSONDecodeError:
                    # skip malformed lines
                    print(f"Skipping malformed JSON line in {path}")


# read jsonl files and populate database
# first unique topicID -> posts table
# other w same topicID -> comments table
# returns posts/comments tables
def populate_db(conn: sqlite3.Connection, data_dir: str) -> tuple[int, int]:
    cur = conn.cursor()

    # track which topic_ids already inserted as a post
    # check DB in case script is re-run
    cur.execute("SELECT topic_id FROM posts")
    seen = {row[0] for row in cur.fetchall()}

    posts_count = 0
    comments_count = 0

    # for each line of data
    for record in load_jsonl_files(data_dir):
        # extract fields from the record
        topic_id = int(record.get("topicID")) if record.get("topicID") is not None else None
        post_id = int(record.get("postID")) if record.get("postID") is not None else None
        author = record.get("author")
        timestamp = record.get("timestamp")
        content = record.get("content")
        topic_title = record.get("topicTitle")
        category = record.get("category")

        # error handling
        if topic_id is None:
            # error in record, skip
            continue


        # insert into post table
        if topic_id not in seen:
            # first time seen topic_id, found original post
            cur.execute(
                """INSERT OR IGNORE INTO posts (topic_id, topic_title, post_id, author, timestamp, content, category)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (topic_id, topic_title, post_id, author, timestamp, content, category),
            )
            conn.commit()
            seen.add(topic_id)
            posts_count += 1
        # insert into comments table
        else:
            # duplicate topic_id, found comment
            cur.execute(
                "INSERT INTO comments (topic_id, post_id, author, timestamp, content) VALUES (?, ?, ?, ?, ?)",
                (topic_id, post_id, author, timestamp, content),
            )
            comments_count += 1

        # commit in small batches
        if (posts_count + comments_count) % 200 == 0:
            conn.commit()

    conn.commit()
    return posts_count, comments_count


def main(db_path: str, data_dir: str):
    # check data directory exists
    if not os.path.isdir(data_dir):
        print(f"Data directory not found: {data_dir}")
        return

    print(f"Opening database at: {db_path}")
    # create/open database and tables
    conn = create_connection(db_path)
    create_tables(conn)

    print("Populating database from JSONL files in:", data_dir)
    posts_inserted, comments_inserted = populate_db(conn, data_dir)

    print(f"Done. Posts inserted: {posts_inserted}, Comments inserted: {comments_inserted}")
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load scraped JSONL files into SQLite")
    parser.add_argument("--db", default="scraped_forum.db", help="SQLite DB filename (default: scraped_forum.db)")
    parser.add_argument("--data-dir", default="data", help="Directory containing .jsonl files (default: data)")
    args = parser.parse_args()

    main(args.db, args.data_dir)


