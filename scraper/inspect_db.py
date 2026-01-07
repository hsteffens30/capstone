import sqlite3

def inspect_db(db_path: str):
    # connect to database
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # list tables
    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    print(cur.fetchall())

    # Count total posts
    cur.execute("SELECT COUNT(*) FROM posts")
    total_posts = cur.fetchone()[0]
    print(f"Total unique posts: {total_posts}")

    # Count total comments
    cur.execute("SELECT COUNT(*) FROM comments")
    total_comments = cur.fetchone()[0]
    print(f"Total comments: {total_comments}")

    # Count posts per category
    cur.execute("SELECT category, COUNT(*) FROM posts GROUP BY category")
    print("Posts per category:")
    for row in cur.fetchall():
        category, count = row
        print(f"  {category}: {count}")

    conn.close()

def main():
    inspect_db("scraped_forum.db")

if __name__ == "__main__":
    main()