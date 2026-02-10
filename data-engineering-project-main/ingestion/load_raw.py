
import json
import psycopg2
from pathlib import Path

DATA_DIR = Path("data/raw")

DB_CONFIG = {
    "host": "host.docker.internal",
    "port": 5432,
    "dbname": "project-db",
    "user": "charry",
    "password": "password1"
}
def get_connection():
    return psycopg2.connect(**DB_CONFIG)


#Loading users
def load_users(conn, file_path):
    with conn.cursor() as cur, open(file_path, "r") as f:
        for line in f:
            user = json.loads(line)

            cur.execute(
                """
                INSERT INTO raw.users (
                    user_id,
                    signup_timestamp,
                    country,
                    device,
                    initial_plan,
                    source_file
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    user["user_id"],
                    user["signup_timestamp"],
                    user["country"],
                    user["device"],
                    user["initial_plan"],
                    file_path.name
                )
            )
    conn.commit()
    # ---------------- LOAD EVENTS ----------------
def load_events(conn, file_path):
    with conn.cursor() as cur, open(file_path, "r") as f:
        for line in f:
            event = json.loads(line)

            cur.execute(
                """
                INSERT INTO raw.events (
                    event_id,
                    user_id,
                    event_type,
                    event_timestamp,
                    session_id,
                    event_properties,
                    is_late,
                    is_duplicate,
                    source_file
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    event["event_id"],
                    event["user_id"],
                    event["event_type"],
                    event["event_timestamp"],
                    event["session_id"],
                    json.dumps(event.get("event_properties", {})),
                    event.get("is_late", False),
                    event.get("is_duplicate", False),
                    file_path.name
                )
            )
    conn.commit()
    # ---------------- MAIN ----------------
def main():
    conn = get_connection()

    for file_path in DATA_DIR.glob("*.json"):
        if file_path.name.startswith("users"):
            print(f"Loading users from {file_path.name}")
            load_users(conn, file_path)

        elif file_path.name.startswith("events"):
            print(f"Loading events from {file_path.name}")
            load_events(conn, file_path)

    conn.close()
    print("Ingestion complete.")

if __name__ == "__main__":
    main()