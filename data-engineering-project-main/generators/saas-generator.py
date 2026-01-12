import json
import random
import uuid
from faker import Faker
from datetime import datetime, timedelta
import copy
fake = Faker()

num_users = 2500

max_user_events = 50

DUPLICATE_RATE = 0.02   # 2% duplicate events
LATE_EVENT_RATE = 0.05 # 5% late-arriving events

event_types = [
    "signup",
    "login",
    "view_dashboard",
    "create_project",
    "upgrade_plan",
    "cancel_subscription"
]
plans = ["free", "pro"]

users = []
events = []

start_date = datetime(2024, 1, 1)

#--Generate users--

for user_id in range(1, num_users + 1):
    signup_time = fake.date_time_between(start_date=start_date, end_date="now")
    user_events = []

    user = {
        "user_id": user_id,
        "signup_timestamp": signup_time.isoformat(),
        "country": fake.country_code(),
        "device": random.choice(["desktop", "mobile"]),
        "initial_plan": "free"
    }
    users.append(user)

    #--Generate events per user--

    num_events = random.randint(5, max_user_events)
    event_time = signup_time
    for _ in range(num_events):
            event_time += timedelta(minutes=random.randint(5, 5000))

            event = {
                "event_id": str(uuid.uuid4()),
                "user_id": user_id,
                "event_type": random.choices(
                    event_types,
                    weights=[1, 5, 10, 5, 1, 0.5],
                    k=1
                )[0],
                "event_timestamp": event_time.isoformat(),
                "session_id": str(uuid.uuid4()),
                "event_properties": {
                    "plan": random.choice(plans)
                }
            }

            # ---- LATE EVENT LOGIC ----
            if random.random() < LATE_EVENT_RATE:
                late_offset = timedelta(
                    hours=random.randint(1, 48),
                    minutes=random.randint(0, 59)
                )
                late_time = event_time - late_offset
                event["event_timestamp"] = late_time.isoformat()
                event["is_late"] = True
            else:
                event["is_late"] = False

            events.append(event)
            user_events.append(event)

            # ---- DUPLICATE EVENT LOGIC ----
            if random.random() < DUPLICATE_RATE:
                duplicate_event = copy.deepcopy(event)
                # SAME event_id, SAME timestamp
                duplicate_event["is_duplicate"] = True
                events.append(duplicate_event)

#--Write files--
with open("data/raw/users.json", "w") as f:
    for u in users:
        f.write(json.dumps(u) + "\n")

with open("data/raw/events.json", "w") as f:
    for e in events:
        f.write(json.dumps(e) + "\n")

print("Data generation complete!")

late_count = sum(1 for e in events if e["is_late"])
dup_count = sum(1 for e in events if e.get("is_duplicate"))

print(f"Total events: {len(events)}")
print(f"Late events: {late_count}")
print(f"Duplicate events: {dup_count}")
