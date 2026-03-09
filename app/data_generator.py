from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone

from faker import Faker

fake = Faker("ru_RU")
random.seed(42)
Faker.seed(42)

DEVICES = ["mobile", "desktop", "tablet"]
EVENT_TYPES = ["click", "view", "purchase", "login", "logout", "search"]
ISSUE_TYPES = ["payment", "delivery", "account", "refund", "bug"]
STATUSES = ["open", "in_progress", "closed"]
PAGES = ["/home", "/catalog", "/product/101", "/product/205", "/cart", "/checkout", "/profile"]
ACTIONS = ["login", "view_product", "search", "add_to_cart", "checkout_start", "purchase", "logout"]
PRODUCT_CATEGORIES = ["electronics", "books", "clothes", "home", "sport"]


def generate_documents(
    users_cnt: int = 100,
    products_cnt: int = 30,
    days: int = 60
) -> tuple[list[dict], list[dict], list[dict], list[dict], list[dict]]:
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)

    users: list[dict] = []
    products: list[dict] = []
    sessions: list[dict] = []
    events: list[dict] = []
    tickets: list[dict] = []

    user_ids = [f"user_{i:03d}" for i in range(1, users_cnt + 1)]
    product_ids = [f"prod_{i:03d}" for i in range(1, products_cnt + 1)]

    for user_id in user_ids:
        created_at = base + timedelta(days=random.randint(0, 20), minutes=random.randint(0, 1440))
        users.append(
            {
                "user_id": user_id,
                "name": fake.name(),
                "email": f"{user_id}@example.com",
                "created_at": created_at,
            }
        )

    for product_id in product_ids:
        products.append(
            {
                "product_id": product_id,
                "name": fake.word().capitalize() + " " + fake.word().capitalize(),
                "category": random.choice(PRODUCT_CATEGORIES),
                "price": round(random.uniform(10, 5000), 2),
                "created_at": base + timedelta(days=random.randint(0, 10)),
            }
        )

    for day in range(days):
        day_dt = base + timedelta(days=day)

        for _ in range(random.randint(20, 50)):
            user_id = random.choice(user_ids)
            start_time = day_dt + timedelta(minutes=random.randint(0, 1439))
            duration_min = random.randint(2, 90)
            end_time = start_time + timedelta(minutes=duration_min)
            pages = random.sample(PAGES, k=random.randint(2, 5))
            actions = random.sample(ACTIONS, k=random.randint(2, 5))
            session_id = f"sess_{day:03d}_{random.randint(1000, 9999)}"

            sessions.append(
                {
                    "session_id": session_id,
                    "user_id": user_id,
                    "start_time": start_time,
                    "end_time": end_time,
                    "pages_visited": pages,
                    "device": {"type": random.choice(DEVICES)},
                    "actions": actions,
                }
            )

            for i in range(random.randint(1, 4)):
                events.append(
                    {
                        "event_id": f"evt_{day:03d}_{random.randint(10000, 99999)}_{i}",
                        "timestamp": start_time + timedelta(minutes=i * random.randint(1, 10)),
                        "event_type": random.choice(EVENT_TYPES),
                        "details": {
                            "page": random.choice(pages),
                            "product_id": random.choice(product_ids),
                            "user_id": user_id,
                        },
                    }
                )

        for _ in range(random.randint(5, 15)):
            user_id = random.choice(user_ids)
            created_at = day_dt + timedelta(minutes=random.randint(0, 1439))
            updated_at = created_at + timedelta(hours=random.randint(1, 72))
            status = random.choice(STATUSES)

            if status == "open":
                updated_at = created_at + timedelta(hours=random.randint(1, 24))

            messages = []
            for idx in range(random.randint(2, 6)):
                sender = "user" if idx % 2 == 0 else "support"
                messages.append(
                    {
                        "sender": sender,
                        "message": fake.sentence(nb_words=6),
                        "timestamp": created_at + timedelta(hours=idx),
                    }
                )

            tickets.append(
                {
                    "ticket_id": f"ticket_{day:03d}_{random.randint(1000, 9999)}",
                    "user_id": user_id,
                    "status": status,
                    "issue_type": random.choice(ISSUE_TYPES),
                    "messages": messages,
                    "created_at": created_at,
                    "updated_at": updated_at,
                }
            )

    if users:
        users.append(users[0].copy())
    if products:
        products.append(products[0].copy())
    if sessions:
        sessions.append(sessions[0].copy())
    if events:
        events.append(events[0].copy())
    if tickets:
        tickets.append(tickets[0].copy())

    return users, products, sessions, events, tickets