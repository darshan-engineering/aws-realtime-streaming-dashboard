import boto3
import json
import time
import random
import uuid
from datetime import datetime, timezone

kinesis = boto3.client('kinesis', region_name='us-east-1')
STREAM_NAME = 'dashboard-stream'

MENU_ITEMS = [
    'Butter Chicken', 'Naan', 'Dal Makhani', 'Paneer Tikka',
    'Biryani', 'Garlic Bread', 'Lassi', 'Gulab Jamun',
    'Tandoori Chicken', 'Mango Lassi', 'Samosa', 'Raita'
]

STATUSES = ['NEW', 'PREPARING', 'READY']

active_orders = {}

def flush(records):
    """Send a batch of records to Kinesis using put_records (up to 500 per call)."""
    if not records:
        return
    resp = kinesis.put_records(StreamName=STREAM_NAME, Records=records)
    failed = resp.get('FailedRecordCount', 0)
    if failed:
        print(f"  ⚠️  {failed} record(s) failed to send")

def make_record(order):
    return {
        'Data': json.dumps(order).encode('utf-8'),
        'PartitionKey': order['entity_id']  # same order always routes to same shard
    }

def now():
    return datetime.now(timezone.utc).isoformat()

print("Kitchen producer started — simulating lunch rush...")
print("=" * 55)

# Simulate 15 ticks (~1 second apart).
# Each tick: 2-4 new orders arrive simultaneously (lunch rush burst),
# then every active order has a chance to advance its status.
for tick in range(90):
    records = []

    # --- New orders arriving this tick (burst of 2-4) ---
    new_count = random.randint(2, 4)
    for _ in range(new_count):
        order_id = f"ORD-{str(uuid.uuid4())[:6].upper()}"
        order = {
            'entity_id': order_id,
            'table_no': f"T{random.randint(1, 15)}",
            'items': random.sample(MENU_ITEMS, random.randint(1, 4)),
            'status': 'NEW',
            'placed_at': now(),
            'last_updated': now()
        }
        active_orders[order_id] = order
        records.append(make_record(order))
        print(f"  🆕 {order_id} | Table {order['table_no']} | NEW | {', '.join(order['items'])}")

    # --- Status advances for all active orders ---
    for oid, o in list(active_orders.items()):
        current_idx = STATUSES.index(o['status'])
        if current_idx < len(STATUSES) - 1 and random.random() > 0.45:
            o['status'] = STATUSES[current_idx + 1]
            o['last_updated'] = now()
            records.append(make_record(o))
            emoji = '👨‍🍳' if o['status'] == 'PREPARING' else '✅'
            print(f"  {emoji} {oid} | Table {o['table_no']} | {o['status']}")
            if o['status'] == 'READY':
                del active_orders[oid]

    # Send all events for this tick in one put_records call
    flush(records)
    print(f"  → Tick {tick + 1:02d}: sent {len(records)} event(s) | {len(active_orders)} active orders")
    print()
    time.sleep(1)

print("=" * 55)
print(f"Producer finished. {len(active_orders)} order(s) still active.")
