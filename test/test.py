import json

room_details_lengths = []

with open("scraped_data/properties.ndjson", "r") as f:
    for line in f:
        obj = json.loads(line)
        room_details = obj.get("room_details")
        if isinstance(room_details, list):
            length = len(room_details)
        else:
            length = 0  # or None if you prefer
        room_details_lengths.append(length)

print(room_details_lengths)
