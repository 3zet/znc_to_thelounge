import os
import re
import sqlite3
import json
from datetime import datetime

# Path to The Lounge SQLite database
db_path = ".sqlite3"

# Directory with ZNC logs
znc_log_directory = ""

# The network GUID and channel for each log (you will need to provide this)
network_guid = ""  # Example: "aef323ad-3893-4b22-bc3a-20985c9981fe"
channel = ""  # Example IRC channel

# Connect to the SQLite database
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Function to convert ZNC timestamps (YYYY-MM-DD HH:MM:SS) to milliseconds since epoch
def convert_to_epoch_ms(datetime_str):
    dt = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")
    return int(dt.timestamp() * 1000)

# Function to insert a message into the database
def insert_message(network, channel, timestamp, msg_type, msg_json):
    cursor.execute(
        """
        INSERT INTO messages (network, channel, time, type, msg)
        VALUES (?, ?, ?, ?, ?)
        """, (network, channel, timestamp, msg_type, json.dumps(msg_json))
    )

# Regex pattern to parse ZNC log entries
log_entry_pattern = re.compile(r'^\[(\d{2}:\d{2}:\d{2})\] (<[^>]+>)? (.*)$')

# Iterate through all log files in the directory
for log_file in sorted(os.listdir(znc_log_directory)):
    if not log_file.endswith(".log"):
        continue

    # Extract the date from the filename (e.g., 2023-10-10.log)
    log_date = log_file.replace('.log', '')

    with open(os.path.join(znc_log_directory, log_file), 'r', encoding='utf-8', errors='replace') as file:
        for line in file:
            match = log_entry_pattern.match(line)
            if match:
                log_time = match.group(1)  # Extract the time part [HH:MM:SS]
                sender = match.group(2) if match.group(2) else ""  # Get sender, if available
                log_message = match.group(3)  # The rest of the message

                # Combine log date and time, convert to epoch milliseconds
                full_timestamp = f"{log_date} {log_time}"
                timestamp_ms = convert_to_epoch_ms(full_timestamp)

                # Determine the type of message and construct JSON accordingly
                try:
                    if log_message.startswith("***"):
                        # Join, part, quit events
                        msg_type = 'join' if 'joined' in log_message else 'part' if 'left' in log_message else 'quit'
                        parts = log_message.split()
                        if len(parts) > 1:
                            nick = parts[1].strip('()')  # Remove any parentheses
                            msg_json = {
                                "account": False,
                                "from": {
                                    "mode": "",
                                    "nick": nick
                                },
                                "gecos": nick,
                                "hostmask": "",  # Placeholder, set if available
                                "self": False,
                                "text": log_message
                            }
                        else:
                            print(f"Warning: Unexpected join/part log format: {log_message}")
                            continue  # Skip this log entry if it's not formatted correctly

                    else:
                        # Regular message
                        if sender:  # Ensure sender is not empty
                            nick = sender.strip('<>')
                            msg_type = 'message'
                            msg_json = {
                                "from": {
                                    "mode": "",
                                    "nick": nick
                                },
                                "self": False,
                                "text": log_message
                            }
                        else:
                            print(f"Warning: Empty sender in message log: {log_message}")
                            continue  # Skip this log entry if sender is empty

                    # Insert the log into the SQLite database
                    insert_message(network_guid, channel, timestamp_ms, msg_type, msg_json)

                except Exception as e:
                    print(f"Error processing log message: {e}")
                    print(f"Log message content: {log_message}")  # Print the content for debugging

# Commit changes and close the connection
conn.commit()
conn.close()

print("Logs imported successfully into The Lounge database.")
