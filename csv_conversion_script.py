import os
import csv
from pathlib import Path
import re
from datetime import datetime

def parse_chat_line(line):
    """
    Parse a single line of chat log.
    Handles messages with various punctuation and emotes.
    """
    # First, extract timestamp and the rest
    timestamp_match = re.match(r'\[(.*?)\](.+)', line)
    if not timestamp_match:
        return None

    timestamp_str, rest = timestamp_match.groups()

    try:
        # Parse the timestamp
        timestamp = datetime.strptime(timestamp_str.strip(), '%Y-%m-%d %H:%M:%S')

        # Extract channel and username, with the entire message
        channel_user_message_match = re.match(r'\s*#(\w+)\s+([^:]+):(.+)', rest)

        if channel_user_message_match:
            channel, username, message = channel_user_message_match.groups()
            return {
                'timestamp': timestamp,
                'channel': channel,
                'username': username.strip(),
                'message': message.strip()
            }
    except (ValueError, AttributeError) as e:
        print(f"Error parsing line: {line}, Error: {str(e)}")
        return None

    return None

def process_log_files(logs_directory):
    """
    Process all .txt files in the logs directory and its subdirectories,
    combining them into a single CSV file.
    """
    # Create output directory if it doesn't exist
    output_dir = Path('processed_logs')
    output_dir.mkdir(exist_ok=True)

    # Prepare CSV output file
    output_file = output_dir / 'combined_chat_logs.csv'

    total_lines = 0
    processed_lines = 0

    try:
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            csv_writer = csv.writer(csvfile)
            # Write header
            csv_writer.writerow(['timestamp', 'channel', 'username', 'message', 'source_file'])

            # Walk through all files in the directory
            for root, _, files in os.walk(logs_directory):
                for file in files:
                    if file.endswith('.txt'):
                        file_path = Path(root) / file
                        print(f"Processing {file_path}")

                        try:
                            with open(file_path, 'r', encoding='utf-8') as f:
                                for line in f:
                                    total_lines += 1
                                    line = line.strip()
                                    if not line:  # Skip empty lines
                                        continue

                                    parsed_line = parse_chat_line(line)
                                    if parsed_line:
                                        processed_lines += 1
                                        csv_writer.writerow([
                                            parsed_line['timestamp'].strftime('%Y-%m-%d %H:%M:%S'),
                                            parsed_line['channel'],
                                            parsed_line['username'],
                                            parsed_line['message'],
                                            str(file_path)
                                        ])

                                    # Periodically flush to disk to prevent buffer issues
                                    if processed_lines % 10000 == 0:
                                        csvfile.flush()

                        except Exception as e:
                            print(f"Error processing {file}: {str(e)}")
                            continue

    except KeyboardInterrupt:
        print("\nProcess interrupted by user. Saving progress...")
    finally:
        print(f"\nProcessing summary:")
        print(f"Total lines read: {total_lines}")
        print(f"Successfully processed lines: {processed_lines}")
        print(f"Output saved to: {output_file}")

    return output_file

if __name__ == "__main__":
    # Replace with your logs directory path
    logs_dir = Path('logs')

    if not logs_dir.exists():
        print(f"Directory {logs_dir} does not exist!")
    else:
        output_file = process_log_files(logs_dir)