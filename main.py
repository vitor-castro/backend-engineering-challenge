import json
import argparse
import os
import sys
from datetime import datetime, timedelta

events = [{"timestamp": "2018-12-26 18:11:08.509654", "translation_id": "5aa5b2f39f7254a75aa5",
           "source_language": "en",
           "target_language": "fr", "client_name": "airliberty", "event_name": "translation_delivered",
           "nr_words": 30,
           "duration": 20},
          {"timestamp": "2018-12-26 18:15:19.903159", "translation_id": "5aa5b2f39f7254a75aa4",
           "source_language": "en",
           "target_language": "fr", "client_name": "airliberty", "event_name": "translation_delivered",
           "nr_words": 30,
           "duration": 31},
          {"timestamp": "2018-12-26 18:23:19.903159", "translation_id": "5aa5b2f39f7254a75bb3",
           "source_language": "en",
           "target_language": "fr", "client_name": "taxi-eats", "event_name": "translation_delivered",
           "nr_words": 100,
           "duration": 54}]




def process_input(args):
    # Read the JSON file
    try:
        with open(args.input_file, 'r') as file:
            json_data_raw = file.read()
    except Exception as e:
        print(json.dumps({'error': f'Failed to read input file: {str(e)}'}))
        sys.exit(1)

    window_size = args.window_size

    events_json_data = json.loads(json_data_raw)

    return events_json_data, window_size


def remove_unnecessary_fields(json_data):
    filtered_data = []
    for translation in json_data:
        filtered_translation = {
            'timestamp': translation['timestamp'],
            'duration': translation['duration']
        }
        # Append the filtered translation to the list
        filtered_data.append(filtered_translation)

    return filtered_data


def create_output_file(events_json_data):
    # Parse timestamps in events_json_data to datetime objects
    timestamps = []
    for entry in events_json_data:
        timestamps.append(datetime.strptime(entry["timestamp"], "%Y-%m-%d %H:%M:%S.%f"))

    # Assuming the json is sorted by timestamp
    min_timestamp = timestamps[0]
    max_timestamp = timestamps[len(events_json_data) - 1]

    # Create output_file with the desired structure
    output_file = []
    current_time = min_timestamp.replace(second=0, microsecond=0)
    end_time = max_timestamp.replace(second=0, microsecond=0) + timedelta(minutes=1)

    while current_time <= end_time:
        output_file.append({"date": current_time.strftime("%Y-%m-%d %H:%M:00"), "average_delivery_time": -1})
        current_time += timedelta(minutes=1)

    return output_file


def get_average_delivery_time(events_json_data, current_timestamp, window_size):
    # Parse the current_timestamp
    current_time = datetime.strptime(current_timestamp, "%Y-%m-%d %H:%M:%S")

    total_duration = 0
    nr_of_translations = 0

    # Compute total duration
    for event in events_json_data:
        event_time = datetime.strptime(event['timestamp'], "%Y-%m-%d %H:%M:%S.%f")

        # Compute time difference (in minutes)
        time_diff = (current_time - event_time).total_seconds() / 60

        if event_time < current_time and time_diff <= window_size:
            total_duration += event['duration']
            nr_of_translations += 1

    if nr_of_translations > 0:
        average_duration = total_duration / nr_of_translations
    else:
        average_duration = 0

    return average_duration


def fill_avg_delivery_time(events_json_data, output_file, window_size):
    for i in range(0, len(output_file)):
        avg_delivery_time = get_average_delivery_time(events_json_data, output_file[i]['date'], window_size)
        output_file[i]['average_delivery_time'] = avg_delivery_time

    return output_file


def print_output_file(output_file):
    for line in output_file:
        print(line)


if __name__ == '__main__':

    '''
    # ------ CODE TO USE IN THE FINAL VERSION ------
    # Setup argument parser
    parser = argparse.ArgumentParser(description='Process JSON data with a given window size.')
    parser.add_argument('--input_file', type=str, required=True, help='Path to the input JSON file.')
    parser.add_argument('--window_size', type=int, required=True, help='The window size as an integer.')

    # Parse arguments
    args = parser.parse_args()

    # Process the input
    #events_json_data = process_input(json_data_raw, window_size)
    events_json_data, window_size = process_input(args)
    # ------ CODE TO USE IN THE FINAL VERSION ------
    '''

    # ------ debug code ------
    try:
        with open("events.json", 'r') as file:
            json_data_raw = file.read()
    except Exception as e:
        print(json.dumps({'error': f'Failed to read input file: {str(e)}'}))
        sys.exit(1)
    window_size = 10

    events_json_data = json.loads(json_data_raw)
    # ------ debug code ------

    # Keep only 'timestamp' and 'duration'
    events_json_data = remove_unnecessary_fields(events_json_data)

    # Create output file
    output_file = create_output_file(events_json_data)

    # Fill average_delivery_time column
    output_file = fill_avg_delivery_time(events_json_data, output_file, window_size)

    # Print output file
    print_output_file(output_file)

