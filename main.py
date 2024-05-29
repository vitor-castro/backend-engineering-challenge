
# python3 main.py --input_file events.json --window_size 10

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

import json
import argparse
import os
import sys
from datetime import datetime, timedelta


def process_input(json_data_raw, window_size):
    try:
        # Parse JSON data
        json_data = json.loads(json_data_raw)

        # Log the received data for demonstration
        print("Received JSON data:")
        print(json.dumps(json_data, indent=4))

        print(f"Received window size: {window_size}")

        return json_data

    except json.JSONDecodeError as e:
        print(json.dumps({'error': f'Invalid JSON data: {str(e)}'}))
    except Exception as e:
        print(json.dumps({'error': str(e)}))


def removeUnnecessaryFields(json_data):
    filtered_data = []
    for translation in json_data:
        filtered_translation = {
            'timestamp': translation['timestamp'],
            'duration': translation['duration']
        }
        # Append the filtered translation to the list
        filtered_data.append(filtered_translation)

    return filtered_data


def createOutputFile(events_json_data):
    # Parse timestamps in events_json_data to datetime objects
    timestamps = []
    for entry in events_json_data:
        timestamps.append(datetime.strptime(entry["timestamp"], "%Y-%m-%d %H:%M:%S.%f"))

    # TODO: em vez de usar min e max uso apenas indice 0 e indice 'size'-1 pq assumimos que vem ordenado logo
    min_timestamp = min(timestamps)
    max_timestamp = max(timestamps)

    # Create output_file with the desired structure
    output_file = []
    current_time = min_timestamp.replace(second=0, microsecond=0)
    end_time = max_timestamp.replace(second=0, microsecond=0) + timedelta(minutes=1)

    while current_time <= end_time:
        output_file.append({"timestamp": current_time.strftime("%Y-%m-%d %H:%M:00"), "average_delivery_time": -1})
        current_time += timedelta(minutes=1)

    return output_file


def getAverageDeliveryTime(events_json_data, current_timestamp, window_size):
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


def fillAvgDeliveryTime(events_json_data, output_file, window_size):

    for i in range(0, len(output_file)):
        avg_delivery_time = getAverageDeliveryTime(events_json_data, output_file[i]['timestamp'], window_size)
        output_file[i]['average_delivery_time'] = avg_delivery_time

    return output_file



if __name__ == '__main__':

    '''
    # Setup argument parser
    parser = argparse.ArgumentParser(description='Process JSON data with a given window size.')
    parser.add_argument('--input_file', type=str, required=True, help='Path to the input JSON file.')
    parser.add_argument('--window_size', type=int, required=True, help='The window size as an integer.')

    # Parse arguments
    args = parser.parse_args()

    # Read the JSON file
    try:
        with open(args.input_file, 'r') as file:
            json_data_raw = file.read()
    except Exception as e:
        print(json.dumps({'error': f'Failed to read input file: {str(e)}'}))
        sys.exit(1)

    window_size = args.window_size
    '''


    # ------ debug code ------
    try:
        with open("events.json", 'r') as file:
            json_data_raw = file.read()
    except Exception as e:
        print(json.dumps({'error': f'Failed to read input file: {str(e)}'}))
        sys.exit(1)
    window_size = 10
    # ------ debug code ------

    # Process the input
    events_json_data = process_input(json_data_raw, window_size)

    # Keep only 'timestamp' and 'duration'
    events_json_data = removeUnnecessaryFields(events_json_data)

    # Create output file
    output_file = createOutputFile(events_json_data)

    # Fill average_delivery_time column
    output_file = fillAvgDeliveryTime(events_json_data, output_file, window_size)

    # Print json for debug purposes
    print('Debug')
    for line in output_file:
        print(line)





