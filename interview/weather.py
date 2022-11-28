import csv
from dataclasses import dataclass
import collections
import math

"""
The idea is to grouping data stream into buckets of (station_name, date)
and process the data stream as they come in and write
"""

class TemperatureProcessor:
    start: str = None
    end: str = None
    high:str = -math.inf
    low:str = math.inf
    output_fields = ['Station Name','Date','Min Temp','Max Temp','First Temp','Last Temp']

    # data stream is reverse chronological order, 
    # ie. first temp is the last data, while last temp is first data
    def process(self, temp) -> None:
        self.start = temp
        if self.end is None: self.end = temp
        self.high = max(self.high, temp)
        self.low = min(self.low, temp)

class Solution:
    def __init__(self) -> None:
        self.memo = collections.defaultdict(TemperatureProcessor)

    def process_stream(self, reader):
        csv_reader = csv.DictReader(reader)
        for d in csv_reader:
            station_name = d['Station Name']
            date = d['Measurement Timestamp'].split(' ')[0]
            temp = float(d['Air Temperature'])
            hash_key = (station_name, date)
            self.memo[hash_key].process(temp)

    def write_to_csv(self, writer):
        fields = ['Station Name','Date','Min Temp','Max Temp','First Temp','Last Temp']
        csv_writer = csv.DictWriter(writer, fieldnames=TemperatureProcessor.output_fields)
        csv_writer.writeheader()
        for (name, date), v in self.memo.items():
            csv_writer.writerow({
                'Station Name': name,
                'Date': date,
                'Min Temp': v.low,
                'Max Temp': v.high,
                'First Temp': v.start,
                'Last Temp': v.end
            })

def process_csv(reader, writer):
    s = Solution()
    s.process_stream(reader=reader)
    s.write_to_csv(writer=writer)
