#!/usr/bin/python
# -*- coding: utf-8 -*-

"""simulation.py: IS 211 Assignment 5."""

__author__ = 'Adam Volin'
__email__ = 'Adam.Volin56@spsmail.cuny.edu'

import argparse
import urllib.request as request
import urllib.error
import csv

class Queue:
    """An abstract data type for queues."""

    def __init__(self):
        self.items = []

    def is_empty(self):
        return self.items == []

    def enqueue(self,item):
        self.items.insert(0, item)

    def dequeue(self):
        return self.items.pop()

    def size(self):
        return len(self.items)

class Server:
    """A server class."""
    def __init__(self):
        self.current_request = None
        self.time_remaining = 0

    def tick(self):
        if self.current_request != None:
            self.time_remaining = self.time_remaining - 1
            if self.time_remaining <= 0:
                self.current_request = None

    def busy(self):
        if self.current_request != None:
            return True
        else:
            return False

    def start_next(self, new_request):
        self.current_request = new_request
        self.time_remaining = new_request.get_time()

class Request:
    """A request class."""
    def __init__(self, request):
        self.timestamp = int(request[0])
        self.process_time = int(request[2])

    def get_stamp(self):
        return self.timestamp

    def get_time(self):
        return self.process_time

    def wait_time(self, previous_time):
        return self.timestamp - int(previous_time)

def getNextAvailableServerFromQueue(server_queue):
    server = server_queue.dequeue()
    
    while server.busy():
        server.tick()
        server_queue.enqueue(server)
        server = server_queue.dequeue()
    
    return server

def simulateOneServer(file):
    """Simulates one server."""
    server = Server()
    request_queue = Queue()
    wait_times = []
    previous_time = 0

    for line, row in enumerate(csv.reader(file)):
        request = Request(row)
        request_queue.enqueue(request)

    while not request_queue.is_empty():
        if not server.busy() and not request_queue.is_empty():
            next_request = request_queue.dequeue()
            wait_times.append(next_request.wait_time(previous_time))
            server.start_next(next_request)
            previous_time = next_request.timestamp
        
        server.tick()

    average_wait_time = sum(wait_times) / len(wait_times)
    print("Average wait %6.2f secs %3d requests remaining." % (average_wait_time, request_queue.size()))

def simulateManyServers(file, server_count):
    """Simulates many servers."""
    request_queue = Queue()
    server_queue = Queue()
    wait_times = []
    previous_time = 0

    for line, row in enumerate(csv.reader(file)):
        request = Request(row)
        request_queue.enqueue(request)

    i = 0
    while i < server_count:
        server_queue.enqueue(Server())
        i += 1

    while not request_queue.is_empty():
        server = getNextAvailableServerFromQueue(server_queue)
        
        if not server.busy() and not request_queue.is_empty():
            next_request = request_queue.dequeue()
            wait_times.append(next_request.wait_time(previous_time))
            server.start_next(next_request)
            previous_time = next_request.timestamp
        
        server.tick()
        server_queue.enqueue(server)

    average_wait_time = sum(wait_times) / len(wait_times)
    print("Average wait %6.2f secs %3d requests remaining." % (average_wait_time, request_queue.size()))


def downloadData(url):
    """Accepts a URL as a string and opens it.

    Parameters:
        url (string): the url to be opened

    Example:
        >>> downloadData(
            'http://s3.amazonaws.com/cuny-is211-spring2015/requests.csv')
    """

    response = request.urlopen(url)
    return response.read().decode('utf-8').splitlines()

def main():
    """The function that runs when the program is executed."""

    # Setup --file and --servers arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--file',
                        help='The path to the CSV file to read for request inputs.',
                        type=str
                    )
    parser.add_argument('--servers',
                        help='The number of servers to simulate.',
                        type=int
                    )
    args = parser.parse_args()

    # Check for file argument
    if args.file:
        try:
            csvData = downloadData(args.file)
        except (urllib.error.URLError, urllib.error.HTTPError):
            print('There was an error reading the CSV file at the provided path. \
                    Please try a different path.')
            sys.exit()

        # Check for servers argument to determine function to call
        if args.servers and args.servers > 1:
            simulateManyServers(csvData, args.servers)
        elif not args.servers or (args.servers and args.servers <= 1):
            simulateOneServer(csvData)

    else:
        print('The --file parameter is required.')
        sys.exit()

if __name__ == '__main__':
    main()