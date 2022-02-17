import asyncio
import json
import time

singleton_task = None  # There can ever only be one task
file = ""


def setup_stream(**kwargs):
    global file
    if "file" in kwargs:
        file = kwargs["file"]


def get_stream(dequeue_object):

    async def process_msgs(_previous_task, _file_path,  _current_data):
        if _previous_task is not None:
            stop_stream(_previous_task)

        data = {}
        with open(_file_path) as json_file:
            data = json.load(json_file)

        delta_s = data["frameTime"]
        msgs = data["recordEntries"]

        counter = 0
        while True:
            try:
                if counter >= len(msgs):
                    counter = 0
                msg = msgs[counter]
                _current_data.append(msg)
                await asyncio.sleep(delta_s/3) # Not sure why I have to divide by 3.
                counter = counter + 1
            except:
                break

    global singleton_task
    global file
    process_task = asyncio.create_task(process_msgs(singleton_task, file, dequeue_object))

    if singleton_task is not None:
        singleton_task = None
    singleton_task = process_task

    return process_task


def stop_stream(task):
    task.cancel()
