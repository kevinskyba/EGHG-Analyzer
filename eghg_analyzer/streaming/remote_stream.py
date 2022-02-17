import asyncio
import json

singleton_task = None  # There can ever only be one task
ip = ""
port = 0


async def tcp_connection(ip, port, running_func):
    reader = None
    write = None
    connection_future = asyncio.open_connection(ip, port)

    try:
        # Wait for 3 seconds, then raise TimeoutError
        reader, writer = await asyncio.wait_for(connection_future, timeout=3)
    except asyncio.TimeoutError:
        return

    while running_func():
        try:
            data = await reader.readuntil(b'\r')
        except:
            break
        data = data.decode()
        msg = json.loads(data)
        yield msg

    writer.close()
    await writer.wait_closed()


def setup_stream(**kwargs):
    global port, ip
    if "port" in kwargs:
        port = kwargs["port"]
    if "ip" in kwargs:
        ip = kwargs["ip"]


def get_stream(dequeue_object):

    async def process_msgs(_previous_task, _current_data):
        if _previous_task is not None:
            stop_stream(_previous_task)
            await asyncio.sleep(1)

        msgs = tcp_connection(ip, port, lambda: True)
        while True:
            try:
                msg = await msgs.__anext__()
                _current_data.append(msg)
            except:
                break

    global singleton_task
    process_task = asyncio.create_task(process_msgs(singleton_task, dequeue_object))

    if singleton_task is not None:
        singleton_task = None
    singleton_task = process_task

    return process_task


def stop_stream(task):
    task.cancel()
