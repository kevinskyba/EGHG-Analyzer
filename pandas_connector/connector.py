import asyncio
import json
import pandas as pd


singleton_task = None  # There can ever only be one task
dataframe = None
constants = dict()
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


def setup(**kwargs):
    global port, ip
    if "port" in kwargs:
        port = kwargs["port"]
    if "ip" in kwargs:
        ip = kwargs["ip"]


types = ["object", "int64", "float64", "bool", "datetime64", "vector2"]


def _convert_type(_type, column, _df):
    if _type == "float64":
        _df[column] = _df[column].replace(',', '.', regex=True).astype(_type)
    if _type == "vector2":
        _df[[column + ".x", column + ".y"]] = _df[column]\
            .str.replace('(', '').str.replace(')', '')\
            .str.split(",", expand=True)
        _df[column + ".x"] = _df[column + ".x"].astype("float64")
        _df[column + ".y"] = _df[column + ".y"].astype("float64")
    if _type == "bool":
        _df[column] = _df[column]\
            .replace('False', False)\
            .replace('True', True).astype("bool")

    return _df


def connect(n=200):
    async def process_msgs(_previous_task):
        global dataframe
        if _previous_task is not None:
            disconnect(_previous_task)
            await asyncio.sleep(1)

        msgs = tcp_connection(ip, port, lambda: True)
        first_message = True
        while True:
            try:
                msg = await msgs.__anext__()

                if first_message:
                    for idx, key in enumerate(msg["Keys"]):
                        constants[key] = msg["Values"][idx]
                    first_message = False
                else:
                    row = dict()
                    for idx, column in enumerate(msg["Columns"]):
                        row[column] = msg["Values"][idx]
                    dataframe = dataframe.append(row, ignore_index=True).tail(n)
                    for idx, column in enumerate(msg["Columns"]):
                        _type = types[msg["Types"][idx]]
                        dataframe = _convert_type(_type, column, dataframe)
            except:
                break

    global singleton_task
    global dataframe
    dataframe = pd.DataFrame()
    process_task = asyncio.create_task(process_msgs(singleton_task))

    if singleton_task is not None:
        singleton_task = None
    singleton_task = process_task

    return process_task


def disconnect(task):
    task.cancel()
