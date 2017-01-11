import os
import asyncio
from migration.migration import Migration
import pytest
from pytest_lamp import asyncserver
import whisper


def test_migration_example():
    pass


def test_extract_wsp(monkeypatch):
    worker = Migration('/opt/graphite/storage/whisper/zon',
                       '127.0.0.1', 2003)
    def mock_return(path):
        yield ('/opt/graphite/storage/whisper/zon', [], ['where.wsp'])
    monkeypatch.setattr(os, 'walk', mock_return)
    relative_path, full_path = next(worker._extract_wsp())
    assert relative_path == '/where.wsp' 
    assert full_path == '/opt/graphite/storage/whisper/zon/where.wsp'


@pytest.mark.asyncio
@asyncserver('127.0.0.1', 2003)
async def test_graphite_connect():
    loop = asyncio.get_event_loop()
    worker = Migration('/opt/graphite/storage/whisper/zon',
                       '127.0.0.1', 2003, loop=loop)
    await worker.connect_to_graphite()

    message = "pytest-lamp"
    reader = worker.graphite_conn._reader
    writer = worker.graphite_conn._writer
    writer.write(message.encode("ascii"))
    await writer.drain()
    writer.write_eof()
    await  writer.drain()
    data = (await reader.read()).decode("utf-8")
    writer.close()
    assert message == data
    await worker.close_conn_to_graphite()


@pytest.mark.asyncio
async def test_read_from_wsps(monkeypatch):
    loop = asyncio.get_event_loop()
    worker = Migration('/opt/graphite/storage/whisper/zon',
                       '127.0.0.1', 2003, loop=loop)
    def fetch_mock_return(path, i):
        return ((1483668388, 1483668392, 2), [7, 8])

    monkeypatch.setattr(whisper, 'fetch', fetch_mock_return)

    def walk_mock_return(path):
        yield ('/opt/graphite/storage/whisper/zon', [], ['where.wsp'])
    monkeypatch.setattr(os, 'walk', walk_mock_return)

    await worker.read_from_wsps()

    num = worker.queue.qsize()
    # two datapoints and one terminator
    assert num == 3
    data1 = await worker.queue.get()
    data2 = await worker.queue.get()
    terminator = await worker.queue.get()
    assert data1 == ('zon.where', 7, 1483668388)
    assert data2 == ('zon.where', 8, 1483668390)
    assert terminator == None


@pytest.mark.asyncio
@asyncserver('127.0.0.1', 2003)
async def test_write_to_graphite():
    loop = asyncio.get_event_loop()
    worker = Migration('/opt/graphite/storage/whisper/zon',
                       '127.0.0.1', 2003, loop=loop)
    await worker.connect_to_graphite()

    # prefill some data into queue
    data = ('zon.test.metric', 7, 1483668388)
    await worker.queue.put(data)
    await worker.queue.put(None)
    # send data
    await worker.write_to_graphite()
    await worker.close_conn_to_graphite()


async def handler(reader, writer):
    data = (await reader.read())
    assert data == 'mondev 7 1483668388\n'
    writer.write(data)
    await writer.drain()
    writer.close()


@pytest.mark.asyncio
async def test_send_one_wsp(monkeypatch):
    loop = asyncio.get_event_loop()
    host = '127.0.0.1'
    port = 2003
    server = await asyncio.start_server(handler, host, port)
    worker = Migration('/opt/graphite/storage/whisper/zon',
                       host, port, loop=loop)
    await worker.connect_to_graphite()
    def fetch_mock_return(path, i):
        return ((1483668388, 1483668390, 2), [7])

    monkeypatch.setattr(whisper, 'fetch', fetch_mock_return)

    def exist_mock_return(path):
        return True

    monkeypatch.setattr(os.path, 'exists', exist_mock_return)
    storage = '/zon/where'
    metric = 'velocity'
    new_metric = 'mondev'

    await worker.send_one_wsp(storage, metric, new_metric)    
    server.close()


@pytest.mark.asyncio
async def test_run(monkeypatch):
    loop = asyncio.get_event_loop()
    host = '127.0.0.1'
    port = 2003
    server = await asyncio.start_server(handler, host, port)
    worker = Migration('/opt/graphite/storage/whisper/zon',
                       host, port, loop=loop)
    await worker.connect_to_graphite()
    def fetch_mock_return(path, i):
        return ((1483668388, 1483668390, 2), [7])
    monkeypatch.setattr(whisper, 'fetch', fetch_mock_return)
    def walk_mock_return(path):
        yield ('/opt/graphite/storage/whisper/zon', [], ['where.wsp'])
    monkeypatch.setattr(os, 'walk', walk_mock_return)

    await worker.run()    
    server.close()


@pytest.mark.asyncio
async def test_run_with_context_manager(monkeypatch):
    loop = asyncio.get_event_loop()
    host = '127.0.0.1'
    port = 2003
    server = await asyncio.start_server(handler, host, port)
    def fetch_mock_return(path, i):
        return ((1483668388, 1483668390, 2), [7])
    monkeypatch.setattr(whisper, 'fetch', fetch_mock_return)
    def walk_mock_return(path):
        yield ('/opt/graphite/storage/whisper/zon', [], ['where.wsp'])
    monkeypatch.setattr(os, 'walk', walk_mock_return)
    async with Migration('/opt/graphite/storage/whisper/zon',
                       host, port, loop=loop) as worker:
        await worker.run()    
    server.close()
