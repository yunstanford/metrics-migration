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

