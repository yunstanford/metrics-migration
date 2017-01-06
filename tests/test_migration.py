import os
import asyncio
from migration.migration import Migration
import pytest
from pytest_lamp import asyncserver


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
