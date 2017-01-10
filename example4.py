from migration.migration import Migration
import asyncio


loop = asyncio.get_event_loop()
host = "127.0.0.1"
port = 2003
directory = '/Users/yunx/Documents/PROJECTS/metrics-migration/examples'


async def go():
    """
    Use context manager
    """
    async with Migration(directory, host, port, loop=loop, debug=True) as migration_worker:
        await migration_worker.run()


def main():
    loop.run_until_complete(go())
    loop.close()


if __name__ == '__main__':
    main()
