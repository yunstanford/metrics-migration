from migration.migration import Migration
import asyncio


loop = asyncio.get_event_loop()
host = "127.0.0.1"
port = 2003
directory = '/Users/yunx/Documents/PROJECTS/metrics-migration/examples'


async def go():
    migration_worker = Migration(directory, host, port, loop=loop)
    await migration_worker.connect_to_graphite()
    await migration_worker.run()
    await migration_worker.close_conn_to_graphite()


def main():
    loop.run_until_complete(go())
    loop.close()


if __name__ == '__main__':
    main()