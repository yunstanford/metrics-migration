metrics-migration
=================

.. image:: https://travis-ci.org/yunstanford/metrics-migration.svg?branch=master
    :alt: build status
    :target: https://travis-ci.org/yunstanford/metrics-migration

.. image:: https://coveralls.io/repos/github/yunstanford/metrics-migration/badge.svg?branch=master
    :alt: coverage status
    :target: https://coveralls.io/github/yunstanford/metrics-migration?branch=master


a simple tool to help you migrate your graphite metrics


---------------------------
What is metrics-migration ?
---------------------------

metrics-migration a Python3 tool (async io), designed to help graphite users to migrate metrics
in several ways.

* Migrate whole storage directory.
* Migrate specific whisper file (with new metric name).
* Allow schema change during Migration (Provide schema rule).


-------------
Install
-------------

You can install aiographite globally with any Python package manager:

.. code::

    pip3 install metrics-migration


-------------
Dependency
-------------

Whisper on pypi only supports python2, we should download whisper egg from github.

.. code::

    pip3 install https://github.com/graphite-project/whisper/tarball/feature/py3


-------------
Examples
-------------

Let's get started with several examples.

Example 1. A simple example to migrate a directory.

.. code::

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


Example 2. Migrate a whipser file with new metric.

.. code::

    from migration.migration import Migration
    import asyncio


    loop = asyncio.get_event_loop()
    host = "127.0.0.1"
    port = 2003
    directory = '/Users/yunx/Documents/PROJECTS/metrics-migration/examples'
    storage_dir = '/Users/yunx/Documents/PROJECTS/metrics-migration'
    metric = "examples.committedPoints"
    new_metric = 'hello.world'


    async def go():
        migration_worker = Migration(directory, host, port, loop=loop, debug=True)
        await migration_worker.connect_to_graphite()
        await migration_worker.send_one_wsp(storage_dir, metric, new_metric)
        await migration_worker.close_conn_to_graphite()


    def main():
        loop.run_until_complete(go())
        loop.close()


    if __name__ == '__main__':
        main()


Example 3. Have Multiple Directories to migrate ?

.. code::

    from migration.migration import Migration
    import asyncio


    loop = asyncio.get_event_loop()
    host = "127.0.0.1"
    port = 2003
    directories_and_prefixes = [
        ('/opt/graphite/metrics-migration/zon1', 'zon1'),
        ('/opt/graphite/metrics-migration/zon2', 'zon2'),
    ]


    async def go():
        migration_worker = Migration(directory, host, port, loop=loop)
        await migration_worker.connect_to_graphite()
        for (directory, prefix) in directories_and_prefixes:
            wait migration_worker.run(directory=directory, prefix=prefix)
        await migration_worker.run()
        await migration_worker.close_conn_to_graphite()


    def main():
        loop.run_until_complete(go())
        loop.close()


    if __name__ == '__main__':
        main()


Example 4. async with context manager

.. code::

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


------------
Development
------------

Run unit tests.

.. code::
    
    ./uranium test


