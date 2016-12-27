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
Examples
-------------

Let's get started with several examples.

Example 1.

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


------------
Development
------------

Dev mode.
Need more unit tests.
