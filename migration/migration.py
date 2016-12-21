from aiographite.aiographite import connect
from aiographite.protocol import PlaintextProtocol
import asyncio
from concurrent.futures import ProcessPoolExecutor
import whisper


def default_schema_func(key):
    return key


class Migration:

    def __init__(self, directory, host, port,
                 max_size_queue=None,
                 loop=None, schema_func=None,
                 protocol=None):
        self.directory = directory
        self.host = host
        self.port = port
        self.max_size = max_size_queue or 0
        self.loop = loop or asyncio.get_event_loop()
        self.schema_func = schema_func or default_schema_func
        self.protocol = protocol or PlaintextProtocol()
        self.graphite_conn = None
        self.queue = asyncio.Queue(loop=self.loop)
        self.writer_executor = ProcessPoolExecutor(1)
        self.read_executor = ProcessPoolExecutor(4)


    async def connect_to_graphite(self):
        self.graphite_conn = await connect(self.host,
                                          self.port,
                                          self.protocol,
                                          loop=self.loop)

    async def close_conn_to_graphite(self):
        await self.graphite_conn.close()


    async def run(self):
        # spawn read workers
        read_future = self.loop.run_in_executor(self.read_executor, self.read_from_wsps)
        # spawn write workers
        write_future = self.loop.run_in_executor(self.writer_executor, self.write_to_graphite)
        await read_future
        await write_future


    async def write_to_graphite(self):
        """
        Consumes the metrics in Queue and send to target graphite.
        """
        while True:
            try:
                future = self.queue.get()
                metric, value, timestamp = await asyncio.wait_for(future, 20, loop=self.loop)
                await self.graphite_conn.send(metric, value, timestamp)
            except asyncio.futures.TimeoutError:
                break


    async def read_from_wsps(self):
        """
        Read metrics from wsp file and then publish
        to an asyncio Queue.
        """
        for relative_path, full_path in self._extract_wsp():
            if full_path.endswith('.wsp'):
                metric_path = relative_path.replace('/', '.')[:-4]
                try:
                    time_info, values = whisper.fetch(full_path, 0)
                except whisper.CorruptWhisperFile:
                    print 'Corrupt, skipping'
                    continue
                metrics = zip(range(*time_info), values)
                for timestamp, value in metrics:
                    if value is not None:
                        await self.queue.put((metric_path, value, timestamp))


    def _extract_wsp(self):
        """
        avoid to put all wsp files into memory once, let's make
        it a generator.
        """
        directory = self.directory
        if directory.endswith('/')
            directory = directory[:-1]
        for dirpath, dirnames, filenames in os.walk(directory):
            for filename in filenames:
                path = os.path.join(dirpath, filename)
                yield path[len(directory):], path