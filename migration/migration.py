from aiographite.aiographite import connect
from aiographite.protocol import PlaintextProtocol
import asyncio
import whisper
import os


def default_schema_func(key):
    return key


def debug_print(s, debug):
    if debug:
        print(s)


class Migration:

    def __init__(self, directory, host, port,
                 prefix=None,
                 max_size_queue=None,
                 loop=None, schema_func=None,
                 protocol=None, debug=False):
        self.directory = directory
        self.host = host
        self.port = port
        self.prefix = prefix
        self.max_size = max_size_queue or 0
        self.loop = loop or asyncio.get_event_loop()
        self.schema_func = schema_func or default_schema_func
        self.protocol = protocol or PlaintextProtocol()
        self.graphite_conn = None
        self.debug = debug
        self.queue = asyncio.Queue(loop=self.loop)


    async def connect_to_graphite(self):
        self.graphite_conn = await connect(self.host,
                                           self.port,
                                           self.protocol,
                                           loop=self.loop)


    async def close_conn_to_graphite(self):
        await self.graphite_conn.close()


    async def run(self, directory=None, prefix=None):
        # If you have any addional directory that you wanna migrate
        if directory:
            self.directory = directory
        if prefix:
            self.prefix = prefix
        listener_task = asyncio.ensure_future(self.write_to_graphite())
        producer_task = asyncio.ensure_future(self.read_from_wsps())
        done, pending = await asyncio.wait(
            [listener_task, producer_task],
            return_when=asyncio.ALL_COMPLETED)


    async def write_to_graphite(self):
        """
        Consumes the metrics in Queue and send to target graphite.
        """
        debug_print("start writing to graphite", self.debug)
        while True:
            try:
                future = self.queue.get()
                data = await asyncio.wait_for(future, 10, loop=self.loop)
                if not data:
                    debug_print("There is no data anymore.", self.debug)
                    break
                metric, value, timestamp = data
                await self.graphite_conn.send(metric, value, timestamp)
                debug_print("writing {0}, {1}, {2}".format(metric, value, timestamp), self.debug)
            except asyncio.futures.TimeoutError:
                debug_print("writer is waiting too long...", self.debug)
                break


    async def read_from_wsps(self):
        """
        Read metrics from wsp file and then publish
        to an asyncio Queue.
        """
        debug_print("start reading from wsp", self.debug)
        prefix = self.prefix or os.path.basename(self.directory)
        for relative_path, full_path in self._extract_wsp():
            if full_path.endswith('.wsp'):
                metric_path = relative_path.replace('/', '.')[:-4]
                metric = "{0}{1}".format(prefix, metric_path)
                metric = self.schema_func(metric)
                try:
                    time_info, values = whisper.fetch(full_path, 0)
                except whisper.CorruptWhisperFile:
                    debug_print('Corrupt, skipping', self.debug)
                    continue
                metrics = zip(range(*time_info), values)
                for timestamp, value in metrics:
                    if value is not None:
                        # await asyncio.sleep(0.1)
                        await self.queue.put((metric, value, timestamp))
                        debug_print("reading {0}, {1}, {2}".format(metric, value, timestamp), self.debug)
        # Send singal to writer, there is no data anymore
        await self.queue.put(None)


    async def send_one_wsp(self, storage_dir, metric, new_metric):
        """
        Send one wsp back to graphite with new metric name
        """
        clean_pattern = metric.replace('\\', '')
        relative_path = clean_pattern.replace('.', '/')
        full_path = "{0}/{1}.wsp".format(storage_dir, relative_path)
        if os.path.exists(full_path):
            try:
                time_info, values = whisper.fetch(full_path, 0)
                metrics = zip(range(*time_info), values)
                for timestamp, value in metrics:
                    if value is not None:
                        await self.graphite_conn.send(new_metric, value, timestamp)
                        debug_print("writing {0}, {1}, {2}".format(new_metric, value, timestamp), self.debug)
            except whisper.CorruptWhisperFile as e:
                raise e
        else:
            debug_print("{0} doesn't exist".format(full_path), self.debug)


    def _extract_wsp(self):
        """
        avoid to put all wsp files into memory once, let's make
        it a generator.
        """
        directory = self.directory
        if directory.endswith('/'):
            directory = directory[:-1]
        for dirpath, dirnames, filenames in os.walk(directory):
            for filename in filenames:
                path = os.path.join(dirpath, filename)
                yield path[len(directory):], path
