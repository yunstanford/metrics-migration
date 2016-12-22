from aiographite.aiographite import connect
from aiographite.protocol import PlaintextProtocol
import asyncio
import whisper
import os


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


    async def connect_to_graphite(self):
        self.graphite_conn = await connect(self.host,
                                           self.port,
                                           self.protocol,
                                           loop=self.loop)


    async def close_conn_to_graphite(self):
        await self.graphite_conn.close()


    async def run(self):
        listener_task = asyncio.ensure_future(self.write_to_graphite())
        producer_task = asyncio.ensure_future(self.read_from_wsps())
        done, pending = await asyncio.wait(
            [listener_task, producer_task],
            return_when=asyncio.ALL_COMPLETED)


    async def write_to_graphite(self):
        """
        Consumes the metrics in Queue and send to target graphite.
        """
        print("start writing to graphite")
        while True:
            try:
                future = self.queue.get()
                data = await asyncio.wait_for(future, 10, loop=self.loop)
                if not data:
                    print("There is no data anymore.")
                    break
                metric, value, timestamp = data
                await self.graphite_conn.send(metric, value, timestamp)
                print("writing {0}, {1}, {2}".format(metric, value, timestamp))
            except asyncio.futures.TimeoutError:
                print("writer is waiting too long...")
                break


    async def read_from_wsps(self):
        """
        Read metrics from wsp file and then publish
        to an asyncio Queue.
        """
        print("start reading from wsp")
        prefix = os.path.basename(self.directory)
        for relative_path, full_path in self._extract_wsp():
            if full_path.endswith('.wsp'):
                metric_path = relative_path.replace('/', '.')[:-4]
                metric = "{0}{1}".format(prefix, metric_path)
                metric = self.schema_func(metric)
                try:
                    time_info, values = whisper.fetch(full_path, 0)
                except whisper.CorruptWhisperFile:
                    # print('Corrupt, skipping')
                    continue
                metrics = zip(range(*time_info), values)
                for timestamp, value in metrics:
                    if value is not None:
                        # await asyncio.sleep(0.1)
                        await self.queue.put((metric, value, timestamp))
                        print("reading {0}, {1}, {2}".format(metric, value, timestamp))
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
                        print("writing {0}, {1}, {2}".format(new_metric, value, timestamp))
            except whisper.CorruptWhisperFile as e:
                raise e
        else:
            print("{0} doesn't exist".format(full_path))


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