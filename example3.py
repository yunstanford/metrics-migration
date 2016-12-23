"""
This is a secrete schema conversion func for zon metrics migration.

We have two types of metrics need to migrate which should follow our
new metric schema pattern.

"""
from aiographite.graphite_encoder import GraphiteEncoder
import motor.motor_asyncio
import yaml
import asyncio
from pymongo.read_preferences import ReadPreference
import os


def secrete_schema_func(metric):
    pass


def load_config(config_path="/Users/yunx/Desktop/config.yaml"):
    with open(config_path) as fh:
        return yaml.load(fh.read())


def init_async_db():
    config = load_config()
    m_config = config["mongo"]
    db_config = {
        "hosts": m_config["servers"],
        "replicaset": m_config["replica_set"],
        "db_name": m_config["database"]
    }
    connection = motor.motor_asyncio.AsyncIOMotorClient(
        host=db_config["hosts"],
        replicaset=db_config["replicaset"],
        maxPoolSize=10
    )
    # connection.write_concern = {'w': 1, 'wtimeout': 15000}
    # connection.read_preference = ReadPreference.PRIMARY_PREFERRED
    db = connection[db_config["db_name"]]
    return db


async def go(storage_dir="/storage/whisper/zon"):
    print("=== Connecting to mongo ===")
    db = init_async_db()
    test_collection = db["tests"]
    verb_collection = db["verbs"]
    import time
    start = time.time()
    print("=== Connected ===")
    print("=== Start Iteration of tests collection ===")
    async for test in test_collection.find().limit(1):
        # TODO
        test_id = test["_id"]
        verb = await verb_collection.find_one({"test_id": test_id})
        print(test)
        print(verb)
        metric_prefix = generate_old_schema_for_test_metric(test, verb)
        print("metric_prefix: {0}".format(metric_prefix))
        clean_pattern = metric_prefix.replace('\\', '')
        relative_dir = clean_pattern.replace('.', '/')
        directory = os.path.join(storage_dir, relative_dir)
        print(directory)
    end = time.time()
    cost = int (end - start)
    print("=== Tasks Done ===")
    print("Takes: {0}".format(cost))


def generate_old_schema_for_test_metric(test, verb):
    """
    generate old type of metrics based on test definition.
    return metric_prefix.
    Example:
    metric_prefix.* includes:
    metric_prefix.failure
    Any additional metrics like:
    metric_prefix.success_count
    """
    metric_prefix = get_graphite_metric_prefix(test, verb)
    return metric_prefix


def generate_new_schema_for_test_metric(test, verb):
    """
    generate new type of metrics based on test definition
    """


def is_test_metric(metric):
    """
    old: zon.feature.env.host.name.type.*
    """
    # pass


def is_where_metric(metric):
    """
    where metric
    """
    # pass


################################
# clean key func used in Zon   #
################################

def clean_key_for_where(metric):
    """
    clean the key so it is a valid graphite metric.

    this is specifically how where keys are cleaned. In the future,
    we should replace this with the standard graphite clean_key.
    """
    return metric.replace('.', '_')\
                 .replace(':', '#')\
                 .replace(',', '##')\
                 .replace("-", "_")


def clean_key_for_test(metric):
    """
    clean the key so it is a valid graphite metric, specifically for a test
    metrics, which is escaped differently than the 'where' metric.

    This escaping was verified by reading all val_300 test names from the db
    and verifying that there is an associated graphite metric name with this
    escaping.
    """
    return metric.replace('-', '_')\
                 .replace(':', '#')\
                 .replace(' ', '_')


def generate_graphite_metric_for_test(metric_parts):
    """
    generate the graphite metrics, for a test.
    """
    # Replace '.' in all metric name parts with '_', and join all
    # parts with '.'
    # We have to deal with '.' separately because it is the delimiter between
    # metric parts.
    result = ".".join([s.replace(".","_") for s in metric_parts])
    result = clean_key_for_test(result)
    return result
    #return ".".join(map(lambda s: s.replace(".", "_").replace("-", "_"),
    #                    metric_parts))


def extract_info_from_where(wheres, fields=[]):
    '''
    @summary: Method fetches test's where info and returns info of specified fields

    Parameters:
        @param test: Test object which 'where' info needs to be extracted
        @type test: zonlib.db.test.Test object

        @param fields: List of fields which info is required to fetch from 'where'
        @type fields: List

    @return: Dict of fields as key and their respective value from test's 'where'
    '''
    return dict(
        field.split(':', 1) for field in wheres
        if field.count(',') == 0
        if field.split(':', 1)[0] in fields
    )


def get_graphite_metric_prefix(test, verb):
    """ for a test object, return the graphite metric prefix, fully escaped. """
    wheres = extract_info_from_where(test["where"], ["env", "feature", "host"])
    return generate_graphite_metric_for_test([
        wheres["feature"], wheres["env"],
        wheres["host"], test["name"], verb["type"]
    ])


########
# main #
########

loop = asyncio.get_event_loop()

def main():
    loop.run_until_complete(go())
    loop.close()


if __name__ == '__main__':
    main()




