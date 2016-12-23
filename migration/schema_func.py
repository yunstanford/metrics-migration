"""
This is a secrete schema conversion func for zon metrics migration.

We have two types of metrics need to migrate which should follow our
new metric schema pattern.

"""
from aiographite.graphite_encoder import GraphiteEncoder


def secrete_schema_func(metric):
	pass


def connect_to_db(hosts) :
	pass



def is_test_metric(metric):
	"""
	old: zon.feature.env.host.name.type.*
	"""
	pass


def is_where_metric(metric):
	pass


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