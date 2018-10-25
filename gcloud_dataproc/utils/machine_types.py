from collections import namedtuple, OrderedDict

#MACHINE_TYPES = ["n1-standard-1"] + [
#    "n1-%s-%s" % (i, j) for i in ["standard", "highmem", "highcpu"] for j in ["2", "4", "8", "16", "32", "64"]
#]


# Machine type	Virtual CPUs	Memory	Price (USD)	Preemptible price (USD)
# copy-pasted from https://cloud.google.com/compute/pricing#machinetype

_pricing_table = """
n1-standard-1	1	3.75GB	$0.0475	$0.0100
n1-standard-2	2	7.5GB	$0.0950	$0.0200
n1-standard-4	4	15GB	$0.1900	$0.0400
n1-standard-8	8	30GB	$0.3800	$0.0800
n1-standard-16	16	60GB	$0.7600	$0.1600
n1-standard-32	32	120GB	$1.5200	$0.3200
n1-standard-64	64	240GB	$3.0400	$0.6400
n1-highmem-2	2	13GB	$0.1184	$0.0250
n1-highmem-4	4	26GB	$0.2368	$0.0500
n1-highmem-8	8	52GB	$0.4736	$0.1000
n1-highmem-16	16	104GB	$0.9472	$0.2000
n1-highmem-32	32	208GB	$1.8944	$0.4000
n1-highmem-64	64	416GB	$3.7888	$0.8000
n1-highcpu-2	2	1.80GB	$0.0709	$0.0150
n1-highcpu-4	4	3.60GB	$0.1418	$0.0300
n1-highcpu-8	8	7.20GB	$0.2836	$0.0600
n1-highcpu-16	16	14.40GB	$0.5672	$0.1200
n1-highcpu-32	32	28.80GB	$1.1344	$0.2400
n1-highcpu-64	64	57.6GB	$2.2688	$0.4800
"""

MachineType = namedtuple("MachineType", ["machine_type", "cpus", "memory_gb", "price_per_hour", "preemptible_price_per_hour"])

# parse the _pricing_table
MACHINE_TYPES = OrderedDict()
for row in _pricing_table.strip().split("\n"):
    fields = row.split()
    machine_type = fields[0]
    fields[1] = int(fields[1])
    fields[2] = float(fields[2].replace("GB", ""))
    fields[3] = float(fields[3].lstrip("$"))
    fields[4] = float(fields[4].lstrip("$"))
    MACHINE_TYPES[machine_type] = MachineType._make(fields)

#for machine_type, info in MACHINE_TYPES.items():
#    print("%20s: %s" % (machine_type, info))


def get_cost(machine_type, hours=1.0, is_preemptible=False):
    """Returns floating point number = the total cost (in dollars) of running the given machine type for the given number of hours"""

    if machine_type not in MACHINE_TYPES:
        raise ValueError("Invalid machine type: " + machine_type)

    mt = MACHINE_TYPES[machine_type]
    return (mt.preemptible_price_per_hour if is_preemptible else mt.price_per_hour) * float(hours)
