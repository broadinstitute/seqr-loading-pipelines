import collections
import logging
import math
from hail_scripts.v01.utils.gcloud_utils import get_local_or_gcloud_file_stats, google_bucket_file_iter

logger = logging.getLogger()

MAX_SAMPLES_PER_INDEX = 250


def _parse_family_ids_from_fam_file(fam_file_path):
    """Takes a local or gs:// file path and returns a mapping of sample_id to family id"""
    if get_local_or_gcloud_file_stats(fam_file_path) is None:
        raise ValueError("%s not found" % fam_file_path)

    family_ids = set()
    sample_id_to_family_id = collections.OrderedDict()
    for line_counter, line in enumerate(google_bucket_file_iter(fam_file_path)):
        if line.startswith("#") or line.strip() == "":
            continue
        fields = line.split("\t")
        if len(fields) < 6:
            raise ValueError("Unexpected .fam file format on line %s: %s" % (line_counter+1, line))

        family_id = fields[0]
        sample_id = fields[1]

        family_ids.add(family_id)
        sample_id_to_family_id[sample_id] = family_id

    logger.info("Parsed %s families and %s individuals from %s" % (
        len(family_ids), line_counter + 1, fam_file_path))

    return sample_id_to_family_id


def _compute_sample_groups(vds_sample_ids, sample_id_to_family_id, num_samples_per_group):
    """Assigns samples to sample groups so that all samples in a family are placed in the same group.

    Args:
        vds_sample_ids (list): Sample ids parsed from the variant callset.
        sample_id_to_family_id (dict): Mapping of individual id to family id
        num_samples_per_group (int): when the number of samples in a group exceeds this,
            a new group will be started. This is not a hard limit - keeping all samples in a family
            in the same group takes precedence.
    Returns:
        A list of lists of samples.
    """
    sample_groups_by_family = collections.defaultdict(list)
    for sample_id in vds_sample_ids:
        family_id = sample_id_to_family_id[sample_id]
        sample_groups_by_family[family_id].append(sample_id)

    sample_groups = []
    current_sample_group = []
    for sample_id in vds_sample_ids:
        family_id = sample_id_to_family_id[sample_id]
        sample_group = sample_groups_by_family.pop(family_id, None)
        if not sample_group:
            continue
        current_sample_group += sample_group
        if len(current_sample_group) >= num_samples_per_group:
            sample_groups.append(current_sample_group)
            current_sample_group = []

    if current_sample_group:
        sample_groups.append(current_sample_group)

    logger.info("==> sample groups: ")
    for i, sample_group in enumerate(sample_groups):
        logger.info("==> %s individuals in sample group %s: %s" % (
            len(sample_group), i, ", ".join(["%s:%s" % (sample_id_to_family_id[sample_id], sample_id) for sample_id in sample_group])))

    return sample_groups


def _check_for_extra_sample_ids_in_fam_file(vds_sample_ids, sample_id_to_family_id):
    """Raises a ValueError if any of the sample ids in sample_id_to_family_id are not in vds_sample_ids"""
    sample_ids_in_fam_file_and_not_in_vds = []
    for sample_id in sample_id_to_family_id:
        if sample_id not in vds_sample_ids:
            sample_ids_in_fam_file_and_not_in_vds.append(sample_id)

    if sample_ids_in_fam_file_and_not_in_vds:
        raise ValueError("%s sample ids from .fam file not found in vds.\nvds ids: '%s'\nfam file ids not in vds: '%s'" % (
            len(sample_ids_in_fam_file_and_not_in_vds),
            "', '".join(vds_sample_ids),
            "', '".join(sample_ids_in_fam_file_and_not_in_vds)))


def _check_for_extra_sample_ids_in_vds(vds_sample_ids, sample_id_to_family_id):
    """Raises a ValueError if any of the sample ids in vds_sample_ids are not in sample_id_to_family_id"""
    sample_ids_in_vds_and_not_in_fam_file = []
    for sample_id in vds_sample_ids:
        if sample_id not in sample_id_to_family_id:
            sample_ids_in_vds_and_not_in_fam_file.append(sample_id)

    if sample_ids_in_vds_and_not_in_fam_file:
        raise ValueError("%s sample ids from vds not found in .fam file\nvds ids: '%s'\nvds ids not in fam file: '%s'" % (
		len(sample_ids_in_vds_and_not_in_fam_file),
		"', '".join(vds_sample_ids),
	    	"', '".join(sample_ids_in_vds_and_not_in_fam_file)))


def compute_sample_groups_from_fam_file(fam_file_path,
                                        vds_sample_ids,
                                        max_samples_per_index=225,
                                        ignore_extra_sample_ids_in_vds=False,
                                        ignore_extra_sample_ids_in_fam_file=False):
    """Takes a list of sample ids from a variant callset, and a .fam file path that describes how
    samples are related to eachother. Groups the sample ids into groups of size
    max_samples_per_index, and returns the groups as a list of lists of sample ids.

    Args:
        fam_file_path (string): path of .fam file. This can be a local file path or a gs:// google bucket path.
        vds_sample_ids (list): list of sample ids from the variant callset
        max_samples_per_index (int): max number of samples to put in one sample group.
        ignore_extra_sample_ids_in_vds (bool): if False, an error will be raised if any sample ids
            are in the vds_sample_ids but not in the .fam file.
        ignore_extra_sample_ids_in_fam_file (bool): if False, an error will be raised if any sample
            ids are in the .fam file but not in the vds_sample_ids list.
    Return:
        A list of lists of sample ids.
    """

    NUM_INDEXES = int(math.ceil(float(len(vds_sample_ids)) / min(max_samples_per_index, MAX_SAMPLES_PER_INDEX)))
    NUM_SAMPLES_PER_INDEX = int(math.ceil(float(len(vds_sample_ids)) / NUM_INDEXES))

    sample_id_to_family_id = _parse_family_ids_from_fam_file(fam_file_path)

    if ignore_extra_sample_ids_in_fam_file:
        vds_sample_ids_set = set(vds_sample_ids)
        for sample_id in list(sample_id_to_family_id.keys()):
            if sample_id not in vds_sample_ids_set:
                del sample_id_to_family_id[sample_id]
    else:
        _check_for_extra_sample_ids_in_fam_file(vds_sample_ids, sample_id_to_family_id)

    if ignore_extra_sample_ids_in_vds:
        vds_sample_ids = list(set(vds_sample_ids) & set(sample_id_to_family_id.keys()))
    else:
        _check_for_extra_sample_ids_in_vds(vds_sample_ids, sample_id_to_family_id)


    if len(sample_id_to_family_id) == len(vds_sample_ids):
        sort_order = list(sample_id_to_family_id.keys())
        sorted_vds_sample_ids = sorted(vds_sample_ids, key=lambda sample_id: sort_order.index(sample_id))
    else:
        sorted_vds_sample_ids = vds_sample_ids

    sample_groups = _compute_sample_groups(sorted_vds_sample_ids, sample_id_to_family_id, NUM_SAMPLES_PER_INDEX)

    return sample_groups




#sample_groups = [
#   samples[0:100],
#   samples[100:200],
#   samples[200:300],
#   samples[300:400],
#   samples[400:501],
#   samples[501:602],
#   samples[602:701],
#   samples[701:802],
#   samples[802:905],
#]
# 5 indexes
#vds_sample_ids[0:200],
#vds_sample_ids[200:400],
#vds_sample_ids[400:602],
#vds_sample_ids[602:802],
#vds_sample_ids[802:905],

# 4 indexes
#vds_sample_ids[0:225],
#vds_sample_ids[225:449],
#vds_sample_ids[449:674],
#vds_sample_ids[674:900],
#vds_sample_ids[900:1125],

# 3 indexes
#vds_sample_ids[0:300],
#vds_sample_ids[300:602],   # split on family boundaries
#vds_sample_ids[602:900],

# 1 index
#vds_sample_ids,
#]

