import gzip
import xml.etree.ElementTree as ET
from collections import Counter
import requests

CLINVAR_GOLD_STARS_LOOKUP = {
    'no classification for the single variant': 0,
    'no classification provided': 0,
    'no assertion criteria provided': 0,
    'no classifications from unflagged records': 0,
    'criteria provided, single submitter': 1,
    'criteria provided, conflicting classifications': 1,
    'criteria provided, multiple submitters, no conflicts': 2,
    'reviewed by expert panel': 3,
    'practice guideline': 4,
}


WEEKLY_XML_RELEASE = (
    'https://ftp.ncbi.nlm.nih.gov/pub/clinvar/xml/ClinVarVCVRelease_00-latest.xml.gz'
)


def extract_variant_info(elem):
    variation_id = elem.attrib['VariationID']  # will raise KeyError if missing
    positions = {}

    classified_record = elem.find('ClassifiedRecord')
    if classified_record is None:
        return None

    location_blocks = classified_record.findall('SimpleAllele/Location')
    if not location_blocks:
        return None

    for loc in location_blocks:
        for seq_loc in loc.findall('SequenceLocation'):
            if (
                seq_loc.get('referenceAlleleVCF')
                and seq_loc.get('alternateAlleleVCF')
                and seq_loc.get('start')
            ):
                positions[seq_loc.attrib['Assembly']] = {
                    'chrom': seq_loc.attrib['Chr'],
                    'pos': int(seq_loc.attrib['start']),
                    'ref': seq_loc.attrib['referenceAlleleVCF'],
                    'alt': seq_loc.attrib['alternateAlleleVCF'],
                }

    if not positions:
        return None

    review_status = classified_record.find(
        'Classifications/GermlineClassification/ReviewStatus',
    )
    if review_status is None:
        # From inspecting the XML, records without a GermlineClassification/ReviewStatus
        # had a SomaticClinicalImpact.  These seem irrelevant for us?
        return None
    pathogenicity_node = classified_record.find(
        'Classifications/GermlineClassification/Description',
    )
    pathogenicity = pathogenicity_node.text.split(';')[0]
    assertion = (
        pathogenicity_node.text.split(';')[1].strip()
        if len(pathogenicity_node.text.split(';')) > 1
        else None
    )
    conflicting_pathogenicities = dict(Counter(
        cp.text.replace(
            'Uncertain Significance',
            'Uncertain significance',
        ) for cp in classified_record.findall(
            'ClinicalAssertionList/ClinicalAssertion/Classification/GermlineClassification'
        )
    ))
    submitters = {
        s.attrib['SubmitterName']
        for s in classified_record.findall(
            'ClinicalAssertionList/ClinicalAssertion/ClinVarAccession',
        )
    }
    conditions = {
        c.attrib['Name']
        for c in classified_record.findall('TraitMappingList/TraitMapping/MedGen')
    }
    return {
        'variantId': f'{positions["GRCh38"]["chrom"]}-{positions["GRCh38"]["pos"]}-{positions["GRCh38"]["ref"]}-{positions["GRCh38"]["alt"]}',
        'alleleId': int(classified_record.find('SimpleAllele').attrib['AlleleID']),
        'goldStars': CLINVAR_GOLD_STARS_LOOKUP[review_status.text],
        'pathogenicity': pathogenicity,
        'conflictingPathogenicities': list(conflicting_pathogenicities.items()) if len(conflicting_pathogenicities) > 1 else None,
        'assertion': assertion,
        'submitters': submitters,
        'conditions': conditions,
    }

import json
def set_default(obj):
    if isinstance(obj, set):
        return list(obj)
    raise TypeError

with requests.get(WEEKLY_XML_RELEASE, stream=True, timeout=5) as r:
    r.raise_for_status()
    for event, elem in ET.iterparse(gzip.GzipFile(fileobj=r.raw), events=('end',)):
        if elem.tag == 'VariationArchive':
            variant_info = extract_variant_info(elem)
            if variant_info:
                print(json.dumps(variant_info, default=set_default))
            elem.clear()
    # for byte_line in gzip.GzipFile(fileobj=r.raw):
    #    print(byte_line.decode().strip())
