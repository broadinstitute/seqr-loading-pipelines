#!/usr/bin/env python3
import argparse
import os

import hail as hl

from hail_scripts.reference_data.constants import GCS_PREFIXES
from hail_scripts.utils.clinvar import (
    download_and_import_latest_clinvar_vcf,
    CLINVAR_GOLD_STARS_LOOKUP,
)
from hail_scripts.utils.hail_utils import write_ht

CLINVAR_HT_PATH = 'clinvar/clinvar.GRCh{genome_version}.{timestamp}.ht'
PARTITIONS = 100 # per https://github.com/broadinstitute/seqr-loading-pipelines/pull/383

CLINVAR_SIGNIFICANCES = {sig: i for i, sig in enumerate([
    'Pathogenic', 'Pathogenic,_risk_factor', 'Pathogenic,_Affects', 'Pathogenic,_drug_response',
    'Pathogenic,_drug_response,_protective,_risk_factor', 'Pathogenic,_association', 'Pathogenic,_other',
    'Pathogenic,_association,_protective', 'Pathogenic,_protective', 'Pathogenic/Likely_pathogenic',
    'Pathogenic/Likely_pathogenic,_risk_factor', 'Pathogenic/Likely_pathogenic,_drug_response',
    'Pathogenic/Likely_pathogenic,_other', 'Likely_pathogenic,_risk_factor', 'Likely_pathogenic',
    'Conflicting_interpretations_of_pathogenicity', 'Conflicting_interpretations_of_pathogenicity,_risk_factor',
    'Conflicting_interpretations_of_pathogenicity,_Affects',
    'Conflicting_interpretations_of_pathogenicity,_association,_risk_factor',
    'Conflicting_interpretations_of_pathogenicity,_other,_risk_factor',
    'Conflicting_interpretations_of_pathogenicity,_association',
    'Conflicting_interpretations_of_pathogenicity,_drug_response',
    'Conflicting_interpretations_of_pathogenicity,_drug_response,_other',
    'Conflicting_interpretations_of_pathogenicity,_other', 'Uncertain_significance',
    'Uncertain_significance,_risk_factor', 'Uncertain_significance,_Affects', 'Uncertain_significance,_association',
    'Uncertain_significance,_other', 'Affects', 'Affects,_risk_factor', 'Affects,_association', 'other', 'NA',
    'risk_factor', 'drug_response,_risk_factor', 'association', 'confers_sensitivity', 'drug_response', 'not_provided',
    'Likely_benign,_drug_response,_other', 'Likely_benign,_other', 'Likely_benign', 'Benign/Likely_benign,_risk_factor',
    'Benign/Likely_benign,_drug_response', 'Benign/Likely_benign,_other', 'Benign/Likely_benign', 'Benign,_risk_factor',
    'Benign,_confers_sensitivity', 'Benign,_association,_confers_sensitivity', 'Benign,_drug_response', 'Benign,_other',
    'Benign,_protective', 'Benign', 'protective,_risk_factor', 'protective',
    # In production - sort these correctly (get current list of values from actual clinvar file)
    'Pathogenic/Pathogenic,_low_penetrance', 'Pathogenic/Pathogenic,_low_penetrance|risk_factor',
    'Pathogenic/Likely_pathogenic/Pathogenic,_low_penetrance', 'Pathogenic/Likely_pathogenic/Pathogenic,_low_penetrance|other',
    'Pathogenic/Likely_pathogenic,_low_penetrance', 'Conflicting_interpretations_of_pathogenicity,_protective',
    'Likely_risk_allele', 'Uncertain_significance,_drug_response', 'Uncertain_significance/Uncertain_risk_allele',
    'Uncertain_risk_allele', 'Uncertain_risk_allele,_risk_factor', 'Uncertain_risk_allele,_protective',
    'association,_risk_factor', 'association,_drug_response', 'association,_drug_response,_risk_factor',
    'association_not_found', 'drug_response,_other', 'Likely_benign,_association', 'Benign/Likely_benign,_association',
    'Benign/Likely_benign,_drug_response,_other', 'Benign/Likely_benign,_other,_risk_factor', 'Benign,_association',
])}
# In production - use actual values in latest clinvar file version
CLINVAR_SIGNIFICANCES.update({sig.replace(',_', '|'): i for sig, i in CLINVAR_SIGNIFICANCES.items()})

def run(environment: str):
    for genome_version in ['37', '38']:
        mt = download_and_import_latest_clinvar_vcf(genome_version)
        timestamp = hl.eval(mt.version)
        ht = mt.rows()
        ht.describe()
        ht = ht.transmute(
            alleleId=ht.info.select('ALLELEID'),
            clinical_significance_id=hl.dict(CLINVAR_SIGNIFICANCES)[hl.delimit(x)],
            gold_stars=CLINVAR_GOLD_STARS_LOOKUP.get(hl.delimit(ht.info.CLNREVSTAT)),
        )
        ht = ht.repartition(PARTITIONS)
        destination_path = os.path.join(GCS_PREFIXES[environment], CLINVAR_HT_PATH).format(
            environment=environment,
            genome_version=genome_version,
            timestamp=timestamp,
        )
        print(f'Uploading ht from {hl.eval(ht.sourceFilePath)} to {destination_path}')
        write_ht(ht, destination_path)
        os.remove(hl.eval(ht.sourceFilePath))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--environment',
        default='dev',
        choices=['dev', 'prod']
    )
    args = parser.parse_args()
    run(args.environment)
