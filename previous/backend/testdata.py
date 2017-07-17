variant_fields = [
    # keys
    ('dataset_id', '"cohen"'),
    ('chrom', 'v.contig'),
    ('start', 'v.start'),
    ('ref', 'v.ref'),
    ('alt', 'v.alt'),
    # other
    ('filters', 'va.filters'),
    ('pass ', 'va.pass'),
    ('rsid ', 'va.rsid'),
    ('AC', 'va.info.AC[va.aIndex - 1]'),
    ('AN', 'va.info.AN'),
    ('AF', 'va.info.AF[va.aIndex - 1]'),
    ('was_split', 'va.wasSplit')
]

def genotype_fields(num_alt_missing3=False):
    num_alt_expr = 'g.nNonRefAlleles'
    if num_alt_missing3:
        num_alt_expr = 'orElse({}, 3)'.format(num_alt_expr)
    
    return [
        ('num_alt', num_alt_expr),
        ('gq', 'g.gq'),
        ('ab', 'let s = g.ad.sum in g.ad[0] / s'),
        ('dp', 'g.dp')
    ]

def solr_condition(fields, variants=False):
    stored = ['dataset_id', 'chrom', 'start', 'ref', 'alt']
    
    return ', '.join([
        '{} {{ docValues=false {} }} = {}'.format(
            field,
            '' if variants and field in stored else ', stored=false',
            expr)
        for field, expr in fields
    ])

def export_condition(fields):
    return ', '.join([
        '{} = {}'.format(field, expr)
        for field, expr in fields
    ])
