Variant Search Query:

request:
```
{
	page: 1,
	limit: 50,
	sort_by: ["chrom", "pos"],
	variant_filters: {
		chrom:  { "eq": "1" },
		pos:    { "range": [12345, 54321] },
		ref:    { "eq": "A"  },
		alt:    { "eq": "G" },
		filter: { "eq": "PASS" },
		1kg_wgs_phase3_global: { "range": ["*", 0.01] },
		1kg_wgs_phase3_popmax: { "range": [0, 0.01] },
		exac_v3_global: { "range": [0, 0.01] },
		exac_v3_popmax: { "range": [0, 0.01] },
		transcript_consequence: { "in" : ["stop_gained", "splice_donor_variant", "splice_acceptor_variant"] },
		transcript_gene_id: { "in" : ["ENSG00012345", "ENSG0012412"] },
		dataset_id: { "eq": "INMR" },
		dataset_version: { "eq": "2016_04_12" },
		dataset_type: { "eq": "wgs" },
	},

	genotype_filters: [{
			"id": { "eq": "NA12879" },
			num_alt: { "in" : [0, 1] },
			"AB": { "range": [0.2, "*"] },
			"GQ": { "range": [90, "*"] },
		}, {
			"id": { "eq": "NA12879" },
			"num_alt": { "eq": 0 },
			"AB": { "range": [0.2, "*"] },
			"GQ": { "range": [90, "*"]	},
		}, { 
			"id": { "eq": "NA12879" },
			"num_alt": { "eq": 1 },
			"AB": { "range": [0.2, "*"] },
			"GQ": { "range": [90, "*"] },
		}, 
	]
}
```

response:
```
{
	api_version: 0.1,
	page: 1,
	limit: 50,
	found: 2,
	variants: [
		{ 
			chrom:  "1",
			pos:    12345,
			ref:    "A",
			alt:    "G",
			filter: "PASS",
			rsid: "rs1800234",  
			1kg_wgs_phase3_global: 0.01,
			1kg_wgs_phase3_popmax: 0.01,
			exac_v3_global: 0.0,
			exac_v3_popmax: 0.0,
			gene_id: ["ENSG0001234", "ENSG000534321"],
			transcript_to_show: {
				gene_id: "ENSG00012345",
				transcript_id: "ENST00012345",
				consequence: "stop_gained",
				is_protein_coding: true, 
				is_canonical: true, 
			},
			transcripts: [{
					gene_id: "ENSG00012345",
					transcript_id: "ENST00012345",
					consequence: "stop_gained",
					is_protein_coding: true, 
					is_canonical: true, 
				}, {
					gene_id: "ENSG00012346",
					transcript_id: "ENST00012346",
					consequence: "missense",
					is_protein_coding: true, 
					is_canonical: false, 
				}
			],
			genotypes: [{
					id: : "NA12879",
					num_alt: 2,
					AD: "40,40",
					DP: 80,
					AB: 0.5,
					GQ: 99,
					PL: "100,70,0"
				}, {
					id: "NA12879",
					num_alt: 0,
					AD: "40,40",
					DP: 80,
					AB: 0.5,
					GQ: 90,
					PL: "0,70,180"
				}, 
			]
		}, 
		{
			chrom: "1", 
			pos:   12346, 
			...
		}

	]

}
```
