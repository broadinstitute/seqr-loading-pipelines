zcat $1 | head -n 300 | grep CHROM | cut -f 10- | tr '\t' '\n' 
