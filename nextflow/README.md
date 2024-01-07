## Nextflow pipeline

```bash
nextflow run fastq_to_vcf_pipeline.nf \
    --input_csv input_csv \
    --vcf_dir vcf_output_dir
```
input csv

```csv
sampleA,/path/data/fastq/sampleA_R1.fastq
sampleB,/path/data/fastq/sampleB_R1.fastq
```