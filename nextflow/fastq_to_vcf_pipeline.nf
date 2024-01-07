nextflow.enable.dsl=2

process bwa_map {
    input:
        tuple val(sampleName), path(fastqs)
    output:
        tuple  val(sampleName), path("sample_${sampleName}.bam")
    script:
        """
        cat  $fastqs > sample_${sampleName}.bam
        """
}

process vcf_calling {
    input:
        tuple val(sampleName), path(bam)
    output:
        tuple  val(sampleName), path("sample_${sampleName}.vcf")
    script:
        """
        cat $bam > sample_${sampleName}.vcf
        """  
}

process merge_vcf {
    publishDir params.vcf_dir, mode: 'copy', overwrite: true
    input:
        file "*.vcf"
    output:
        path 'merged.vcf'
    script:
        """
        cat *.vcf > merged.vcf
        """  
}

workflow {
    paths = Channel
            .fromPath(params.input_csv)
            .splitCsv()
            .map(row -> tuple(row[0], row[1]))
    bams_ch = bwa_map(paths)
    vcf_ch = vcf_calling(bams_ch)
    merge_ch = \
        merge_vcf(
            vcf_ch.map(row -> row[1]).collect())
    merge_ch.view()
}
