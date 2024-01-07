import os, tempfile, subprocess
from pendulum import datetime
from airflow.decorators import dag, task, task_group
from airflow.utils.edgemodifier import Label
from airflow.operators.python import get_current_context

doc_md_DAG = """Airflow DAG for mapping fastq files to BAM and calling vcf for
individual samples and merging all vcf files to a single merged vcf

Requirements:

* reference_genome
* samples

Samples is a dictionary and should contain these following keys.
* sample_id
* fastq1

It has four tasks:

* get_sample_list
* map_fastq
* vcf_calling
* merge_vcf

```yaml
reference_genome: /path
samples:
  - sample_id: id
    fastq1: /path
```

"""
## task get samples list
@task(
    task_id="get_sample_list",
    retries=2,
    pool="airflow",
    multiple_outputs=True)
def get_sample_list() -> dict:
    """
    Airflow task for fetching analysis design from dat_run.conf
    and return a dictionary for ref genome and samples info

	:returns: A dictionary containing 'reference_genome' and 'samples'
	"""
    context = get_current_context()
    dag_run = context.get('dag_run')
    if dag_run is None or \
       dag_run.conf is None:
        raise ValueError("Missing dag_run.conf data")
    conf = dag_run.conf
    # get samples and reference_genome from dag_run.conf
    samples = conf.get("samples")
    reference_genome = conf.get("reference_genome")
    if samples is None:
        raise ValueError("Missing samples info")
    output_dict = {
        "reference_genome": reference_genome,
        "samples": samples} 
    return output_dict

@task(
    task_id="map_fastq",
    retries=2,
    pool="airflow",
    multiple_outputs=False)
def map_fastq(
        sample_info: dict,
        reference_genome: str) -> str:
    """
    Airflow task for creating bam from fastq file

    :param sample_info: A dictionary containing 'sample_id' and 'fastq1'
    :param reference_genome: A string of reference genome path
    :returns: A string value of bam file path
    """
    sample_id = sample_info.get('sample_id')
    fastq1 = sample_info.get('fastq1')
    ## modify paths for demo set-up
    airflow_home = \
        os.environ['AIRFLOW_HOME']
    fastq1 = \
        os.path.join(
            airflow_home,
            fastq1)
    ## get temp dir and set bam filename
    bam_dir = \
        tempfile.mkdtemp(
            prefix=sample_id)
    bam_file_name = \
        os.path.basename(fastq1)
    bam_file_name = \
        bam_file_name.\
        replace('.gz', '').\
        replace('.fastq', '.bam')
    bam_file = \
        os.path.join(
            bam_dir,
            bam_file_name)
    ## check contents of the fastq file
    with open(fastq1, 'r') as fp:
        fastq1_data = fp.read()
    print(f"Fastq data: {fastq1_data}")
    ## execute demo command and create bam file
    subprocess.run(
        f"cat {fastq1} > {bam_file}",
        shell=True)
    ## check contents of the bam file
    with open(bam_file, 'r') as fp:
        bam_file_data = fp.read()
    print(f"BAM data: {bam_file_data}")
    return bam_file

@task(
    task_id="vcf_calling",
    retries=2,
    pool="airflow",
    multiple_outputs=False)
def vcf_calling(
        bam_path: str) -> str:
    """
    Airflow task for creating vcf from bam files for each samples

    :param bam_path: A string value of bam file path
    :returns: A string value of vcf file path
    """
    ## get vcf file name
    vcf_file = \
        bam_path.replace('.bam', '.vcf')
    ## execute demo command and generate vcf file
    subprocess.run(
        f"cat {bam_path} > {vcf_file}",
        shell=True)
    ## check the contents of the vcf file
    with open(vcf_file, 'r') as fp:
        vcf_file_data = fp.read()
    print(f"VCF data: {vcf_file_data}")
    return vcf_file

@task(
    task_id="merge_vcf",
    retries=2,
    pool="airflow",
    multiple_outputs=False)
def merge_vcf() -> str:
    """
    Airflow task for fetcing and merging vcf for all samples to one file

    :returns: A string value of merged VCF file path
    """
    ## get context
    context = get_current_context()
    ti = context.get('ti')
    all_lazy_task_ids = \
        context['task'].\
        get_direct_relative_ids(upstream=True)
    ## ftech xcoms for all upstream tasks and filter None values
    vcf_files = \
        ti.xcom_pull(
            task_ids=all_lazy_task_ids)
    vcf_files = [
        f for f in vcf_files if f is not None]
    ## join vcf files for running bash commands
    vcf_files = ' '.join(vcf_files)
    merged_vcf_dir = \
        tempfile.mkdtemp(prefix='merged')
    merged_vcf_file = \
        os.path.join(
            merged_vcf_dir,
            'merged.vcf')
    ## execute demo command and create output file
    subprocess.run(
        f"cat {vcf_files} > {merged_vcf_file}",
        shell=True)
    ## check the contents of the merged vcf file
    with open(merged_vcf_file, 'r') as fp:
        merged_vcf_data = fp.read()
    print(f"Merged VCF data: {merged_vcf_data}")
    return merged_vcf_file

## task group to combine map_fastq and vcf-calling steps
@task_group
def fastq_to_vcf(sample_info: dict, reference_genome: str) -> None:
    bam_path = map_fastq(sample_info, reference_genome)
    vcf_file = vcf_calling(bam_path)

## dag
@dag(
    start_date=datetime(2023, 1, 1),
    max_active_runs=10,
    schedule=None,
    catchup=False,
    doc_md=doc_md_DAG,
    tags=["test"])
def airflow_analysis_dag():
    ## step 1: get reference_genome and samples list from design
    analysis_info = get_sample_list()
    ## step 2: dynamically create tasks for each samples
    tg = \
        fastq_to_vcf.\
        partial(reference_genome=analysis_info['reference_genome']).\
        expand(sample_info=analysis_info['samples'])
    ## step 3: collect vcf for each samples and merge together
    tg >> merge_vcf()

## call dag
airflow_analysis_dag()