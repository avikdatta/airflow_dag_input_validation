import pandas as pd
import os, tempfile, subprocess
from pendulum import datetime
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
## nextflow script and config
NEXTFLOW_SCRIPT = \
    os.path.join(
        os.environ['AIRFLOW_HOME'],
        'nextflow/fastq_to_vcf_pipeline.nf')
NEXTFLOW_CONF = \
    os.path.join(
        os.environ['AIRFLOW_HOME'],
        'nextflow/nextflow_demo.conf')
## task get samples list
@task(
    task_id="get_sample_list_and_create_nextflow_input",
    retries=2,
    pool="airflow",
    multiple_outputs=True)
def get_sample_list_and_create_nextflow_input() -> dict:
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
    ## set input file path
    work_dir = \
        tempfile.mkdtemp()
    input_csv = \
        os.path.join(work_dir, 'input.csv')
    df = pd.DataFrame(samples)
    ## alter fastq path for demo set-up
    df['fastq1'] = \
        df['fastq1'].\
        map(
            lambda x: \
                os.path.join(
                    os.environ['AIRFLOW_HOME'], x))
    ## write input file for Nextflow pipeline
    df.to_csv(
        input_csv,
        index=False,
        header=False)
    output_dict = {
        "input_csv": input_csv,
        "work_dir": work_dir}
    return output_dict

@task(
    task_id="run_nextflow_pipeline",
    retries=2,
    pool="nextflow",
    multiple_outputs=False)
def run_nextflow_pipeline(
        input_csv: str,
        work_dir: str) -> None:
    try:
        vcf_dir = \
            os.path.join(
                work_dir, 
                'final_output')
        os.makedirs(vcf_dir, exist_ok=True)
        command = f"""cd {work_dir};\
            nextflow run \
            {NEXTFLOW_SCRIPT} \
            --input_csv {input_csv} \
            --vcf_dir {vcf_dir} \
            -c {NEXTFLOW_CONF};\
            sleep 5"""
        stdout_file = \
            os.path.join(work_dir, 'command_out.log')
        stderr_file = \
            os.path.join(work_dir, 'command_err.log')
        with open(stdout_file , 'w') as fout:
            with open(stderr_file, 'w') as ferr:
                subprocess.check_call(
                    command,
                    shell=True,
                    stdout=fout,
                    stderr=ferr)
        with open(stdout_file, 'r') as fp:
            out_data = fp.read()
        print(out_data)
        return os.path.join(vcf_dir, 'merged.vcf')
    except subprocess.CalledProcessError:
        with open(stderr_file, 'r') as fp:
            error_data = fp.read()
        raise ValueError(
            f"Failed to run Nextflow pipeline. Error: {error_data}")

@task(
    task_id="check_nextflow_output",
    retries=2,
    pool="airflow",
    multiple_outputs=False)
def check_nextflow_output(
        final_vcf: str, work_dir: str) -> None:
    print(os.listdir(os.path.join(work_dir, 'final_output')))
    print(work_dir)
    print(final_vcf)
    with open(final_vcf, 'r') as fp:
        vcf_data = fp.read()
    print(vcf_data)

## dag
@dag(
	start_date=datetime(2023, 1, 1),
	max_active_runs=10,
	schedule=None,
	catchup=False,
	tags=["test"])
def nextflow_analysis_wrapper():
    nextflow_info = \
        get_sample_list_and_create_nextflow_input()
    merged_vcf = \
        run_nextflow_pipeline(
            input_csv=nextflow_info['input_csv'],
            work_dir=nextflow_info['work_dir'])
    check_nextflow_output(
        final_vcf=merged_vcf,
        work_dir=nextflow_info['work_dir'])
## call dag
nextflow_analysis_wrapper()