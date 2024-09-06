from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.docker_operator import DockerOperator
from airflow.models.param import Param

default_args = {
    'owner': 'minamini',
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

# TODO: Update parameter set for running Flye.
unicycler_params = {
    'tech': Param(
        title='Sequencing Technology',
        description_md='Technology used to generate the reads.\n\n'
                        '- nanopore = Reads produced by Oxford Nanopore sequencer\n'
                        '- pacbio = Reads produced by PacBio sequencer\n',
        type='string',
        enum=['Nanopore', 'PacBio'],
        values_display={
            'nano': 'Nanopore',
            'pacbio': 'PacBio',
        }
    ),
    'qual': Param(
        title='Sequencing Technology',
        description_md='Expected error rate.\n\n'
                        '- raw = <20%% error\n'
                        '- corrected = <3%% error\n'
                        '- hifi = <1%% error (PacBio only)\n'
                        '- hq = <5%% error (ONT only, Guppy5+ SUP or Q20)\n',
        type='string',
        enum=['raw', 'corrected', 'hifi', 'hq']
    ),
    'input_fastq': Param(description='FASTQ file containg reads to be assembled.', type='string'),
    'genome_size': Param(
        title='Genome size',
        description='Estimate genome size for example, 5m or 2.6g.',
        type='string',
    ),
    'output_directory': Param('', title='Output directory', description='Output directory (required)', type='string'),
    'polish_iter': Param(
        title='Polishing iteration',
        description='Number of polishing iterations'
    )
}
# TODO: Update command set for running Unicycler hybrid assembly.
with DAG(
    dag_id='Unicycler_short_read_only',
    default_args=default_args,
    description='This DAG runs Unicycler in a short-read only mode with specified parameters',
    params=unicycler_params,
) as dag:
    task1 = DockerOperator(
        task_id = 'Unicycler_short_read',
        docker_url='tcp://dind-service:2376',
        tls_verify=True,
        network_mode='bridge',
        image='nosograph-assemblers',
        tls_ca_cert='/certs/ca/cert.pem',
        tls_client_cert='/certs/client/cert.pem',
        tls_client_key='/certs/client/key.pem',
        command='flye'
    )
    task1
