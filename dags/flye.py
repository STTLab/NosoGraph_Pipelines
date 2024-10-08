from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.docker_operator import DockerOperator
from airflow.models.param import Param
from docker.types import Mount

default_args = {
    'owner': 'minamini',
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

# TODO: Update parameter set for running Flye.
flye_params = {
    'tech': Param(
        default='nano',
        title='Sequencing Technology',
        description_md='Technology used to generate the reads.\n\n'
                        '- nanopore = Reads produced by Oxford Nanopore sequencer\n'
                        '- pacbio = Reads produced by PacBio sequencer\n',
        type='string',
        enum=['nano', 'pacbio'],
        values_display={
            'nano': 'Nanopore',
            'pacbio': 'PacBio',
        }
    ),
    'qual': Param(
        default='raw',
        title='Sequencing Technology',
        description_md='Expected error rate.\n\n'
                        '- raw = <20%% error i.e. PacBio CLR reads or ONT reads, pre-Guppy5\n'
                        '- corrected = <3%% error i.e. PacBio or ONT reads that were corrected with other methods\n'
                        '- hifi = <1%% error (PacBio only)\n'
                        '- hq = <5%% error (ONT only, Guppy5+ SUP or Q20)\n',
        type='string',
        enum=['raw', 'corrected', 'hifi', 'hq'],
        values_display={
            'raw': 'Raw <20%% error',
            'corr': 'Corrected <3%% error',
            'hifi': 'PacBio HiFi <1%% error (PacBio only)',
            'hq': 'ONT <5%% error (Guppy5+ SUP or Q20)'
        }
    ),
    'input_fastq': Param(default='', description='FASTQ file containg reads to be assembled.', type='string'),
    'genome_size': Param(
        default='',
        title='Genome size',
        description='Estimate genome size for example, 5m or 2.6g.',
        type='string',
    ),
    'output_directory': Param(
        default='',
        title='Output directory',
        description='Output directory (required)',
        type='string'
    ),
    'polish_iter': Param(
        default=1,
        type='number',
        minimum=1,
        title='Polishing iteration',
        description='Number of polishing iterations (default: 1)',
    ),
    'min_overlap': Param(
        default=None,
        type=['null', 'number'],
        minimum=1000,
        title='Minimum overlap',
        description='Minimum overlap between reads (default: auto)',
    ),
    'asm_coverage': Param(
        default=None,
        type=['null', 'number'],
        title='Reduced coverage',
        description='Reduced coverage for initial disjointig assembly (default: not set)',
    ),
    'read_error': Param(
        default=None,
        type=['null', 'number'],
        title='Explicit Error rate',
        description='Adjust parameters for given read error rate (as fraction e.g. 0.03) (default: not set)',
    ),
    'meta_mode': Param(
        default=False,
        type='boolean',
        title='Use metagenomic mode (uneven coverage dataset)'
    ),
    'keep_haplotypes': Param(
        default=False,
        type='boolean',
        title='Keep Haplotypes',
        description_md='Do not collapse alternative haplotypes\n\n'
                        'By default, Flye (and metaFlye) collapses '
                        'graph structures caused by alternative haplotypes '
                        '(bubbles, superbubbles, roundabouts) to produce longer '
                        'consensus contigs.'
    ),
    'no_alt_contigs': Param(
        default=False,
        type='boolean',
        title='Don\'t output alternative contigs',
        description_md='Do not output contigs representing alternative haplotypes\n\n'
                        'In default mode, Flye is performing collapsed/haploid assmebly, '
                        'but may output contigs representing alternative alleles '
                        'if they differ significatnly from the "primary" assmebled allele.'
    ),
    'scaffold': Param(
        default=False,
        type='boolean',
        title='Scaffolding',
        description_md='Enable scaffolding using graph [disabled by default]\n\n'
                        'Flye does not perform scaffolding by default,'
                        'which guarantees that all assembled sequences do not have any gaps.'
    ),
    'threads': Param(
        default=4,
        type='number',
        title='Number of parallel threads'
    )
}

def build_flye_command(params):
    return '''flye --{{ params.tech }}-{{ params.qual }} {{ params.input_fastq }}
    -g {{ params.genome_size }} -o {{ params.output_directory }} -t {{ params.threads }}
    --iterations {{params.polish_iter}}
    {% if params.min_overlap %}--min-overlap {{ params.min_overlap }}{% endif %}
    {% if params.asm_coverage %}--asm-coverage {{ params.asm_coverage }}{% endif %}
    {% if params.meta_mode %}--meta{% endif %}
    {% if params.keep_haplotypes %}--keep-haplotypes{% endif %}
    {% if params.keep_haplotypes %}--keep-haplotypes{% endif %}
    {% if params.no_alt_contigs %}--no-alt-contigs{% endif %}
    {% if params.scaffold %}--scaffold{% endif %}
    '''

with DAG(
    dag_id='Flye_assembly_single_fastq',
    default_args=default_args,
    description='This DAG runs Flye with specified parameters',
    params=flye_params,
) as dag:
    check_flye = DockerOperator(
        task_id = 'Flye_check',
        docker_url='tcp://dind-service:2376',
        tls_verify=True,
        network_mode='bridge',
        image='nosograph-assemblers:latest',
        tls_ca_cert='/certs/ca/cert.pem',
        tls_client_cert='/certs/client/cert.pem',
        tls_client_key='/certs/client/key.pem',
        command='flye --version'
    )
    v_mounts = [Mount('/data', '/data', type='bind')]
    assemble = DockerOperator(
        task_id = 'Flye_long_read',
        docker_url='tcp://dind-service:2376',
        tls_verify=True,
        network_mode='bridge',
        image='nosograph-assemblers',
        tls_ca_cert='/certs/ca/cert.pem',
        tls_client_cert='/certs/client/cert.pem',
        tls_client_key='/certs/client/key.pem',
        command=build_flye_command(flye_params)
    )
    check_flye >> assemble
