from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.docker_operator import DockerOperator
from airflow.models.param import Param
from docker.types import Mount

default_args = {
    'owner': 'minamini',
    'retries': 0,
    'retry_delay': timedelta(minutes=2)
}

unicycler_params = {
    'long_fq': Param('', description='FASTQ or FASTA file of long reads', type='string'),
    'output_directory': Param(
        title='Output directory',
        description='Output directory (required)',
        type='string'
    ),
    'min_fasta_length': Param(
        default=100,
        title='Minimum read length',
        description='Exclude contigs from the FASTA file '
                    'which are shorter than this length (default: 100)',
        type='integer',
    ),
    'keep': Param(
        default=1,
        title='Select file retention level',
        description_md='Level of file retention (default: save graphs at main checkpoints)',
        type='integer',
        enum=[*range(0, 4)],
        values_display={
            0: 'only keep final files',
            1: 'also save graphs at main checkpoints',
            2: 'also keep SAM (enables fast rerun in different mode)',
            3: 'keep all temp files and save all graphs (for debugging)'
        }
    ),
    'mode': Param(
        default='normal',
        title='Bridging mode',
        description_md='Bridging mode (default: normal)\n\n'
                        '- conservative = smaller contigs, lowest misassembly rate\n'
                        '- normal = moderate contig size and misassembly rate\n'          
                        '- bold = longest contigs, higher misassembly rate\n',
        type='string',
        enum=['conservative', 'normal', 'bold']
    ),
    'min_bridge_qual': Param(
        default=10,
        title='Minimum bridging quality',
        description_md='**Do not** apply bridges with a quality below this value\n\n'
                        '- conservative mode default: 25.0\n'
                        '- normal mode default: 10.0\n'
                        '- bold mode default: 1.0',
        type='number'
    ),
    'linear_seqs': Param(
        default=0,
        title='Expected number of linear sequences',
        description='The expected number of linear (i.e. non-circular) sequences in the underlying sequence (default: 0)',
        type='integer',
        minimum=0
    ),
    'no_miniasm': Param(
        default=False,
        type='boolean',
        title='Skip miniasm+Racon bridging',
        description='Skip miniasm+Racon bridging'
                    '(default: use miniasm and Racon to'
                    'produce long-read bridges)'
    ),
    # These options control the use of long-read alignment to produce long-read bridges.
    'no_simple_bridges': Param(
        title='Skip simple long-read bridging',
        default=False,
        type='boolean'
    ),
    'no_long_read_alignment': Param(
        title='Skip long-read-alignment-based bridging',
        default=False,
        type='boolean'
    ),
    'contamination': Param(
        title='FASTA file of known contamination',
        description='FASTA file of known contamination in long reads',
        type=['null', 'string']
    ),
    'scores': Param(
        title='Explitit alignment panelty score',
        description='Comma-delimited string of alignment scores: '
                    'match, mismatch, gap open, gap extend (default: 3,-6,-5,-2)',
        type=['null', 'string']
    ),
    'low_score': Param(
        title='Alignment Score threshold',
        description='Alignments below this are considered poor.'
                    '(default: set threshold automatically)',
        type=['null', 'number']
    ),

    # These options control the removal of small leftover sequences
    # after bridging is complete.
    'min_component_size': Param(
        default=1000,
        title='Alignment Score threshold',
        description='Graph components smaller than this size (bp) '
                    'will be removed from the final graph (default: 1000)',
        type=['null', 'number']
    ),
    'min_dead_end_size': Param(
        default=1000,
        title='Alignment Score threshold',
        description='Graph dead ends smaller than this size (bp) '
                    'will be removed from the final graph (default: 1000)',
        type=['null', 'number']
    ),

    # These options control the rotation of completed circular sequence
    # near the end of the Unicycler pipeline.
    'assembly_rotation': Param(
        default=True,
        title='Rotate completed replicons',
        description='Rotate completed replicons to start at a standard gene',
        type='boolean',
    ),
    'start_genes': Param(
        default='start_genes.fasta',
        description='FASTA file of genes for start point of rotated replicons',
        type='string'
    ),
    'start_gene_id': Param(
        default=90,
        title='Minimum BLAST identity for gene search (%)',
        description='The minimum required BLAST percent identity '
                    'for a start gene search (default: 90.0)',
        type='number',
        minimum=0,
        maximum=100
    ),
    'start_gene_cov': Param(
        default=95,
        title='Minimum BLAST coverage for gene search (%)',
        description='The minimum required BLAST percent coverage for a start gene search (default: 95.0)',
        type='number',
        minimum=0,
        maximum=100
    ),
    'threads': Param(
        default=8,
        title='Threads'
    )
}

with DAG(
    dag_id='Unicycler_long_read_only',
    default_args=default_args,
    description='This DAG runs Unicycler in a short-read only mode with specified parameters',
    params=unicycler_params,
) as dag:
    check_unicycler = DockerOperator(
        task_id = 'Unicycler_check',
        docker_url='tcp://dind-service:2376',
        tls_verify=True,
        network_mode='bridge',
        image='nosograph-assemblers:latest',
        tls_ca_cert='/certs/ca/cert.pem',
        tls_client_cert='/certs/client/cert.pem',
        tls_client_key='/certs/client/key.pem',
        command='unicycler --version'
    )
    v_mounts = [Mount('/data', '/data', type='bind')]
    assemble = DockerOperator(
        task_id = 'Unicycler_short_read',
        docker_url='tcp://dind-service:2376',
        tls_verify=True,
        network_mode='bridge',
        image='nosograph-assemblers',
        tls_ca_cert='/certs/ca/cert.pem',
        tls_client_cert='/certs/client/cert.pem',
        tls_client_key='/certs/client/key.pem',
        mount_tmp_dir=False,
        mounts=v_mounts,
        # TODO: Add advance arguments
        command=' '.join([
            'unicycler --long {{ params.long_fq }}',
            '-o {{ params.output_directory }}',
            '--min_fasta_length {{ params.min_fasta_length }}',
            '--keep {{ params.keep }} --mode {{ params.mode }}',
            '--linear_seqs {{ params.linear_seqs }}'
        ])
    )
    check_unicycler >> assemble
