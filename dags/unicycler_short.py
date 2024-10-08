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
    'short_1': Param('', description='FASTQ file of first short reads in each pair', type='string'),
    'short_2': Param('', description='FASTQ file of second short reads in each pair', type='string'),
    'output_directory': Param('', title='Output directory', description='Output directory (required)', type='string'),
    'min_fasta_length': Param(
        default=100,
        title='Minimum read length',
        description='Exclude contigs from the FASTA file which are shorter than this length (default: 100)',
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
    'SPAdes_conf': Param(
        default={
            'min_kmer_frac': 0.20,
            'max_kmer_frac': 0.95,
            'kmers': 'automatic',
            'kmer_count': 8,
            'depth_filter': 0.25,
            'keep_largest_component': True,
        },
        schema={
            'type': 'object',
            'properties': {
                'depth_filter': { 'type': 'number' },
                'keep_largest_component': { 'type': 'boolean' },
                'kmers': { 'type': 'string' },
                'kmer_count': { 'type': 'integer' },
                'min_kmer_frac': { 'type': 'number' },
                'max_kmer_frac': { 'type': 'number' },
                'spades_options': { 'type': ['null', 'object']}
        }
}

    ),
    'min_kmer_frac': Param(
        default=0.2,
        title='SPAdes Lowest k-mer fraction',
        description='Lowest k-mer size for SPAdes assembly, expressed as a fraction of the read length (default: 0.2)',
        type='number',
    ),
    'max_kmer_frac': Param(
        default=0.95,
        title='SPAdes Highest k-mer fraction',
        description='Highest k-mer size for SPAdes assembly, expressed as a fraction of the read length (default: 0.95)',
        type='number',
    ),
    'kmer_count': Param(
        default=8,
        title='SPAdes Number of k-mer steps',
        description='Number of k-mer steps to use in SPAdes assembly (default: 8)',
        type='integer',
    ),
    'automatic_kmers': Param(
        default=True,
        title='SPAdes Automatic k-mer size',
        description='Automatically set k-mer size (default: automatic). If turn off, please fill the exact k-mers option below',
        type='boolean',
    ),
    'kmers': Param(
        default=None,
        title='SPAdes exact k-mers',
        description_md='**Left blank if use Automatic k-mer size**\n\nExact k-mers to use for SPAdes assembly, comma-separated (example: 21,51,71).',
        type=['null', 'string'],
    ),
    'depth_filter': Param(
        default=0.25,
        title='SPAdes Depth filter treshold',
        description='Filter out contigs lower than this fraction of the chromosomal depth, if doing so does not result in graph dead ends (default: 0.25)',
        type='number',
    ),
    'largest_component': Param(
        default=False,
        title='SPAdes Only keep the largest connected component',
        description='Only keep the largest connected component of the assembly graph (default: keep all connected components)',
        type='boolean',
    ),
    'assembly_rotation': Param(
        default=True,
        title='Rotate completed replicons',
        description='Rotate completed replicons to start at a standard gene',
        type='boolean',
    ),
    'start_genes': Param('start_genes.fasta', description='FASTA file of genes for start point of rotated replicons', type='string'),
    'start_gene_id': Param(
        default=90,
        title='Minimum BLAST identity for gene search (%)',
        description='The minimum required BLAST percent identity for a start gene search (default: 90.0)',
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
}
with DAG(
    dag_id='Unicycler_short_read',
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
        command=' '.join([
            'unicycler -1 {{ params.short_1 }} -2 {{ params.short_2 }}',
            '-o {{ params.output_directory }}',
            '--min_fasta_length {{ params.min_fasta_length }}',
            '--keep {{ params.keep }} --mode {{ params.mode }}',
            '--linear_seqs {{ params.linear_seqs }}'
        ])
    )
    check_unicycler >> assemble
