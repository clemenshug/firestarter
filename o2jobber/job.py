import os
import pathlib
from textwrap import dedent
from datetime import datetime
from string import Template
from .transfer import transfer_files

class DgeBcbioJob:
    yaml_template = Template(dedent("""\
    details:
        - analysis: scRNA-seq
            algorithm:
                transcriptome_fasta: $transcriptome_fasta
                transcriptome_gtf: $transcriptome_gtf
                umi_type: harvard-scrb
                minimum_barcode_depth: 0
                cellular_barcode_correction: 1
                positional_umi: False
                genome_build: hg38
    """))

    def __init__(self, name, working_directory, transcriptome_fasta, transcriptome_gtf, fastq_files):
        self.name = name
        self.working_directory = pathlib.Path(working_directory).resolve()
        self.transcriptome_fasta = transcriptome_fasta
        self.transcriptome_gtf = transcriptome_gtf
        self.fastq_files = fastq_files if type(fastq_files) == list  else [fastq_files]
        self.run_directory = None
        self.fastq_directory = None

    def prepare_working_directory(self):
        self.working_directory.mkdir(exist_ok = True)

    def prepare_run_directory(self):
        self.prepare_working_directory()
        cur_time = datetime.now().isoformat(timespec = "minutes")
        self.run_directory = self.working_directory / (cur_time + self.name)
        self.run_directory.mkdir(exist_ok = False)
        self.fastq_directory = self.run_directory / "fastq"
        self.fastq_directory.mkdir()

    def transfer_files(self):
        files = [self.transcriptome_fasta, self.transcriptome_gtf] + self.fastq_files
        destinations = [self.working_directory / "transcriptome"]*2 + [self.fastq_directory]*len(self.fastq_files)
        transfer_files(files, destinations)

    def prepare_run(self):
        self.prepare_run_directory()
        self.transfer_files()
