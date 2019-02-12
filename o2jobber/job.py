import os
import pathlib
import itertools
import typing
import subprocess
import yaml
from textwrap import dedent
from datetime import datetime
from string import Template
from .transfer import transfer_files_batch

class DgeBcbioJob(object):
    dge_config_template = Template(dedent("""\
    details:
    - algorithm:
        cellular_barcode_correction: 1
        minimum_barcode_depth: 0
        positional_umi: false
        transcriptome_fasta: $transcriptome_fasta
        transcriptome_gtf: $transcriptome_gtf
        umi_type: harvard-scrb
      analysis: scRNA-seq
      description: masterplate
      files: $fastq_files
      genome_build: hg38
      metadata: {}
    fc_name: $name
    #resources:
    #    default:
    #        memory: $mem
    #        cores: $cores
    upload:
      dir: ../final
    """))
    dge_submit_template = Template(dedent("""\
    #!/bin/sh
    #SBATCH -p $queue
    #SBATCH -J $run_id
    #SBATCH -o run.o
    #SBATCH -e run.e
    #SBATCH -t $time_limit
    #SBATCH --cpus-per-task=1
    #SBATCH --mem=$mem

    export PATH=/n/app/bcbio/dev/anaconda/bin/:/n/app/bcbio/tools/bin:$$PATH
    bcbio_nextgen.py ../config/$name.yaml -n $cores -t ipython -s slurm -q $queue -r t=$time_limit
    """))

    def __init__(self, name, working_directory, transcriptome_fasta,
                 transcriptome_gtf, fastq_files,
                 slurm_params={"time_limit": "0-4:00", "cores": "24", "queue": "short", "mem": "8000"}):
        self.name = name
        self.working_directory = pathlib.Path(working_directory).resolve()
        self.files_origin = {
            "transcriptome_fasta": pathlib.Path(transcriptome_fasta),
            "transcriptome_gtf": pathlib.Path(transcriptome_gtf),
            "fastq_files": [
                pathlib.Path(p) for p in (fastq_files if isinstance(fastq_files, typing.List) else [fastq_files])
            ],
        }
        self.files_destination = {
            "transcriptome_fasta": "transcriptome",
            "transcriptome_gtf": "transcriptome",
            "fastq_files": "fastq",
        }
        self.slurm_params = slurm_params
        self.files_location = None
        self.run_id = None
        self.run_directory = None

    def prepare_working_directory(self):
        self.working_directory.mkdir(exist_ok = True)

    def prepare_run_directory(self):
        self.run_id = datetime.now().isoformat(timespec = "minutes").replace(":", "_") + "_" + self.name
        self.run_directory = self.working_directory / self.run_id
        self.run_directory.mkdir(exist_ok = False)
        (self.run_directory / "config").mkdir(exist_ok = False)
        (self.run_directory / "work").mkdir(exist_ok = False)
        self.files_destination = {
            k: self.run_directory / d for k, d in self.files_destination.items()
        }
        for d in self.files_destination.values():
            d.mkdir(exist_ok = True)

    def transfer_files(self):
        file_transfers = {
            n: (o, self.files_destination[n]) for n, o in self.files_origin.items()
        }
        self.files_location = transfer_files_batch(file_transfers)
    
    def prepare_meta(self):
        with open(self.run_directory / "config" / f"{self.name}.yaml", "w") as f:
            if isinstance(self.files_location["fastq_files"], typing.List):
                fastq_str = ", ".join(f'"{p}"' for p in self.files_location["fastq_files"])
            else:
                fastq_str = '"' + str(self.files_location["fastq_files"]) + '"'
            f.write(
                self.dge_config_template.substitute(
                    transcriptome_fasta = str(self.files_location["transcriptome_fasta"]),
                    transcriptome_gtf = str(self.files_location["transcriptome_gtf"]),
                    fastq_files = "[" + fastq_str + "]",
                    name = self.name,
                    cores = self.slurm_params["cores"],
                    mem = self.slurm_params["mem"],
                )
            )
        with open(self.run_directory / "work" / f"{self.name}_run.sh", "w") as f:
            f.write(
                self.dge_submit_template.substitute(
                    self.slurm_params,
                    name = self.name,
                    run_id = self.run_id,
                )
            )

    def prepare_run(self):
        self.prepare_working_directory()
        self.prepare_run_directory()
        self.transfer_files()
        self.prepare_meta()

    def submit_run(self):
        cp = subprocess.run(
            ["sbatch", f"{self.name}_run.sh"],
            capture_output = True,
            cwd = self.run_directory / "work",
        )
        if not cp.returncode == 0:
            raise RuntimeError(
                "Job submission unsuccesfull:\n",
                cp.stderr
            )

    @classmethod
    def from_yaml(cls, data):
        atr_dict = yaml.safe_load(data)
        return cls(**atr_dict)
