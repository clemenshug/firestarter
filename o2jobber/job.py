import abc
import os
import pathlib
import itertools
import typing
import subprocess
import shutil
import yaml
from copy import deepcopy
from textwrap import dedent
from datetime import datetime
from string import Template
from .transfer import transfer_files_batch
from .util import normalize_path, combine_pairs, concatenate_files


SLURM_PARAMS_DEFAULT = {
    "time_limit": "0-8:00",
    "cores": "12",
    "queue": "short",
    "mem": "8000",
}


class BcbioJob(abc.ABC):
    def __init__(self, name, working_directory, slurm_params=SLURM_PARAMS_DEFAULT):
        self.name = name
        self.working_directory = normalize_path(working_directory)
        self.files_origin = {}
        self.files_destination = {}
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

    def merge_files(self):
        pass

    def prepare_run(self):
        try:
            self.prepare_working_directory()
            self.prepare_run_directory()
            self.transfer_files()
            self.merge_files()
            self.prepare_meta()
        except Exception as e:
            if self.run_directory.exists():
                shutil.rmtree(self.run_directory)
            raise RuntimeError("Error during run directory preparation") from e

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

    @classmethod
    def from_dict(cls, data):
        return cls(**data)

    @abc.abstractmethod
    def prepare_meta(self):
        pass


class RnaseqGenericBcbioJob(BcbioJob):
    def __init__(self, name, working_directory, transcriptome_fasta,
                 transcriptome_gtf, fastq_files,
                 slurm_params = SLURM_PARAMS_DEFAULT):
        super().__init__(name = name, working_directory = working_directory,
                         slurm_params = slurm_params)
        self.name = name
        self.working_directory = normalize_path(working_directory)
        self.files_origin = {
            "transcriptome_fasta": normalize_path(transcriptome_fasta),
            "transcriptome_gtf": normalize_path(transcriptome_gtf),
            "fastq_files": [
                normalize_path(p) for p in (fastq_files if isinstance(fastq_files, typing.List) else [fastq_files])
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
    
    def merge_files(self):
        def perform_merge(files, i):
            prefix = os.path.commonprefix(*files)
            if len(prefix) == 0:
                raise ValueError("Couldn't find prefix for " + str(files))
            ext = "".join(pathlib.Path(files[0]).suffixes)
            dest =  f"{prefix}_merged_{i}{ext}"
            print("Merging", " ".join(files), "into", dest)
            concatenate_files(files, dest)
            return dest
        pairs = combine_pairs(self.files_destination["fastq_files"])
        if len(pairs) == 1:
            # Nothing to merge
            return
        # If one fastq is paired, all should be paired
        if any(isinstance(p, tuple) for p in pairs):
            if not all(isinstance(p, tuple) for p in pairs):
                raise ValueError("Pairing error: " + str(pairs))
            files_merge = list(zip(*pairs))
            self.files_destination["fastq_files"] = []
            for i, files in enumerate(files_merge):
                dest = perform_merge(files, i)
                self.files_destination["fastq_files"].append(pathlib.Path(dest))
        else:
            dest = perform_merge(pairs, "")
            self.files_destination["fastq_files"] = [pathlib.Path(dest)]


class DgeBcbioJob(RnaseqGenericBcbioJob):
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
    # resources:
    #   tmp:
    #     dir: /tmp
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


class BulkRnaseqBcbioJob(RnaseqGenericBcbioJob):
    sample_meta = {
        "algorithm": {
            "aligner": "hisat2",
            "strandedness": "unstranded",
            "transcriptome_fasta": None,
            "transcriptome_gtf": None,
        },
        "analysis": "RNA-seq",
        "description": None,
        "files": None,
        "genome_build": "GRCh38",
        "metadata": {}
    }

    def prepare_meta(self):
        sm = deepcopy(self.sample_meta)
        sm["algorithm"]["transcriptome_fasta"] = self.files_destination["transcriptome_fasta"]
        sm["algorithm"]["transcriptome_gtf"] = self.files_destination["transcriptome_gtf"]
        sm["description"] = self.name


job_types = {
    "dge": DgeBcbioJob,
    "bulk": BulkRnaseqBcbioJob,
}
