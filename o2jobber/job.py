import abc
import os
import pathlib
import itertools
import typing
import subprocess
import shutil
import pandas as pd
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
    def __init__(self, name, working_directory, data, slurm_params=SLURM_PARAMS_DEFAULT, debug = False):
        self.name = name
        self.working_directory = normalize_path(working_directory)
        self.data = data
        self.data_transformed = data.copy(deep = True)
        self.files_destination = {}
        self.slurm_params = slurm_params
        self.files_location = None
        self.run_id = None
        self.run_directory = None
        self.debug = debug

    def check_data(self):
        required_cols = {"id"} | set(self.files_destination.keys())
        missing_cols = required_cols - set(self.data)
        if len(missing_cols) > 0:
            raise ValueError(
                "The following required columns are missing from data:\n",
                str(missing_cols)
            )

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
        file_transfers = {}
        for n in self.data:
            if n not in self.files_destination:
                continue
            file_transfers[n] = (list(set(self.data[n])), self.files_destination[n])
        locations = transfer_files_batch(file_transfers)
        for n, l in locations.items():
            o, d = list(zip(*l))
            new_loc = pd.DataFrame({
                n: o,
                f"{n}_moved": d,
            })
            transformed = pd.merge(self.data_transformed, new_loc, how = "left", on = n)
            transformed[n] = transformed[f"{n}_moved"]
            transformed.drop(columns=f"{n}_moved", inplace=True)
            if not all(p.exists() for p in transformed[n]):
                raise RuntimeError(
                    "Files not found at new location:\n",
                    str(list(filter(lambda p: not p.exists(), transformed[n])))
                )
            self.data_transformed = transformed


    def merge_files(self):
        pass

    def prepare_run(self):
        if self.debug:
            self.prepare_working_directory()
            self.prepare_run_directory()
            self.transfer_files()
            self.merge_files()
            self.prepare_meta()
            return
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

    def normalize_paths(self):
        for n in self.files_destination:
            self.data[n] = self.data[n].map(normalize_path)

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
    sample_meta = None
    submit_template = None

    def __init__(self, name, working_directory, data, slurm_params = SLURM_PARAMS_DEFAULT, debug = False):
        super().__init__(name = name, working_directory = working_directory,
                         data = data, slurm_params = slurm_params, debug = debug)
        self.files_destination = {
            "transcriptome_fasta": "transcriptome",
            "transcriptome_gtf": "transcriptome",
            "fastq": "fastq",
        }
        self.normalize_paths()
        self.check_data()

    def merge_files(self):
        def perform_merge(files, i):
            prefix = os.path.commonprefix(files)
            if len(prefix) == 0:
                raise ValueError("Couldn't find prefix for " + str(files))
            ext = "".join(pathlib.Path(files[0]).suffixes)
            dest =  f"{prefix}_merged{i}{ext}"
            print("Merging", " ".join(str(f) for f in files), "into", dest)
            concatenate_files(files, dest)
            return dest
        def merge_per_sample(files):
            pairs = combine_pairs(files)
            if len(pairs) == 1:
                # Nothing to merge
                return files
            # If one fastq is paired, all should be paired
            if any(isinstance(p, typing.List) for p in pairs):
                if not all(isinstance(p, typing.List) for p in pairs):
                    raise ValueError("Pairing error: " + str(pairs))
                files_merge = list(zip(*pairs))
                files_destination = []
                for i, files in enumerate(files_merge):
                    dest = perform_merge(files, f"_{i + 1}")
                files_destination.append(pathlib.Path(dest))
            else:
                dest = perform_merge(pairs, "")
                files_destination = [pathlib.Path(dest)]
            return files_destination
        sample_groups = self.data_transformed.groupby("id")
        merged_data = []
        for _, g in sample_groups:
            d = merge_per_sample(g["fastq"])
            m = g.head(n = len(d))
            m["fastq"] = d
            merged_data.append(m)
        self.data_transformed = pd.concat(merged_data, ignore_index=True)

    def prepare_meta(self):
        sample_meta = []
        data_by_sample = self.data_transformed.groupby("id")
        for n, g in data_by_sample:
            m = deepcopy(self.sample_meta)
            m["description"] = n
            m["files"] = list(g["fastq"])
            m["algorithm"]["transcriptome_fasta"] = g["transcriptome_fasta"][0]
            m["algorithm"]["transcriptome_gtf"] = g["transcriptome_gtf"][0]
            meta_cols = set(self.data_transformed) - ({"id"} | set(self.files_destination.keys()))
            m["metadata"] = {c: g[c][0] for c in meta_cols}
            sample_meta.append(m)
        sample_meta = {
            "details": sample_meta,
            "fc_name": self.name,
            "upload": {
                "dir": "../final",
            }
        }
        with open(self.run_directory / "config" / f"{self.name}.yaml", "w") as f:
            yaml.safe_dump(sample_meta, stream = f)
        with open(self.run_directory / "work" / f"{self.name}_run.sh", "w") as f:
            f.write(
                self.submit_template.substitute(
                    self.slurm_params,
                    name = self.name,
                    run_id = self.run_id,
                )
            )


class DgeBcbioJob(RnaseqGenericBcbioJob):
    sample_meta = {
        "algorithm": {
            "cellular_barcode_correction": 1,
            "minimum_barcode_depth": 0,
            "positional_umi": "false",
            "umi_type": "harvard-scrb",
            "transcriptome_fasta": None,
            "transcriptome_gtf": None,
        },
        "analysis": "scRNA-seq",
        "description": None,
        "files": None,
        "genome_build": "hg38",
        "metadata": {}
    }
    submit_template = Template(dedent("""\
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
    submit_template = Template(dedent("""\
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


job_types = {
    "dge": DgeBcbioJob,
    "bulk": BulkRnaseqBcbioJob,
}
