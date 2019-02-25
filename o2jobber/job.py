import abc
import os
import pathlib
import itertools
import typing
import subprocess
import shutil
import numpy as np
import pandas as pd
import yaml
from copy import deepcopy
from textwrap import dedent
from datetime import datetime
from string import Template
from attr import attrs, attrib
from .transfer import transfer_files_batch
from .util import normalize_path, combine_pairs, concatenate_files


SLURM_PARAMS_DEFAULT = {
    "time_limit": "0-8:00",
    "cores": "12",
    "queue": "short",
    "mem": "8000",
}


EMPTY_DEFAULT = object()


@attrs
class JobParameter(object):
    name = attrib()
    path = attrib()
    default = attrib(default=None, kw_only=True)
    per_sample = attrib(default=True, kw_only=True)
    _compatible_types = tuple(yaml.SafeDumper.yaml_representers.keys())[:-1]

    @staticmethod
    def _nested_set(d, path, value):
        # From https://stackoverflow.com/a/13688108/4603385
        if not isinstance(path, typing.List):
            path = [path]
        for key in path[:-1]:
            d = d.setdefault(key, {})
        d[path[-1]] = value

    @staticmethod
    def _basic_type(value):
        value = list(value)
        if not isinstance(value[0], JobParameter._compatible_types):
            value = list(str(x) for x in value)
        if len(value) > 1:
            return value
        return value[0]

    def add_default_data(self, data, job=None):
        data = data.copy()
        if self.name not in data:
            if self.default is None:
                raise ValueError("{self.name} not found in ", str(data))
            if callable(self.default):
                data[self.name] = self.default(job)
            elif self.default is EMPTY_DEFAULT:
                # Don't add column, is supposed to be empty
                return data
            else:
                data[self.name] = self.default
        return data

    def get_sample_param(self, data, job=None):
        if self.name not in data:
            if self.default is EMPTY_DEFAULT:
                return EMPTY_DEFAULT
            raise RuntimeError(
                f"Something went wrong, data should have column {self.name}"
            )
        v = data[self.name]
        if self.per_sample:
            if not len(set(v)) == 1:
                raise ValueError(
                    f"Expected only a single unique value for {self.name} in ", str(v)
                )
            return self._basic_type([v.iloc[0]])
        return self._basic_type(v)

    def set_param_meta(self, data, meta):
        v = self.get_sample_param(data)
        if v is EMPTY_DEFAULT:
            return m
        m = self._nested_set(meta, self.path, v)
        return m


@attrs
class FileJobParameter(JobParameter):
    destination = attrib(kw_only=True)


BCBIOJOB_PARAMS = [
    JobParameter("id", "description"),
]


class BcbioJob(abc.ABC):
    params = BCBIOJOB_PARAMS

    def __init__(
        self,
        name,
        working_directory,
        data,
        slurm_params=SLURM_PARAMS_DEFAULT,
        debug=False,
    ):
        self.name = name
        self.working_directory = normalize_path(working_directory)
        self.data = data
        self.slurm_params = slurm_params
        self.run_directory = None
        self.debug = debug
        self.check_data(data)

    def check_data(self, data):
        required_cols = set(p.name for p in self.required_params)
        missing_cols = required_cols - set(self.data)
        if len(missing_cols) > 0:
            raise ValueError(
                "The following required columns are missing from data:\n",
                str(missing_cols),
            )

    def add_defaults(self, data):
        data = data.copy()
        for p in self.optional_params:
            data = p.add_default_data(data, job=self)
        return data

    @property
    def file_params(self):
        return [p for p in self.params if isinstance(p, FileJobParameter)]

    @property
    def file_destinations(self):
        if not self.run_directory:
            raise RuntimeError("Run directory not set yet, destinations unknown")
        return {p.name: self.run_directory / p.destination for p in self.file_params}

    @property
    def required_params(self):
        return [p for p in self.params if p.default is None]

    @property
    def optional_params(self):
        return [p for p in self.params if p.default is not None]

    def prepare_working_directory(self):
        self.working_directory.mkdir(exist_ok=True)

    def prepare_run_directory(self):
        self.run_id = (
            datetime.now().isoformat(timespec="minutes").replace(":", "_")
            + "_"
            + self.name
        )
        self.run_directory = self.working_directory / self.run_id
        self.run_directory.mkdir(exist_ok=False)
        (self.run_directory / "config").mkdir(exist_ok=False)
        (self.run_directory / "work").mkdir(exist_ok=False)
        for d in self.file_destinations.values():
            d.mkdir(exist_ok=True)

    def transfer_files(self, data):
        data = data.copy()
        file_transfers = {}
        destinations = self.file_destinations
        for p in self.file_params:
            file_transfers[p.name] = (list(set(data[p.name])), destinations[p.name])
        locations = transfer_files_batch(file_transfers)
        for n, l in locations.items():
            o, d = list(zip(*l))
            new_loc = pd.DataFrame({n: o, f"{n}_moved": d})
            data = pd.merge(data, new_loc, how="left", on=n)
            data[n] = data[f"{n}_moved"]
            data.drop(columns=f"{n}_moved", inplace=True)
            if not all(p.exists() for p in data[n]):
                raise RuntimeError(
                    "Files not found at new location:\n",
                    str(list(filter(lambda p: not p.exists(), data[n]))),
                )
        return data

    def merge_files(self, data):
        return data

    def _prepare_run(self):
        self.prepare_working_directory()
        self.prepare_run_directory()
        data_defaults = self.add_defaults(self.data)
        data_normalized = self.normalize_paths(data_defaults)
        data_transferred = self.transfer_files(data_normalized)
        data_merged = self.merge_files(data_transferred)
        self.prepare_meta(data_merged)

    def prepare_run(self):
        if self.debug:
            self._prepare_run()
            return
        try:
            self._prepare_run()
        except Exception as e:
            if self.run_directory.exists():
                shutil.rmtree(self.run_directory)
            raise RuntimeError("Error during run directory preparation") from e

    def submit_run(self):
        cp = subprocess.run(
            ["sbatch", f"{self.name}_run.sh"],
            capture_output=True,
            cwd=self.run_directory / "work",
        )
        if not cp.returncode == 0:
            raise RuntimeError("Job submission unsuccesfull:\n", cp.stderr)

    def normalize_paths(self, data):
        data = data.copy()
        for n in (p.name for p in self.file_params):
            data[n] = data[n].map(normalize_path)
        return data

    @classmethod
    def from_yaml(cls, data):
        atr_dict = yaml.safe_load(data)
        return cls(**atr_dict)

    @classmethod
    def from_dict(cls, data):
        return cls(**data)

    @abc.abstractmethod
    def prepare_meta(self, data):
        pass


RNASEQJOB_PARAMS = BCBIOJOB_PARAMS + [
    JobParameter("genome", "genome_build", default="hg38"),
    FileJobParameter(
        "transcriptome_fasta",
        ["algorithm", "transcriptome_fasta"],
        destination="transcriptome",
    ),
    FileJobParameter(
        "transcriptome_gtf",
        ["algorithm", "transcriptome_gtf"],
        destination="transcriptome",
    ),
    FileJobParameter("fastq", "files", destination="fastq", per_sample=False),
]


class RnaseqGenericBcbioJob(BcbioJob):
    sample_meta = None
    submit_template = None
    params = RNASEQJOB_PARAMS

    def merge_files(self, data):
        def perform_merge(files, i):
            prefix = os.path.commonprefix(files)
            if len(prefix) == 0:
                raise ValueError("Couldn't find prefix for " + str(files))
            ext = "".join(pathlib.Path(files[0]).suffixes)
            dest = f"{prefix}_merged{i}{ext}"
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

        sample_groups = data.groupby("id")
        merged_data = []
        for _, g in sample_groups:
            d = merge_per_sample(g["fastq"])
            m = g.copy().head(n=len(d))
            m["fastq"] = d
            merged_data.append(m)
        return pd.concat(merged_data, ignore_index=True)

    def prepare_meta(self, data):
        sample_meta = []
        data_by_sample = data.groupby("id")
        for _, g in data_by_sample:
            m = deepcopy(self.sample_meta)
            for p in self.params:
                p.set_param_meta(g, m)

            meta_cols = set(data) - set(p.name for p in self.params)
            m["metadata"] = {c: g[c].iloc[0] for c in meta_cols}
            sample_meta.append(m)
        sample_meta = {
            "details": sample_meta,
            "fc_name": self.name,
            "upload": {"dir": "../final"},
        }
        with open(self.run_directory / "config" / f"{self.name}.yaml", "w") as f:
            yaml.safe_dump(sample_meta, stream=f, default_flow_style=False)
        with open(self.run_directory / "work" / f"{self.name}_run.sh", "w") as f:
            f.write(
                self.submit_template.substitute(
                    self.slurm_params, name=self.name, run_id=self.run_id
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
        "genome_build": None,
        "metadata": {},
    }
    submit_template = Template(
        dedent(
            """\
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
    """
        )
    )


BULKRNASEQJOB_PARAMS = RNASEQJOB_PARAMS + [
    JobParameter("aligner", ["algorithm", "aligner"], default="hisat2"),
    JobParameter("strandedness", ["algorithm", "strandedness"], default="unstranded"),
    JobParameter("aligner", ["algorithm", "aligner"], default="hisat2"),
    FileJobParameter(
        "spikein_fasta", ["algorithm", "spikein_fasta"], default=EMPTY_DEFAULT
    ),
]


class BulkRnaseqBcbioJob(RnaseqGenericBcbioJob):
    params = BULKRNASEQJOB_PARAMS
    sample_meta = {
        "algorithm": {
            "aligner": None,
            "strandedness": "unstranded",
            "transcriptome_fasta": None,
            "transcriptome_gtf": None,
        },
        "analysis": "RNA-seq",
        "description": None,
        "files": None,
        "genome_build": None,
        "metadata": {},
    }
    submit_template = Template(
        dedent(
            """\
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
    """
        )
    )


job_types = {
    "dge": DgeBcbioJob,
    "bulk": BulkRnaseqBcbioJob,
}
