import abc
import inspect
import itertools
import os
import pathlib
import shutil
import subprocess
import typing
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from string import Template
from textwrap import dedent
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Text,
    Tuple,
    Union,
)

import numpy as np
import pandas as pd
import yaml
from attr import attrib, attrs

from .transfer import transfer_files_batch
from .util import PathLike, combine_pairs, merge_files, normalize_path

SLURM_PARAMS_DEFAULT = {
    "time_limit": "0-12:00",
    "cores": "12",
    "queue": "short",
    "mem": "8000",
}


EMPTY_DEFAULT = "EMPTY_DEFAULT"


@attrs(auto_attribs=True)
class JobParameter(object):
    name: Text
    path: Union[Text, List[Text]] = attrib(repr=False)
    description: Text = attrib(default="")
    default: Optional[Any] = attrib(default=None, kw_only=True)
    per_sample: bool = attrib(default=True, kw_only=True)
    _compatible_types = tuple(yaml.SafeDumper.yaml_representers.keys())[:-1]

    @staticmethod
    def _nested_set(d: Dict, path: Union[Text, List[Text]], value: Any) -> None:
        # From https://stackoverflow.com/a/13688108/4603385
        if not isinstance(path, typing.List):
            path = [path]
        for key in path[:-1]:
            d = d.setdefault(key, {})
        d[path[-1]] = value

    @staticmethod
    def _basic_type(value: Any) -> Any:
        if not isinstance(value, Sequence):
            value = list(value)
        if not isinstance(value[0], JobParameter._compatible_types):
            value = list(str(x) for x in value)
        if len(value) > 1:
            return value
        return value[0]

    def add_default_data(
        self, data: pd.DataFrame, job: "BcbioJob" = None
    ) -> pd.DataFrame:
        data = data.copy()
        if self.name not in data:
            if self.default is None:
                raise ValueError("{self.name} not found in ", str(data))
            if callable(self.default):
                data[self.name] = self.default(job)  # pylint: disable=not-callable
            elif self.default is EMPTY_DEFAULT:
                # Don't add column, is supposed to be empty
                return data
            else:
                data[self.name] = self.default
        return data

    def get_sample_param(self, data: pd.DataFrame, job: "BcbioJob" = None) -> Any:
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

    def set_param_meta(self, data: pd.DataFrame, meta: Dict) -> None:
        v = self.get_sample_param(data)
        if v is EMPTY_DEFAULT:
            return
        self._nested_set(meta, self.path, v)


def merge_rule_none(
    job: "BcbioJob", parameter: JobParameter, data: pd.DataFrame
) -> pd.DataFrame:
    return data


def merge_rule_fastq(
    job: "BcbioJob", parameter: JobParameter, data: pd.DataFrame
) -> pd.DataFrame:
    def merge_destination(sample_id, i, files):
        ext = "".join(pathlib.Path(files[0]).suffixes)
        merged_name = f"{sample_id}_merged_{i}"
        merged_dir = job.file_destinations[parameter.name] / "merged"
        merged_dir.mkdir(exist_ok=True)
        return merged_dir / (merged_name + ext)

    def merge_per_sample(sample_id, sample_data):
        files = sample_data[parameter.name]
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
                dest = merge_files(files, merge_destination(sample_id, i + 1, files))
                files_destination.append(dest)
        else:
            dest = merge_files(pairs, merge_destination(sample_id, 1, pairs))
            files_destination = [dest]
        return files_destination

    sample_groups = data.groupby("id")
    merged_data = []
    for sample_id, sample_data in sample_groups:
        d = merge_per_sample(sample_id, sample_data)
        # Guaranteed by check_data in job class that values are unique per sample
        m = sample_data.copy().head(n=len(d))
        m[parameter.name] = d
        merged_data.append(m)
    return pd.concat(merged_data, ignore_index=True)


@attrs
class FileJobParameter(JobParameter):
    destination: PathLike = attrib(kw_only=True, repr=False)
    merge_rule: Callable = attrib(kw_only=True, default=merge_rule_none, repr=False)

    def merge_files(self, job: "BcbioJob", data: pd.DataFrame) -> pd.DataFrame:
        return self.merge_rule(job, self, data)  # pylint: disable=not-callable


BCBIOJOB_PARAMS = [
    JobParameter("id", "description"),
]


class BcbioJob(abc.ABC):
    """Class representing jobs using the Bcbio framework."""

    params = BCBIOJOB_PARAMS
    run_directory: Path
    working_directory: Optional[Path]

    def __init__(
        self,
        name: Text,
        data: pd.DataFrame,
        working_directory: Optional[PathLike] = None,
        run_directory: Optional[PathLike] = None,
        slurm_params: Mapping = SLURM_PARAMS_DEFAULT,
        debug: bool = False,
    ):
        self.name = name
        if run_directory:
            self.run_directory = Path(normalize_path(run_directory))
            self.working_directory = None
        elif working_directory:
            self.working_directory = Path(normalize_path(working_directory))
        else:
            raise ValueError("Either working or run directory must be set")
        self.data = data
        self.slurm_params = slurm_params
        self.debug = debug
        self.check_data(data)

    def check_data(self, data: pd.DataFrame) -> None:
        """Checks if data supplied to the job instance are complete and valid.

        Args:
            data: DataFrame describing the job parameters.
        Raises:
            ValueError: Columns are missing or non-unique values in per sample columns.
        """
        required_cols = set(p.name for p in self.required_params)
        missing_cols = required_cols - set(data)
        if len(missing_cols) > 0:
            raise ValueError(
                "The following required columns are missing from data:\n",
                str(missing_cols),
            )
        # All columns that are metadata and per-sample job paramters need to
        # have a single unique value per sample id in the job data
        per_sample_cols = list(
            (set(self.meta_cols) | set(p.name for p in self.per_sample_params))
            & set(data)
        )
        sample_groups = data.groupby("id")
        for g, d in sample_groups:
            unique_vals = d[per_sample_cols].agg(lambda x: len(x.unique())) == 1
            if not unique_vals.all():
                raise ValueError(
                    f"Sample {g} has multiple values for the per sample columns ",
                    str(list(unique_vals.index[unique_vals])),
                )

    def add_defaults(self, data: pd.DataFrame) -> pd.DataFrame:
        """Adds defaults of optional parameters to job data.

        Args:
            data: DataFrame describing the job parameters.
        Returns:
            DataFrame with optional parameters added, if not present before.
        """
        data = data.copy()
        for p in self.optional_params:
            data = p.add_default_data(data, job=self)
        return data

    @property
    def file_params(self) -> List[FileJobParameter]:
        """Gets a list of all job parameters that are file inputs."""
        return [p for p in self.params if isinstance(p, FileJobParameter)]

    @property
    def file_destinations(self) -> Dict[Text, Path]:
        """Gets a dictionary of the destinations of all file inputs for this job."""
        if not self.run_directory:
            raise RuntimeError("Run directory not set yet, destinations unknown")
        return {p.name: self.run_directory / p.destination for p in self.file_params}

    @property
    def required_params(self) -> List[JobParameter]:
        """Gets a list of all required job parameters."""
        return [p for p in self.params if p.default is None]

    @property
    def optional_params(self) -> List[JobParameter]:
        """Gets a list of all optional job parameters."""
        return [p for p in self.params if p.default is not None]

    @property
    def per_sample_params(self) -> List[JobParameter]:
        """Gets a list of all job parameters for which only a single value
        per sample is allowed."""
        return [p for p in self.params if p.per_sample]

    @property
    def meta_cols(self) -> List[Text]:
        """Gets a list of column names in the job data that are not job parameters
        but metadata."""
        return list(set(self.data) - set(p.name for p in self.params))

    def prepare_working_directory(self) -> None:
        if self.working_directory:
            self.working_directory.mkdir(exist_ok=True)

    def prepare_run_directory(self) -> None:
        """Prepares run directory creating all necessary subdirectories."""
        self.run_id = (
            datetime.now().isoformat(timespec="minutes").replace(":", "_")
            + "_"
            + self.name
        )
        if not getattr(self, "run_directory", False) and self.working_directory:
            self.run_directory = self.working_directory / self.run_id
        self.run_directory.mkdir(exist_ok=True)
        (self.run_directory / "config").mkdir(exist_ok=True)
        (self.run_directory / "work").mkdir(exist_ok=True)
        for d in self.file_destinations.values():
            d.mkdir(exist_ok=True)

    def transfer_files(self, data: pd.DataFrame) -> pd.DataFrame:
        """Transfers all input files for this job to the run directory.

        Args:
            data: DataFrame describing the job parameters.
        Returns:
            DataFrame with job parameters with file locations updated to reflect
            their new location within the run directory.
        Raises:
            RuntimeError: Files not found at new location.
        """
        data = data.copy()
        file_transfers = {}
        destinations = self.file_destinations
        for p in (x for x in self.file_params if x.name in data):
            unique_files = list(set(data[p.name]))
            transfers = list(zip(unique_files, itertools.repeat(destinations[p.name])))
            file_transfers[p.name] = transfers
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

    def merge_files(self, data: pd.DataFrame) -> pd.DataFrame:
        # Unique values for all per-sample cols are guaranteed by check_data method
        merge_params = [
            p for p in self.file_params if p.merge_rule is not merge_rule_none
        ]
        if not merge_params:
            return data
        if len(merge_params) > 1:
            raise RuntimeError(
                "Merging more than one of the file inputs is currently not supported"
            )
        p = merge_params[0]
        data_merged = p.merge_files(self, data)
        return data_merged

    def _prepare_run(self) -> None:
        self.prepare_working_directory()
        self.prepare_run_directory()
        data_defaults = self.add_defaults(self.data)
        data_normalized = self.normalize_paths(data_defaults)
        data_transferred = self.transfer_files(data_normalized)
        data_merged = self.merge_files(data_transferred)
        self.prepare_meta(data_merged)

    def prepare_run(self) -> None:
        if self.debug:
            self._prepare_run()
            return
        try:
            self._prepare_run()
        except Exception as e:
            if self.run_directory.exists():
                shutil.rmtree(self.run_directory)
            raise RuntimeError("Error during run directory preparation") from e

    def submit_run(self) -> None:
        cp = subprocess.run(
            ["sbatch", f"{self.name}_run.sh"],
            capture_output=True,
            cwd=self.run_directory / "work",
        )
        if not cp.returncode == 0:
            raise RuntimeError("Job submission unsuccesfull:\n", cp.stderr)

    def normalize_paths(self, data: pd.DataFrame) -> pd.DataFrame:
        data = data.copy()
        for n in (p.name for p in self.file_params if p.name in data):
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
    FileJobParameter(
        "fastq",
        "files",
        destination="fastq",
        per_sample=False,
        merge_rule=merge_rule_fastq,
    ),
]


class RnaseqGenericBcbioJob(BcbioJob):
    sample_meta: Dict[Text, Any]
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
    bcbio_nextgen.py ../config/$name.yaml -n $cores -t ipython -s slurm -q $queue -r t=$time_limit --timeout 120
    """
        )
    )
    params = RNASEQJOB_PARAMS

    def prepare_meta(self, data: pd.DataFrame) -> pd.DataFrame:
        sample_meta_list = []
        data_by_sample = data.groupby("id")
        for _, g in data_by_sample:
            m = deepcopy(self.sample_meta)
            for p in self.params:
                p.set_param_meta(g, m)
            m["metadata"] = {c: list(g[c])[0] for c in self.meta_cols}
            sample_meta_list.append(m)
        sample_meta = {
            "details": sample_meta_list,
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
    job_type = "bcbio_dge"
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


BULKRNASEQJOB_PARAMS = RNASEQJOB_PARAMS + [
    JobParameter("aligner", ["algorithm", "aligner"], default="hisat2"),
    JobParameter("strandedness", ["algorithm", "strandedness"], default="unstranded"),
    JobParameter("aligner", ["algorithm", "aligner"], default="hisat2"),
    FileJobParameter(
        "spikein_fasta",
        ["algorithm", "spikein_fasta"],
        default=EMPTY_DEFAULT,
        destination="transcriptome",
    ),
]


class BulkRnaseqBcbioJob(RnaseqGenericBcbioJob):
    job_type = "bcbio_bulk"
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


job_types = {
    cls.job_type: cls
    for cls in globals().values()
    if (inspect.isclass(cls) and issubclass(cls, BcbioJob) and hasattr(cls, "job_type"))
}
