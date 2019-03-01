firestarter
===========

firestarter is a Python package that automates job submission to the o2 cluster at
Harvard Medical School. The idea is that all the information about a job is given in a
single CSV file, including paths to input files, job parameters and experiment metadata.
The package then automates copying the files from remote servers (such as the transfer
node), merging of files (especially fastq files) and creation of the working directory.

At the moment, only a limited number of job types are supported (see :doc:`job_types`)
New job types can be easily created, because all inputs and parameters are parametrized
using a simple object model.

Example usage
=============

This is an example of a :py:class:`~BulkRnaseqBcbioJob` with 8 samples.
The fastq files are stored on the ``/n/files/ImStor`` file system. Therefore, their path
is prefixed with ``transfer.rc.hms.harvard.edu`` this file system is only accessible
from that node and will be copied to the working directory during job setup.

The required columns for this job type are:

:id:
    The unique sample id. Merging of fastq files happens based on the unique id.

:fastq:
    Path to fastq files. Supports copying from remote servers using SCP using the syntax
    ``host:path``.

:transcriptome_fasta:
    Path to a fasta file containing the transcriptome sequences.

:transcripome_gtf:
    Path to a GTF file containing the gene models.

The remaining columns are metadata describing the sample condition.

.. csv-table:: bulk_rna_job_example.csv
   :file: ../examples/bulk_rna_job_example.csv
   :header-rows: 1
   :widths: auto

Job creation can be kicked off using:

.. code-block:: shell

    firestarter bulk bulk_rna_job_example.csv \
        --wd /n/scratch2/ch305/important_project/alignment/

The script will create a run directory using a unique name in the specified working
directory, copy the files, merge files (if necessary), create Bcbio directory structure
and submit the Bcbio job.
