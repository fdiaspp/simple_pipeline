# Pipelines

Pipeline is a group of operations that need to be met in order to get the results expected.

Each YAML found in that directory contains a structure with multiple jobs defined. Each job has the following components:

- depends_on: Lists the jobs that this job depends on.
- input: Defines the input type and path for the job.
- transformation: Contains transformation operations for the job.
- cleansing: Specifies cleansing operations for the job.
- output: Specifies the output type, path, mode, and partitioning settings for the job.
