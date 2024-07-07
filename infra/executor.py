from typing import Any, List
import docker
from pathlib import Path
from datetime import datetime
from graphlib import TopologicalSorter


class DAG:
    
    def __init__(self):
        self.ts = TopologicalSorter()
        self.executor = Executor()
        self.jobs_descriptor = {}
    
    def add(self, job: str, depends_on: List[str], job_args: List[str]):
        """
        Adds a job to the DAG with its dependencies and job arguments.

        Parameters:
            job (str): The job to be added.
            depends_on (List[str]): List of jobs that this job depends on.
            job_args (List[str]): The arguments for the job.

        Returns:
            None
        """
        self.ts.add(job, *depends_on)
        self.jobs_descriptor[job] = job_args        
    
    def execute(self):
        """
        Executes the jobs in the DAG in a topological order.
        """
        jobs_to_execute = tuple(self.ts.static_order())
        print("Jobs to execute: ", jobs_to_execute)
        
        for job in jobs_to_execute:
            self.executor.execute(job=job, jobs_args=self.jobs_descriptor[job])

    
class Executor:

    def __init__(self):
        self._client = docker.from_env()

    def execute(self, job: str, jobs_args: List[str]):
        """
        Executes a job by running a container with specified image, command, and volumes.
        
        Parameters:
            job (Any): The job to be executed.
            jobs_args (List[str]): The arguments for the job.
        """
        execution_name = f'simple_pipeline_{job}_job_' + str(int(datetime.now().timestamp() * 1000))
        print(f'Executing job "{job}.py" with name {execution_name}')
        
        container = self._client.containers.run(
            image="apache/spark-py:v3.4.0",
            command=f'/opt/spark/bin/spark-submit /app/{job}.py ' + self.__generate_json_escaped(jobs_args),
            name=execution_name,
            labels={"poc.simple_pipeline.job": job},
            volumes={
                f'{Path.cwd()}/platform': {'bind': '/app', 'mode': 'rw'}
            },
            remove=False
        )
        print(container.decode('utf-8'))

    def __generate_json_escaped(self, args: List[str]):
        """
        A function that escapes double quotes in a list of strings and joins them with space.
        
        Args:
            args (List[str]): A list of strings to escape double quotes from.
        
        Returns:
            str: A single string with escaped double quotes and spaces joining the elements.
        """
        params_escaped = [arg.replace('"', '\\"') for arg in args]
        return '"' + '" "'.join(params_escaped) + '"'        
