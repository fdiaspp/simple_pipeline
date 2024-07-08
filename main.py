from infra.executor import DAG
import json
import click
import yaml


@click.command()
@click.option('-p', '--pipeline', required=True, help='The path to the pipeline YAML file.')
def simple_pipeline(pipeline: str):
    dag = DAG()
    with open(pipeline) as stream:
        try:
            pipeline = yaml.safe_load(stream)
            for job in pipeline['jobs']:
                dag.add(job=job, depends_on=pipeline['jobs'][job].get('depends_on', []),  job_args=[json.dumps(pipeline['jobs'][job])])
        
        except yaml.YAMLError as exc:
            print(exc)
    
    dag.execute()


if __name__ == "__main__":
    simple_pipeline()