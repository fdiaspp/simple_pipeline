from infra.executor import DAG
import json


if __name__ == "__main__":
    d = DAG()

    d.add(job='refining', depends_on=['admission'], job_args=['{"hello":"world"}'])
    d.add(job='admission', depends_on=[], job_args=['{"hello":"world"}'])    
    
    d.execute()