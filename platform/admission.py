from core import Operation, Cleansing
import json
import sys

if __name__ == '__main__':
    print("Params: ", sys.argv[1])
    params = json.loads(sys.argv[1])
    
    ops = Operation()
    input_reader = ops.get_reader(params['input']['type'])
    output_writer = ops.get_writer(params['output']['type'])
    
    df = input_reader(path=params['input']['path'])
    
    cleansing = params.get('cleansing', {})
    for operation in cleansing:
        df = getattr(Cleansing, operation)(df, **cleansing[operation])
    
    output_writer(df, 
                  path=params['output']['path'], 
                  partition_by=params['output'].get('partition_by', []),
                  mode=params['output'].get('mode', 'error'))