"""Regenerate with the sibling vgi-python wire-2.2 environment:
cd ~/Development/vgi-python
uv run python ../vgi-csharp/test/QueryFarm.Vgi.Tests/Vectors/generate_function_binary_dictionary_replacement.py
The three one-row batches replace binary dictionaries; default payloads are
opaque bytes to test the byte codec, intentionally not nested typed IPC.
"""
import json
from pathlib import Path
import pyarrow as pa
from vgi.catalog import FunctionInfo, FunctionType, SerializedSchema
from vgi.schema_path import SchemaPath
from vgi_rpc.utils import new_ipc_stream

items=[(b'',b'\x00\xff',None),(b'\x00\xff',b'',b''),(b'\x80\x00\xfe',b'\xff\x00\x81',b'\x00\xff\x80')]
rows=[]
for i,(arguments,output,defaults) in enumerate(items):
    row=FunctionInfo(name=f'row_{i}',comment=None,tags={},schema_path=SchemaPath(['main']),function_type=FunctionType.SCALAR,arguments=SerializedSchema(arguments),output_schema=SerializedSchema(output))._to_row_dict()
    row['parameter_default_values']=defaults
    rows.append(row)

def write(path,source):
    with pa.OSFile(path,'wb') as sink, new_ipc_stream(sink,FunctionInfo.ARROW_SCHEMA) as writer:
        for row in source:
            writer.write_batch(pa.RecordBatch.from_pylist([row],schema=FunctionInfo.ARROW_SCHEMA))
    batches=list(pa.ipc.open_stream(path))
    assert len(batches)==3
    for batch,row in zip(batches,source,strict=True):
        batch.validate(full=True)
        for name in ('arguments','output_schema','parameter_default_values'):
            assert batch.column(name).to_pylist()==[row[name]]
    return [{'name':row['name'],**{name:None if row[name] is None else row[name].hex() for name in ('arguments','output_schema','parameter_default_values')}} for row in source]

path=str(Path(__file__).with_name('function_binary_dictionary_replacement.arrow'))
expected=write(path,rows)

print(path,Path(path).stat().st_size)
print(json.dumps(expected,indent=2))
