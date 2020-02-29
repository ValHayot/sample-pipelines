from boutiques.descriptor2func import function
from boutiques.util.utils import loadJson
from boutiques import evaluate, example
from json import dumps, loads
from inspect import getsource
import pydra
import typing as ty
from types import FunctionType
import argparse
import sys
from os import path as op


def descriptor_parser(descriptor):

    tool = loadJson(descriptor)
    
    parser = argparse.ArgumentParser(tool['name'], description=tool['description'])

    type_mappings = { 'String': str, 'File': str, 'Flag': 'store_true', 'Number': float }

    for in_arg in tool['inputs']:
        if not in_arg['optional']:
            parser.add_argument(in_arg['id'], type=type_mappings[in_arg['type']], help=in_arg['description'])
        else:
            parser.add_argument(in_arg['command-line-flag'], "--{}".format(in_arg['id']), action=type_mappings[in_arg['type']], help=in_arg['description'])

    return parser


class Boutiques2Pydra:

    def __init__(self, descriptor, *args, input_spec=None):
        desc_dict = loadJson(descriptor)
        desc_json = dumps(desc_dict)

        if input_spec is None:
            invocation = example(desc_json)
        else:
            invocation = example(desc_json, "-c")
            invocation = dumps({k:v for k,v in loads(invocation).items() if k in input_spec})
            print(invocation)

        tool_outputs = evaluate(desc_json, invocation, "output-files")
        self.output_types = {k: ty.Any for k in tool_outputs.keys()}
        print(self.output_types)

        parameters = ", ".join([k for k in loads(invocation).keys()])

        self.pydra_task = r'''def pydra_task({0}):

            kwargs = locals()
            tool = function(dumps({1}))
            ret = tool(*{2}, **kwargs)

            if ret.exit_code != 0:
                raise Exception(ret.stdout, ret.stderr)
            
            return [out_f.file_name.encode("utf-8") for out_f in ret.output_files]'''.format(parameters, desc_dict, list(args))

        code = compile(self.pydra_task, "<string>", "exec")

        self.create_task = FunctionType(code.co_consts[0], globals())

        self.create_task = pydra.mark.annotate({ "return" : {"outfile": ty.Any} })(self.create_task)
        self.create_task = pydra.mark.task(self.create_task)


"""
@pydra.mark
def create_task(descriptor, args, **kwargs):
    desc_json = dumps(loadJson(descriptor))

    invocation = example(desc_json)
    
    #tool_inputs = evaluate(desc_json, invocation, "inputs")
    tool_outputs = evaluate(desc_json, invocation, "output-files")

    output_types = {k: ty.Any for k in tool_outputs.keys()}

    #self = pydra.mark.annotate({ "return" : output_types })(self)
    def pydra_tool(args, kwargs):
        print(kwargs)
        tool = function(desc_json)
        ret = tool(*args, **kwargs)

        if ret.exit_code != 0:
            raise Exception(ret.stdout, ret.stderr)
        
        print(ret.output_files)
        return [out_f for out_f in ret.output_files]


    print(kwargs)
    task = pydra_tool(args, kwargs)

    return task
"""

#print(boutiques2pydra("zenodo.3267250", infile="test.nii", maskfile="test"))
