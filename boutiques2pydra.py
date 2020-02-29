from boutiques.descriptor2func import function
from boutiques.util.utils import loadJson
from boutiques import evaluate, example
from json import dumps, loads
import pydra
import typing as ty
import argparse
import sys


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

    def __init__(self, descriptor, *args):
        self.desc_json = dumps(loadJson(descriptor))
        invocation = example(self.desc_json)
        tool_outputs = evaluate(self.desc_json, invocation, "output-files")
        self.output_types = {k: ty.Any for k in tool_outputs.keys()}
        self.options = args
        print(str(self.create_task))

        self.create_task = pydra.mark.task(self.create_task)
        self.create_task = pydra.mark.annotate({ "return" : self.output_types })(self.create_task)


    def create_task(self, infile, **kwargs):
        tool = function(self.desc_json)
        ret = tool(*self.options, **kwargs)

        if ret.exit_code != 0:
            raise Exception(ret.stdout, ret.stderr)
        
        print(ret)
        return [out_f.file_name for out_f in ret.output_files]

"""
@pydra.mark.task 
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
        return [out_f.file_name for out_f in ret.output_files]


    print(kwargs)
    task = pydra_tool(args, kwargs)

    return task
"""

#print(boutiques2pydra("zenodo.3267250", infile="test.nii", maskfile="test"))
