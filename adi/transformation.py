import os
from typing import Dict

from .common import gql_query, read_code
from . import organization

__all__ = ['define']

def define(name, path=None, code=None, inputs=[], host=None, api_key=None):
  if not path and not code:
    raise ValueError("Need to either give a path to a transformation code file or the code itself")
  
  if path and code:
    raise ValueError("Give either a path to a transformation code file or code, not both")

  org = organization.default()

  code = read_code(path)

  query = '''
    mutation CreateTransformationTemplate($name: String!, $inputs: [String], $code: String!, $owner: OrganizationRef!) {
      createTransformationTemplate(
        name: $name,
        inputs: $inputs,
        code: $code,
        owner: $owner
      ) {
        id
        uuid
        name
      }
    }
  '''
  
  variables = {'name':name, 'inputs': inputs, 'code': code, 'owner': org}
  result = gql_query(query, variables=variables, host=host, api_key=api_key)
  
  return result['createTransformationTemplate']
