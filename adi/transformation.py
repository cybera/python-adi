import os
from typing import Dict

from .common import gql_query, read_code
from . import organization


class TransformationAPI:
  def __init__(self, connection):
    self.__connection = connection
    self.__default_org = None

  def define(self, name, path=None, code=None, inputs=[]):
    if not path and not code:
      raise ValueError("Need to either give a path to a transformation code file or the code itself")
    
    if path and code:
      raise ValueError("Give either a path to a transformation code file or code, not both")

    org = self.__connection.organization.default()

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
    result = gql_query(query, variables=variables, connection=self.__connection)
    
    return result['createTransformationTemplate']
