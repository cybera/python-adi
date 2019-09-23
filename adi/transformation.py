import os
from typing import Dict

from .common import gql_query, read_code, is_uuid
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

  def meta(self, uuid_or_name):
    transformationUuid = None
    transformationName = None
    
    if not is_uuid(uuid_or_name):
      transformationName = uuid_or_name
    else:
      transformationUuid = uuid_or_name

    query = '''
    query ($org: OrganizationRef, $transformationUuid: String, $transformationName: String) {
      transformation(org: $org, uuid: $transformationUuid, name: $transformationName) {
        id
        name
        uuid
      }
    }
    '''

    variables = dict(
      org = self.__default_organization(),
      transformationUuid = transformationUuid,
      transformationName = transformationName
    )

    results = gql_query(query, variables=variables, connection=self.__connection)

    return results['transformation']

  def delete(self, uuid_or_name):
    uuid = self.__resolve_to_uuid(uuid_or_name)
    if not uuid:
      raise ValueError("Can't find the transformation to delete")

    query = '''
    mutation DeleteTransformation($uuid: String!) {
      deleteTransformation(uuid: $uuid)
    }
    '''
    result = gql_query(query, variables={'uuid':uuid}, connection=self.__connection)
    
    return result['deleteTransformation']

  def list(self):
    query = '''
    query ($org: OrganizationRef!) {
      transformations(org: $org) {
        id
        name
        uuid
      }
    }
    '''
    
    variables = dict(
      org = self.__default_organization()
    )
    
    results = gql_query(query, variables=variables, connection=self.__connection)

    return results['transformations']

  def __default_organization(self):
    return self.__connection.organization.default()

  def __resolve_to_uuid(self, uuid_or_name):
    if not is_uuid(uuid_or_name):
      info = self.meta(uuid_or_name)
      if info:
        return info['uuid']
      else:
        return None
    else:
      return uuid_or_name
