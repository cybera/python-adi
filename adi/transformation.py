import os
from typing import Dict

from .common import gql_query, read_code, is_uuid
from .api_base import OrganizationAwareAPI


class TransformationAPI(OrganizationAwareAPI):
  def define(self, name, path=None, code=None, inputs=[], tags=[], description=''):
    if not path and not code:
      raise ValueError("Need to either give a path to a transformation code file or the code itself")

    if path and code:
      raise ValueError("Give either a path to a transformation code file or code, not both")

    org = self._connection.organization.default()

    if code is None:
      code = read_code(path)

    query = '''
      mutation CreateTransformationTemplate($name: String!, $description: String, $inputs: [String], $code: String!, $owner: OrganizationRef!, $tags: [String]) {
        createTransformationTemplate(
          name: $name,
          description: $description,
          inputs: $inputs,
          code: $code,
          owner: $owner,
          tagNames: $tags
        ) {
          id
          uuid
          name
          description
          inputs
          code
          tags {
            name
          }
        }
      }
    '''

    variables = {'name':name, 'description': description, 'inputs': inputs, 'code': code, 'owner': org, 'tags': tags}
    result = gql_query(query, variables=variables, connection=self._connection)

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
      org = self._default_organization(),
      transformationUuid = transformationUuid,
      transformationName = transformationName
    )

    results = gql_query(query, variables=variables, connection=self._connection)

    return results['transformation']

  def update(self, uuid_or_name, name=None, inputs=None, code=None, tags=None, description=''):
    uuid = self._resolve_to_uuid(uuid_or_name)
    if not uuid:
      raise ValueError("Can't find the transformation to update")

    query = '''
    mutation UpdateTransformation($uuid: String!, $name: String, $description: String, $inputs: [String], $code: String, $tags: [String]) {
      updateTransformation(uuid: $uuid, fields: {name: $name, description: $description, inputs: $inputs, code: $code, tagNames: $tags}) {
        uuid
        name
        description
        inputs
        code
        tags {
          name
        }
      }
    }
    '''
    result = gql_query(query, variables={'uuid':uuid, 'name':name, 'description':description, 'inputs':inputs, 'code':code, 'tags': tags}, connection=self._connection)

    return result['updateTransformation']

  def delete(self, uuid_or_name):
    uuid = self._resolve_to_uuid(uuid_or_name)
    if not uuid:
      raise ValueError("Can't find the transformation to delete")

    query = '''
    mutation DeleteTransformation($uuid: String!) {
      deleteTransformation(uuid: $uuid)
    }
    '''
    result = gql_query(query, variables={'uuid':uuid}, connection=self._connection)

    return result['deleteTransformation']

  def list(self):
    query = '''
    query ($org: OrganizationRef!) {
      listTransformations(org: $org) {
        transformations {
          id
          name
          uuid
        }
      }
    }
    '''

    variables = dict(
      org = self._default_organization()
    )

    results = gql_query(query, variables=variables, connection=self._connection)

    return results['listTransformations']['transformations']
