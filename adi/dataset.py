import os
import io
import requests
import pandas as pd
from typing import Dict

from .common import is_uuid, gql_query, read_code
from . import organization

class DatasetAPI:
  def __init__(self, connection):
    self.__connection = connection

  def get(self, uuid_or_name, raw=False, as_text=True, format=None):
    uuid = self.__resolve_to_uuid(uuid_or_name)
    connection = self.__connection

    if not uuid:
      raise ValueError(f"Dataset not found for {uuid_or_name}")

    headers = { 'Authorization': f"Api-Key {connection.api_key}" }
    download_url = f"{connection.host}/dataset/{uuid}"
    if format:
      download_url = f"{download_url}?type={format}"

    response = requests.get(download_url, headers=headers)

    if raw:
      if as_text:
        return response.content.decode('utf-8')
      else:
        return response.content
    else:
      return pd.read_csv(io.StringIO(response.content.decode('utf-8')))

  def meta(self, uuid_or_name):
    datasetUuid = None
    datasetName = None
    
    if not is_uuid(uuid_or_name):
      datasetName = uuid_or_name
    else:
      datasetUuid = uuid_or_name

    query = '''
    query ($org: OrganizationRef, $datasetUuid: String, $datasetName: String) {
      dataset(org: $org, uuid: $datasetUuid, name: $datasetName) {
        id
        name
        uuid
      }
    }
    '''

    variables = dict(
      org = self.__default_organization(),
      datasetUuid = datasetUuid,
      datasetName = datasetName
    )

    results = gql_query(query, variables=variables, connection=self.__connection)
    
    if len(results['dataset']) > 1:
      raise ValueError("Couldn't find unique dataset for name or uuid")

    return results['dataset'][0]

  def list(self):
    query = '''
    query ($org: OrganizationRef) {
      dataset(org: $org) {
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

    return results['dataset']

  def create(self, name=None, type=None):
    valid_name = ((not name) or (isinstance(name, str) and not is_uuid(name)))
    assert valid_name, f"{name} is not a valid name"

    query = '''
    mutation ($ownerId: String!, $datasetName: String, $type: DatasetType) {
      createDataset(name: $datasetName, owner: $ownerId, type: $type) {
        name
        id
        uuid
        type
      }
    }
    '''
    
    variables = dict(
      ownerId = self.__default_organization()['uuid'],
      datasetName = name,
      type = type
    )
      
    if type:
      variables['type'] = type
    
    result = gql_query(query, variables=variables, connection=self.__connection)
    
    return result['createDataset']

  def upload(self, uuid_or_name, file, type=None):
    uuid = self.__ensure_dataset(uuid_or_name, type=type)
    
    query = '''
    mutation UploadDataset($uuid: String!, $file: Upload!) {
      updateDataset(uuid: $uuid, file: $file) {
        id
        uuid
        name
      }
    }
    '''
    
    variables = dict(
      uuid = uuid,
      file = 'null' # this is important for the graphql upload standard
    )

    result = gql_query(query, variables=variables, file=file, connection=self.__connection)
    
    return result['updateDataset']

  def define(self, uuid_or_name, path=None, code=None, template:str = None, inputs:Dict[str,str] = {}, type='csv'):
    if (path or code) and not (template or inputs):
      return self.__create_basic_transformation(uuid_or_name, path, code, type=type)
    elif (template and inputs):
      return self.__create_transformation_ref(uuid_or_name, template, inputs, type=type)

    raise Exception("Must provide code, a path, or a transformation template name and inputs")

  def generate(self, uuid_or_name):    
    uuid = self.__ensure_dataset(uuid_or_name)

    query = '''
    mutation GenerateDataset($uuid: String!) {
      generateDataset(uuid: $uuid) {
        id
        uuid
        name
      }
    }
    '''
    result = gql_query(query, variables={'uuid':uuid}, connection=self.__connection)
    
    return result['generateDataset']

  def delete(self, uuid_or_name):
    uuid = self.__resolve_to_uuid(uuid_or_name)
    if not uuid:
      raise ValueError("Can't find the dataset to delete")

    query = '''
    mutation DeleteDataset($uuid: String!) {
      deleteDataset(uuid: $uuid)
    }
    '''
    result = gql_query(query, variables={'uuid':uuid}, connection=self.__connection)
    
    return result['deleteDataset']

  def __create_basic_transformation(self, uuid_or_name, path=None, code=None, type='csv'):
    if not path and not code:
      raise ValueError("Need to either give a path to a transformation code file or the code itself")
    
    if path and code:
      raise ValueError("Give either a path to a transformation code file or code, not both")
      
    uuid = self.__ensure_dataset(uuid_or_name, type=type)
    code = read_code(path)
      
    query = '''
    mutation SaveInputTransformation($uuid: String!, $code: String) {
      saveInputTransformation(uuid: $uuid, code: $code) {
        id
      }
    }
    '''

    variables = dict(
      uuid = uuid,
      code = code
    )
    
    result = gql_query(query, variables=variables, connection=self.__connection)
    
    return result['saveInputTransformation']

  def __create_transformation_ref(self, uuid_or_name, template:str, inputs:Dict[str,str], type='csv'):
    query = '''
      mutation TemplateTransformation($output: String!, $template: TemplateRef, $inputs: [TransformationInputMapping], $org: OrganizationRef) {
        saveInputTransformation(
          uuid: $output,
          template: $template,
          inputs: $inputs,
          org: $org
        ) {
          id
          uuid
          name
        }
      }
    '''

    variables = dict(
      output   = self.__ensure_dataset(uuid_or_name, type=type),
      template = dict(name=template),
      inputs   = [dict(alias=k, dataset=dict(name=v)) for k,v in inputs.items()],
      org      = self.__default_organization(),
    )

    result = gql_query(query, variables=variables, connection=self.__connection)

    return result['saveInputTransformation']

  def __ensure_dataset(self, uuid_or_name, type='csv'):
    uuid = self.__resolve_to_uuid(uuid_or_name)
    if not uuid and (not uuid_or_name or isinstance(uuid_or_name, str)):
      info = self.create(uuid_or_name, type=type)
      uuid = info['uuid']

    return uuid

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
      