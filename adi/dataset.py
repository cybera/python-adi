import os
import io
import requests
import pandas as pd
from typing import Dict

from .common import is_uuid, gql_query, read_code
from . import organization

def get(uuid_or_name, raw=False, as_text=True, host=None, api_key=None):
  if api_key is None:
    api_key = os.environ.get('ADI_API_KEY')
  if host is None:
    host = os.environ.get('ADI_API_HOST')

  uuid = uuid_or_name

  if not is_uuid(uuid_or_name):
    uuid = meta(uuid_or_name, host=host, api_key=api_key)['uuid']

  headers = { 'Authorization': f"Api-Key {api_key}" }
  response = requests.get(f"{host}/dataset/{uuid}", headers=headers)

  if raw:
    if as_text:
      return response.content.decode('utf-8')
    else:
      return response.content
  else:
    return pd.read_csv(io.StringIO(response.content.decode('utf-8')))

def meta(uuid_or_name, host=None, api_key=None):
  org = organization.default()

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
    org = org,
    datasetUuid = datasetUuid,
    datasetName = datasetName
  )

  results = gql_query(query, variables=variables, host=host, api_key=api_key)
  
  if len(results['dataset']) > 1:
    raise ValueError("Couldn't find unique dataset for name or id")

  return results['dataset'][0]

def list(host=None, api_key=None):
  org = organization.default()

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
    org = org
  )
  
  results = gql_query(query, variables=variables, host=host, api_key=api_key)

  return results['dataset']

def create(name, host=None, api_key=None, type=None):
  orgid = organization.default()['uuid']

  query = '''
  mutation ($ownerId: String!, $datasetName: String!, $type: DatasetType) {
    createDataset(name: $datasetName, owner: $ownerId, type: $type) {
      name
      id
      uuid
      type
    }
  }
  '''
  
  variables = dict(
    ownerId = orgid,
    datasetName = name,
    type = type
  )
    
  if type:
    variables['type'] = type
  
  result = gql_query(query, variables=variables, host=host, api_key=api_key)
  
  return result['createDataset']

def upload(uuid_or_name, file, type=None, host=None, api_key=None):
  info = meta(uuid_or_name, host=host, api_key=api_key)

  if not info:
    info = create(uuid_or_name, host=host, api_key=api_key, type=type)
  
  if not is_uuid(uuid_or_name):
    uuid = info['uuid']
  else:
    uuid = uuid_or_name
  
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
    file = 'null'
  )

  result = gql_query(query, variables=variables, file=file, host=host, api_key=api_key)
  
  return result['updateDataset']

def define(uuid_or_name, path=None, code=None, template:str = None, inputs:Dict[str,str] = {}, type='csv', host=None, api_key=None):
  if (path or code) and not (template or inputs):
    return create_basic_transformation(uuid_or_name, path, code, type=type, host=host, api_key=api_key)
  elif (template and inputs):
    return create_transformation_ref(uuid_or_name, template, inputs, type=type, host=host, api_key=api_key)

  raise Exception("Must provide code, a path, or a transformation template name and inputs")

def ensure_dataset(uuid_or_name, type='csv', host=None, api_key=None):
  if not is_uuid(uuid_or_name):
    info = meta(uuid_or_name, host=host, api_key=api_key)
    if not info and isinstance(uuid_or_name, str):
      info = create(uuid_or_name, type=type, host=host, api_key=api_key)
    elif not isinstance(uuid_or_name, str):
      raise ValueError("Can't create a new dataset with a numerical id")
    uuid = info['uuid']
  else:
    uuid = uuid_or_name

  return uuid

def generate(uuid_or_name, host=None, api_key=None):    
  if not is_uuid(uuid_or_name):
    info = meta(uuid_or_name, host=host, api_key=api_key)
    if not info and isinstance(uuid_or_name, str):
      info = create(uuid_or_name, host=host, api_key=api_key)
    elif not isinstance(uuid_or_name, str):
      raise ValueError("Can't create a new dataset with a numerical id")
    uuid = info['uuid']
  else:
    uuid = uuid_or_name

  query = '''
  mutation GenerateDataset($uuid: String!) {
    generateDataset(uuid: $uuid) {
      id
      uuid
      name
    }
  }
  '''
  result = gql_query(query, variables={'uuid':uuid}, host=host, api_key=api_key)
  
  return result['generateDataset']

def delete(uuid_or_name, host=None, api_key=None):    
  if not is_uuid(uuid_or_name):
    info = meta(uuid_or_name, host=host, api_key=api_key)
    if not info:
      raise ValueError("Can't find the dataset to delete")
    uuid = info['uuid']
  else:
    uuid = uuid_or_name

  query = '''
  mutation DeleteDataset($uuid: String!) {
    deleteDataset(uuid: $uuid)
  }
  '''
  result = gql_query(query, variables={'uuid':uuid}, host=host, api_key=api_key)
  
  return result['deleteDataset']

def create_basic_transformation(uuid_or_name, path=None, code=None, type='csv', host=None, api_key=None):
  if not path and not code:
    raise ValueError("Need to either give a path to a transformation code file or the code itself")
  
  if path and code:
    raise ValueError("Give either a path to a transformation code file or code, not both")
    
  uuid = ensure_dataset(uuid_or_name, type=type, host=host, api_key=api_key)
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
  
  result = gql_query(query, variables=variables, host=host, api_key=api_key)
  
  return result['saveInputTransformation']

def create_transformation_ref(uuid_or_name, template:str, inputs:Dict[str,str], type='csv', host=None, api_key=None):
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

  org = organization.default()

  variables = dict(
    output   = ensure_dataset(uuid_or_name, type=type, host=host, api_key=api_key),
    template = dict(name=template),
    inputs   = [dict(alias=k, dataset=dict(name=v)) for k,v in inputs.items()],
    org      = org,
  )

  result = gql_query(query, variables=variables, host=host, api_key=api_key)

  return result['saveInputTransformation']
