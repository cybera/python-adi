import re
import os
import requests
import mimetypes
import json

class APIError(ValueError):
  def __init__(self, graphql_error_list):
    error_msg = '\n'.join([error['message'] for error in graphql_error_list])
    super().__init__(f"\n\n{error_msg}")

def is_uuid(str):
  # RegEx from: https://stackoverflow.com/a/13653180
  uuid_checker = re.compile('^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$', re.IGNORECASE)
  if uuid_checker.match(str):
    return True
  else:
    return False

# This can take a regular graphql query (with no actual file upload) or one
# that also includes a single upload. To modify in order to take multiple
# files, we'd have to review the following spec and modify as necessary:
#
# https://github.com/jaydenseric/graphql-multipart-request-spec
def gql_query(query, variables=dict(), file=None, connection=None):
  if connection is None:
    raise Exception("Connection info not provided")
  
  headers = {
    'Authorization': f"Api-Key {connection.api_key}"
  }

  escaped_gql = (query
    .strip()
    .replace('\n', '\\n')
    .replace('"', '\\"'))

  gql_json = dict(
    query = query.strip(),
    variables = variables
  )
  
  json_data = json.dumps(gql_json)

  data = None
  files = None
  
  # Depending on whether or not there's a file that we have to send along with the
  # regular JSON data, we can either do a fairly simple post, where we just send
  # the JSON as the data body of the post request, or a more complex one where we
  # need to follow the GraphQL multipart form specification.
  if not file:
    data = json_data
    headers['Content-Type'] = 'application/json'
  else:
    fileName = os.path.basename(file)
    mimetype, encoding = mimetypes.guess_type(file)
    # See: https://github.com/cybera/adi/blob/master/manual/src/sections/ExportingAndImporting.md
    # for the curl command this is based off of.
    files = {
      'operations': (None, json_data, 'application/json'),
      'map': (None, '{ "0": ["variables.file"] }', 'application/json'),
      '0': (fileName, open(file, 'rb'), mimetype)
    }

  r = requests.post(f"{connection.host}/graphql", headers=headers, data=data, files=files)

  json_content = {}

  try:
    json_content = json.loads(r.content)
  except Exception as e:
    print("gql_query problem: ")
    if not json_content:
      print(f"Can't parse JSON for {r.content}")
    raise(e)

  if 'errors' in json_content:
    raise APIError(json_content['errors'])
  elif 'data' in json_content:
    return json_content['data']
  else:
    raise Exception(f"Can't get result data from:\n{json_content}")

  return json_content['data']

def read_code(path:str):
  if path and os.path.exists(path):
    with open(path, 'r') as file:
      return file.read()

  raise FileNotFoundError
