from .common import gql_query

__default_org = None

def set_default(name=None, uuid=None, id=None):
  global __default_org

  assert name or uuid or id, "Must supply one of name, uuid, or id"

  if uuid:
    org_key = 'uuid'
    org_val = uuid
  elif name:
    org_key = 'name'
    org_val = name
  elif id:
    org_key = 'id'
    org_val = id

  info = gql_query('''
    query {
      currentUser {
        organizations {
          name
          id
          uuid
        }
      }
    }
  ''')

  organizations = info['currentUser']['organizations']

  matching_orgs = [org for org in organizations if org[org_key] == org_val]

  if len(matching_orgs) != 1:
    raise Exception(f"should match exactly one organization (matches: {len(matching_orgs)})")
  
  __default_org = matching_orgs[0]

def default(host=None, api_key=None):
  global __default_org
  
  if not __default_org:
    info = gql_query('''
    query {
      currentUser {
        organizations {
          name
          id
          uuid
        }
      }
    }
    ''')
    __default_org = info['currentUser']['organizations'][0]
  
  return __default_org
