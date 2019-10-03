import os

from .common import gql_query
from .dataset import DatasetAPI
from .transformation import TransformationAPI
from .organization import OrganizationAPI

class Connection:
  def __init__(self, host=None, api_key=None):
    self.host    = host    or os.environ.get('ADI_API_HOST')
    self.api_key = api_key or os.environ.get('ADI_API_KEY')

    self.dataset = DatasetAPI(connection=self)
    self.organization = OrganizationAPI(connection=self)
    self.transformation = TransformationAPI(connection=self)
  
  def query(self, query, variables=dict(), file=None):
    gql_query(query, variables, file, connection=self)