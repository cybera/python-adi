from .dataset import DatasetAPI
from .transformation import TransformationAPI
from .organization import OrganizationAPI

class Connection:
  def __init__(self, host, api_key):
    self.host    = host    or os.environ.get('ADI_API_HOST')
    self.api_key = api_key or os.environ.get('ADI_API_KEY')

    self.dataset = DatasetAPI(connection=self)
    self.organization = OrganizationAPI(connection=self)
    self.transformation = TransformationAPI(connection=self)