from .common import is_uuid

class APIBase:
  def __init__(self, connection):
    self._connection = connection

class OrganizationAwareAPI(APIBase):
  def _default_organization(self):
    return self._connection.organization.default()

  def _resolve_to_uuid(self, uuid_or_name):
    if not is_uuid(uuid_or_name):
      info = self.meta(uuid_or_name)
      if info:
        return info['uuid']
      else:
        return None
    else:
      return uuid_or_name