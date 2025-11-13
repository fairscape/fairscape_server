from pydantic import BaseModel, Field
from typing import Literal, Optional, List

class Permissions(BaseModel):
	owner: str
	group: Optional[str] = Field(default=None)
	#read: Optional[List[str]] = Field(default = [])
	#write: Optional[List[str]] =  Field(default = [])
	#delete: Optional[List[str]] = Field(default = [])


class UpdatePermissionsRequest(BaseModel):
	read: Optional[List[str]] = Field(default = [])
	write: Optional[List[str]] =  Field(default = [])
	delete: Optional[List[str]] = Field(default = [])


class UserCreateModel(BaseModel):
	email: str
	firstName: str
	lastName: str
	password: str


class UserWriteModel(UserCreateModel):
	metadataType: Literal['Person'] = Field(alias="@type", default="Person")
	session: Optional[str] = Field(default=None)
	groups: Optional[List[str]] = Field(default=[])
	datasets: Optional[List[str]] = Field(default=[])
	software: Optional[List[str]] = Field(default=[])
	computations: Optional[List[str]] = Field(default=[])
	rocrates: Optional[List[str]] = Field(default=[])

	def getPermissions(self)->Permissions:
		permissionsDict = {
				"owner": self.email,
		}

		permissionsDict['group'] = None

		if self.groups:	
			if len(self.groups)>0:
				permissionsDict['group'] = self.groups[0]

		return Permissions.model_validate(permissionsDict)


def checkPermissions(
	permissionsInstance: Permissions, 
	requestingUser: UserWriteModel
	):

	if permissionsInstance.owner == requestingUser.email:
		return True
	elif permissionsInstance.group:
		if permissionsInstance.group in requestingUser.groups:
			return True
	else:
		return False