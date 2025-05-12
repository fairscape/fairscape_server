from bson import SON
from pydantic import (
    Field,
    constr
)
from typing import Optional, List
from fairscape_mds.models.fairscape_base import *
from datetime import datetime




class Software(FairscapeEVIBaseModel, extra = 'allow'):
    metadataType: str = Field(default="evi:Software", alias="@type")
    url: Optional[str]
    owner: str
    author: str
    citation: Optional[str] = Field(default=None)
    dateCreated: Optional[datetime] = Field(default_factory=datetime.now)
    distribution: List[str] = Field(default=[])
    usedBy: List[str] = Field(default=[])
    sourceOrganization: Optional[str] = Field(default=None)
    includedInDataCatalog: Optional[str] = Field(default=None)


    def read(self, MongoCollection: pymongo.collection.Collection) -> OperationStatus:
        ''' read software into pydantic 
        '''
        return super().read(MongoCollection)

    def update(self, MongoCollection: pymongo.collection.Collection) -> OperationStatus:
        # TODO e.g. when software name is updated, it should be reflected in its owners profile
        return super().update(MongoCollection)

    def delete(self, MongoCollection: pymongo.collection.Collection) -> OperationStatus:
        """Delete the software. Update each user who is an owner of the software.

        Args:
            MongoCollection (pymongo.collection.Collection): _description_

        Returns:
            OperationStatus: _description_
        """

        # check that record exists
        # also, if successfull, will unpack the data we need to build the update operation
        read_status = self.read(MongoCollection)

        if not read_status.success:
            if read_status.status_code == 404:
                return OperationStatus(False, "software not found", 404)
            else:
                return read_status

        # create a bulk write operation
        # to remove a document from a list
        pull_operation = {"$pull": {"software": {"@id": self.id}}}

        # for ever member, remove the software from their list of software
        # bulk_edit = [pymongo.UpdateOne({"@id": member.id}, pull_operation) for member in self.members]

        # operations to modify the owner
        bulk_edit = [
            pymongo.UpdateOne({"@id": self.owner.id}, pull_operation)
        ]

        # operations to delete the software document
        bulk_edit.append(
            pymongo.DeleteOne({"@id": self.id})
        )

        # run the transaction
        try:
            bulk_edit_result = MongoCollection.bulk_write(bulk_edit)
        except pymongo.errors.BulkWriteError as bwe:
            return OperationStatus(False, f"Delete Software Error: {bwe}", 500)

        # check that the document was deleted
        if bulk_edit_result.deleted_count == 1:
            return OperationStatus(True, "", 200)
        else:
            return OperationStatus(False, f"{bulk_edit_result.bulk_api_result}", 500)



class SoftwareCreateModel(BaseModel, extra='allow'):
    guid: str = Field(
        title="guid",
        alias="@id"
    )
    name: str
    description: str
    keywords: List[str]
    url: Optional[str] = Field(default=None)
    author: Optional[str] = Field(default=None)
    citation: Optional[str] = Field(default=None)
    owner: Optional[str]
    includedInDataCatalog: Optional[str] = Field(default=None)
    sourceOrganization: Optional[str] = Field(default=None)
    dateCreated: Optional[datetime] = Field(default_factory=datetime.now)
    usedBy: Optional[List[str]] = Field(default=[])


    def convert(self)-> Software:

        return Software(
                guid=self.guid,
                name=self.name,
                description=self.description,
                keywords=self.keywords,
                url= self.url,
                author=self.author,
                owner=self.owner,
                distribution=[],
                includedInDataCatalog=self.includedInDataCatalog,
                sourceOrganization=self.sourceOrganization,
                dateCreated=self.dateCreated,
                usedBy=self.usedBy,
                published=True
                )



def listSoftware(identifierCollection: pymongo.collection.Collection):
    cursor = identifierCollection.find(
        filter={"@type": "evi:Software"},
        projection={"_id": False}
    )
    return {
        "software": [
            {
                "@id": software.get("@id"), 
                "@type": "evi:Software", 
                "name": software.get("name")
            } for software in cursor
            ]
        }

def getSoftware(
        softwareGUID: str, 
        identifierCollection: pymongo.collection.Collection
        ) -> tuple[Software, OperationStatus] :
    
    softwareMetadata = identifierCollection.find_one(
            {"@id": softwareGUID, "@type": "evi:Software"}, 
            projection={"_id": False}
            )
    if softwareMetadata is None:
        return None, OperationStatus(False, "software not found", 404)
    
    softwareInstance = Software.model_validate(softwareMetadata)
    return softwareInstance, OperationStatus(True, "", 200)


def createSoftware(
        softwareInstance: Software, 
        identifierCollection: pymongo.collection.Collection, 
        userCollection: pymongo.collection.Collection
        )-> OperationStatus:
    ''' create a Software Record in mongo Database
    '''
    # check that software does not already exist
    if identifierCollection.find_one({"@id": softwareInstance.guid}) is not None:
        return OperationStatus(False, "software already exists", 400)

    # check that owner exists
    owner = userCollection.find_one({"@id": softwareInstance.owner})
    if owner is None:
        return OperationStatus(False, "owner does not exist", 404)

    # embeded bson documents to enable Mongo queries
    softwareDict = softwareInstance.model_dump(by_alias=True)

    # update operations for the owner user of the software
    insertResult = identifierCollection.insert_one(softwareDict)

    if insertResult.inserted_id is None:
        return OperationStatus(
                False, 
                "error creating software", 
                500
                )

    userUpdateResult = userCollection.update_one(
            {"@id":  softwareInstance.owner}, 
            {"$push": {"software": softwareInstance.guid}}
            )

    if userUpdateResult.modified_count != 1:
        return OperationStatus(
                False, 
                f"error updating user {softwareInstance.owner}\n" + 
                f"matched_documents: {userUpdateResult.matched_count}\n" + 
                f"modified_count: {userUpdateResult.modified_count}",
                500)

    # TODO update organization

    # TODO update project


    return OperationStatus(True, "", 201)


def deleteSoftware(
        softwareGUID: str, 
        identifierCollection: pymongo.collection.Collection, 
        userCollection: pymongo.collection.Collection
        ) -> tuple[Software, OperationStatus]:
    ''' Delete a Software from the mongo Database
    '''

    # check that software does not already exist
    softwareMetadata = identifierCollection.find_one(
            {"@id": softwareGUID, "@type": "evi:Software"}, 
            projection={"_id": False}
            ) 
    if softwareMetadata is None:
        return None, OperationStatus(False, "software not found", 404)
 
    softwareInstance = Software.model_validate(softwareMetadata)

    softwareUpdateResult = identifierCollection.update_one(
            {"@id": softwareGUID, "@type": "evi:Software"}, 
            {"$set": {"published": False}}
            )

    if softwareUpdateResult.modified_count != 1:
        return softwareInstance, OperationStatus(False, "software not deleted", 500)

    updateUserResult = userCollection.update_one(
            {"@id": softwareInstance.owner}, 
            {"$pull": {"software": softwareGUID}}
            )

    if updateUserResult.modified_count != 1:
        return OperationStatus(False, "user not updated", 500)

    # update organization
    identifierCollection.update_many(
            {
            "software": { 
                "$elemMatch": {"$eq": softwareGUID}
                }
            },
            {
            "$pull": {
                "software": softwareGUID
                }
            }
            )

    # TODO check update success

    # update computations using Software
    computationFilter = {
            "@type": "evi:Computation", 
            "usedSoftware": { 
                "$elemMatch": {"$eq": softwareGUID}
                }
            }

    computationUpdate = {
            "$pull": {
                "usedSoftware": softwareGUID
                }
            }

    computationUpdateResult = identifierCollection.update_many(
            computationFilter,
            computationUpdate
            )

    # TODO update RO-crates using Software

    return softwareInstance, OperationStatus(True, "", 201)
