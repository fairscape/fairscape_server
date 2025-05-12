from typing import (
    Optional, 
    Union, 
    Literal,
    List
)
from datetime import datetime
from pydantic import (
    BaseModel,
    Field,
    constr
)
from bson import SON
from fastapi.encoders import jsonable_encoder
import pymongo

from fairscape_mds.models.fairscape_base import (
    FairscapeBaseModel,
    FairscapeEVIBaseModel,
    IdentifierPattern
)
from fairscape_mds.models.dataset import DatasetWriteModel
from fairscape_mds.utilities.operation_status import OperationStatus

class DownloadCreateModel(FairscapeEVIBaseModel, extra='allow'):
    metadataType: Literal['evi:DataDownload'] = Field(alias="@type", default='evi:DataDownload')
    encodingFormat: str
    owner: str = Field(...)
    contentSize: Optional[str] = Field(default=None)
    encodesCreativeWork: str = Field(...)
    sha256: Optional[str] = Field(default=None)
    filename: Optional[str] = Field(default=None)
    uploadDate: Optional[datetime] = Field(default_factory=datetime.now)
    version: Optional[str] = Field(default="0.1.0")
    sourceOrganization: Optional[str] = Field(default=None)
    includedInDataCatalog: Optional[str] = Field(default=None)


class DownloadReadModel(DownloadCreateModel, extra='allow'):
    minioPath: Optional[str] = Field(default=None)
    contentURL: str



def createDownload(
        downloadInstance: DownloadCreateModel, 
        dataObject,
        identifierCollection: pymongo.collection.Collection,
        userCollection: pymongo.collection.Collection, 
        minioClient,
        minioConfig
        ) -> OperationStatus: 

    # check that the data download doesn't already exist
    if identifierCollection.find_one({"@id": downloadInstance.guid}) != None:
        return OperationStatus(
                False, 
                f"dataDownload {downloadInstance.guid} already exists", 
                404)

    # check that owner exists
    if userCollection.find_one({"@id": downloadInstance.owner}) is None:
        return OperationStatus(False, "owner does not exist", 404)

    # Get the creative work
    creativeWork = identifierCollection.find_one({
        "@id": downloadInstance.encodesCreativeWork
        })

    if creativeWork is None:
        return OperationStatus(
                False, 
                f"creative work {downloadInstance.encodesCreativeWork} not found", 
                404
                )

    if downloadInstance.filename is None:
        downloadInstance.filename = dataObject.filename
    
    downloadMetadata = downloadInstance.model_dump(by_alias=True)
    downloadMetadata['contentURL'] = f'{fairscapeConfig.host}/datadownload/{downloadInstance.guid}/download'

    insertDownloadResult = identifierCollection.insert_one(
            downloadMetadata
            )
    
    updateCreativeWorkResult = identifierCollection.update_one(
        {"@id": downloadInstance.encodesCreativeWork},
        {"$push": {"distribution": downloadInstance.guid}}
        )
    
    updateUserResult = userCollection.update_one(
        {"@id": downloadInstance.encodesCreativeWork},
        {"$push": {"downloads": downloadInstance.guid}}
        )
  
    postfix = downloadInstance.guid.split('/', 1)[1] 
    minioPath = f"{postfix}/{dataObject.filename}"

    # upload to minio 
    try:
        upload_operation = minioClient.put_object(
            bucket_name=minioConfig.default_bucket,
            object_name=minioPath,
            data=dataObject.file,
            length=-1,
            part_size=10 * 1024 * 1024,
            #metadata={"identifier": downloadInstance.guid, "name": downloadInstance.name}
            )

        # get the size of the file from the stats
        result_stats = minioClient.stat_object(
            bucket_name=minioConfig.default_bucket,
            object_name=minioPath
        )

    except Exception as e:
        return OperationStatus(False, f"exception uploading: {str(e)}", 500)

    # update the download metadata
    update_download_result = identifierCollection.update_one(
        {"@id": downloadInstance.guid},
        {"$set": {
            "uploadDate": str(result_stats.last_modified),
            "contentSize": str(result_stats.size),
            "minioPath": minioPath,
        }}
        )

    return OperationStatus(True, "", 201)


def getDownloadMetadata(
        downloadGUID: str,
        identifierCollection: pymongo.collection.Collection
        )->tuple[DownloadReadModel, OperationStatus]:
    
    downloadMetadata = identifierCollection.find_one(
            {"@id": downloadGUID},
            projection={"_id": False}
            )

    if downloadMetadata is None:
        return None, OperationStatus(False, f"download {downloadGUID} not found", 404)
    
    downloadInstance = DownloadReadModel.model_validate(downloadMetadata)

    return downloadInstance, OperationStatus(True, "", 200)


def getDownloadMinioPath(
        downloadGUID: str,
        identifierCollection: pymongo.collection.Collection
        ):
    
    downloadMetadata = identifierCollection.find_one(
            {"@id": downloadGUID}
            )

    if downloadMetadata is None:
        return None, OperationStatus(False, f"download {downloadGUID} not found", 404)

    # get minio path
    minioPath = downloadMetadata.get("minioPath")

    if minioPath is None:
        return None, OperationStatus(False, f"download {downloadGUID} has no data stored in fairscape", 404)

    return minioPath, OperationStatus(True, "", 200)


def getDownloadContent(
        minioPath: str,
        minioClient,
        minioBucket
    ):
    # read object from minio
    with minioClient.get_object(minioBucket, minioPath) as minioObject:
        yield from minioObject


def deleteDownload(
        downloadGUID: str,
        identifierCollection: pymongo.collection.Collection,
        userCollection: pymongo.collection.Collection,
        minioClient
        )-> OperationStatus:

    # get download metadata 
    downloadMetadata = identifierCollection.find_one(
            {"@id": downloadGUID}
            )
   


    published = downloadMetadata.get("published")
    minioPath = downloadMetadata.get("minioPath")
    creativeWork = downloadMetadata.get("encodesCreativeWork")
    ownerGUID = downloadMetadata.get("owner")

    if not published and minioPath is None:
        return OperationStatus(True, "", 200)

    if downloadMetadata is None:
        return OperationStatus(False, f"download {downloadGUID} not found", 404)

    # update user 
    updateUserResult = userCollection.update_one(
            {"@id": ownerGUID},
            {"$pull": {"downloads": downloadGUID}}
            )

    # update Dataset specified by encodesCreativeWork
    updateDatasetResult = identifierCollection.update_one(
            {"@id": creativeWork},
            {"$pull": {"distribution": downloadGUID}}
            )


    # remove object from minio
    minioClient.remove_object(
            minioConfig.default_bucket,
            minioPath
            )

    # update downloadMetadata
    updateDownloadResult = identifierCollection.update_one(
            {"@id": downloadGUID},
            {"$set": {
                "minioPath": None,
                "published": False
                }}
            )

    return OperationStatus(True, "", 200)


def listDownloads(
        identifierCollection: pymongo.collection.Collection
        ):
    ''' list all data downloads available 
    '''

    cursor = identifierCollection.find(
        {"@type": "evi:DataDownload"},
        projection={"_id": False}
    )

    dataDownloadList = [ 
        {
            "@id": dataDownload.get("@id"), 
            "@type": "evi:Computation", 
            "name": dataDownload.get("name"),
            "uploadDate": dataDownload.get("uploadDate"),
            "encodesCreativeWork": dataDownload.get("encodesCreativeWork"),
            "contentURL": dataDownload.get("contentURL")
        } for dataDownload in cursor
    ]

    return { "downloads": dataDownloadList}
