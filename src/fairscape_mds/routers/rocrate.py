from typing import Union
from fastapi import (
    APIRouter, 
    Depends,
    UploadFile, 
    File, 
)
from fastapi.responses import JSONResponse

from fairscape_mds.config import get_fairscape_config

from fairscape_mds.models.utils import remove_ids
from fairscape_mds.models.rocrate import (
    UploadExtractedCrate,
    UploadZippedCrate,
    DeleteExtractedCrate,
    GetMetadataFromCrate,
    StreamZippedROCrate,
    GetROCrateMetadata,
    PublishROCrateMetadata,
    PublishProvMetadata,
    ROCrate,
    ROCrateDistribution
)

from fairscape_mds.worker import (
    AsyncRegisterROCrate,
    createUploadJob,
    getUploadJob
    )

import logging
import sys

from typing import List, Dict
from uuid import UUID, uuid4
from pathlib import Path

from typing import Annotated
from fairscape_mds.models.user import UserLDAP
from fairscape_mds.auth.oauth import getCurrentUser

router = APIRouter()

# setup clients to backend
fairscapeConfig = get_fairscape_config()

mongoClient = fairscapeConfig.CreateMongoClient()
mongoDB = mongoClient[fairscapeConfig.mongo.db]
rocrateCollection = mongoDB[fairscapeConfig.mongo.rocrate_collection]
identifierCollection = mongoDB[fairscapeConfig.mongo.identifier_collection]
userCollection = mongoDB[fairscapeConfig.mongo.user_collection]
asyncCollection = mongoDB[fairscapeConfig.mongo.async_collection]

minioConfig= fairscapeConfig.minio
minioClient = fairscapeConfig.CreateMinioClient()




@router.post(
        "/rocrate/upload-async",
        summary="Upload a Zipped RO-Crate",
        status_code=202
        )
def uploadAsync(
    currentUser: Annotated[UserLDAP, Depends(getCurrentUser)],
    crate: UploadFile,
):

    # create a uuid for transaction
    transactionUUID = uuid4()
    transactionFolder = str(transactionUUID)

    # get the zipfile's filename
    zipFilename = str(Path(crate.filename).name)

    # set the key for uploading the object to minio
    zippedObjectName = Path(fairscapeConfig.minio.default_bucket_path) / currentUser.cn / 'rocrates' / transactionFolder / zipFilename

    # upload the zipped ROCrate 
    zipped_upload_status= UploadZippedCrate(
        MinioClient=minioClient,
        ZippedObject=crate.file,
        ObjectName= str(zippedObjectName),
        BucketName=fairscapeConfig.minio.rocrate_bucket,
        Filename=zipFilename
    )

    if zipped_upload_status is None:
        return JSONResponse(
            status_code=zipped_upload_status.status_code,
            content={
                "error": zipped_upload_status.message
                }
        )


    # add to the dictionary of tasks
    uploadTask = AsyncRegisterROCrate.apply_async(args=(
        currentUser.cn,
        str(transactionUUID),
        str(zippedObjectName)
        ))

    # create the
    uploadJob = createUploadJob(
        userCN=currentUser.cn,
        transactionFolder=str(transactionUUID), 
        zippedCratePath=str(zippedObjectName)
        )

    uploadMetadata = uploadJob.model_dump()
    uploadMetadata['timeStarted'] = uploadMetadata['timeStarted'].timestamp()

    return JSONResponse(
        status_code=201,
        content=uploadMetadata
        )


@router.get(
        "/rocrate/upload/status/{submissionUUID}",
        summary="Check the Status of an Asynchronous Upload Job"
        ) 
def getROCrateStatus(
    currentUser: Annotated[UserLDAP, Depends(getCurrentUser)],
    submissionUUID: str
    ):

    jobMetadata = getUploadJob(submissionUUID)

    # check authorization to view upload status
    if currentUser.cn != jobMetadata.userCN:
        return JSONResponse(
                status_code = 401,
                content={
                    "submissionUUID": str(submissionUUID), 
                    "error": "User Unauthorized to View Upload Job"
                    }
                )
        

    if jobMetadata is None:
        return JSONResponse(
                status_code = 404,
                content={
                    "submissionUUID": str(submissionUUID), 
                    "error": "rocrate submission not found"
                    }
                )

    else:
        jobResponse = jobMetadata.model_dump()
        jobResponse['timeStarted'] = jobResponse['timeStarted'].timestamp()
        if jobResponse['timeFinished']:
            jobResponse['timeFinished'] = jobResponse['timeFinished'].timestamp()
        
        return JSONResponse(
                status_code=200,
                content=jobResponse
                )


@router.get("/rocrate",
    summary="List all ROCrates",
    response_description="Retrieved list of ROCrates")
def rocrate_list(
    currentUser: Annotated[UserLDAP, Depends(getCurrentUser)],
    ):

    if fairscapeConfig.ldap.adminDN in currentUser.memberOf:
        cursor = rocrateCollection.find(
            {},
            projection={ 
                "_id": 0, 
            }
        )

    else:
        # filter by group ownership
        cursor = rocrateCollection.find(
            {"permissions.group": currentUser.memberOf[0]} , 
            projection={ "_id": 0}
            )

    responseJSON = { 
        "rocrates": [
            {
                "@id": f"{fairscapeConfig.url}/{crate.get('@id')}",
                "name": crate.get("name"),
                "description": crate.get("description"),
                "keywords": crate.get("keywords"),
                "sourceOrganization": crate.get("sourceOrganization"),
                "contentURL": f"{fairscapeConfig.url}/rocrate/download/{crate.get('@id')}",
                "@graph": [
                    {
                        "@id": f"{fairscapeConfig.url}/{crateElem.get('@id')}",
                        "@type": crateElem.get("@type"),
                        "name": crateElem.get("name"),
                        "contentURL": f"{fairscapeConfig.url}/dataset/download/{crateElem.get('@id')}"
                     }
                     for crateElem in crate.get("@graph")
                ]
            } for crate in list(cursor)
        ]
    }
    
    return JSONResponse(
        status_code=200,
        content=responseJSON
    )

def remove_object_id(data):
    if isinstance(data, dict):
        return {k: remove_object_id(v) for k, v in data.items() if k != '_id'}
    elif isinstance(data, list):
        return [remove_object_id(v) for v in data]
    else:
        return data

@router.get("/rocrate/ark:{NAAN}/{postfix}",
    summary="Retrieve metadata about a ROCrate",
    response_description="JSON metadata describing the ROCrate")
def dataset_get(NAAN: str, postfix: str):
    """
    Retrieves a dataset based on a given identifier:

    - **NAAN**: Name Assigning Authority Number which uniquely identifies an organization e.g. 12345
    - **postfix**: a unique string
    """

    rocrateGUID = f"ark:{NAAN}/{postfix}"
    rocrateMetadata = rocrateCollection.find_one(
        {"@id": rocrateGUID}, 
        projection={"_id":0}
        ) 

    if rocrateMetadata is None:
        return JSONResponse(
            status_code=404,
            content={"@id": rocrateGUID, "error": "ROCrate not found"}
        )

    # format json-ld with absolute URIs
    rocrateMetadata['@id'] = f"{fairscapeConfig.url}/{rocrateGUID}"

    # remove permissions from top level metadata
    rocrateMetadata.pop("permissions", None)
    

    # process every crate elem in @graph of ROCrate 
    for crateElem in rocrateMetadata.get("@graph", []):
        crateElem['@id'] = f"{fairscapeConfig.url}/{crateElem['@id']}"
        crateElem.pop("_id", None)
        crateElem.pop("permissions", None)
        
        if 'file' in crateElem.get('contentURL'):
            crateElem["contentURL"] = f"{fairscapeConfig.url}/dataset/download/{crateElem.get('@id')}"


    return JSONResponse(
        status_code=200,
        content=rocrateMetadata
    )

@router.get("/rocrate/download/ark:{NAAN}/{postfix}",
            summary="Download archived form of ROCrate using StreamingResponse",
            response_description="ROCrate downloaded as a zip file")
def archived_rocrate_download(
    currentUser: Annotated[UserLDAP, Depends(getCurrentUser)],
    NAAN: str,
    postfix: str
    ): 
    """
    Download the Zipped ROCrate from MINIO
    """

    rocrateGUID = f"ark:{NAAN}/{postfix}"
    rocrateMetadata = rocrateCollection.find_one(
        {"@id": rocrateGUID}, 
        projection={"_id": 0}
        )
    
    if rocrateMetadata is None:
        return JSONResponse(
            status_code=404,
            content={
                "@id": f"{fairscapeConfig.url}/{rocrateGUID}",
                "error": f"unable to find record for RO-Crate: {rocrateGUID}"
            }
        )

    rocrateGroup = rocrateMetadata.get("permissions", {}).get("group")

    # AuthZ: check if user is allowed to download 
    # if a user is a part of the group that uploaded the ROCrate OR user is an Admin
    if rocrateGroup in currentUser.memberOf or fairscapeConfig.ldap.adminDN in currentUser.memberOf:
        objectPath = rocrateMetadata.get("distribution", {}).get("archivedObjectPath", None)

        # TODO contentURI is external reference
        # redirect

        if objectPath is None:
            return JSONResponse(
                status_code=404,
                content={
                    "@id": f"{fairscapeConfig.url}/{rocrateGUID}",
                    "error": f"No downloadable content found for ROCrate: {rocrateGUID}"
                }
            )

        else:
            return StreamZippedROCrate(
                MinioClient=minioClient,
                BucketName=fairscapeConfig.minio.rocrate_bucket,
                ObjectPath = objectPath
            )

    else:
        # return a 401 error
        return JSONResponse(
            status_code=401,
            content={
            "@id": f"{fairscapeConfig.url}/{rocrateGUID}",
            "error": "Current user is not authorized to download ROcrate"
            }
        )
        
