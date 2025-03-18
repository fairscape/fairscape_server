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
    ROCrateDistribution,
    ROCrateV1_2
)
from fairscape_mds.rocrate.publish import (
    MintROCrateMetadataRequest
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
minioClient = fairscapeConfig.minio.CreateBotoClient()


@router.post(
    "/rocrate/metadata",
    summary="Upload an ROCrate metadata record",
    status_code=201
    )
def publishMetadata(
    currentUser: Annotated[UserLDAP, Depends(getCurrentUser)],
    crateMetadata: ROCrateV1_2
):
    mintRequest = MintROCrateMetadataRequest(
        rocrateCollection,
        identifierCollection,
        crateMetadata,
        currentUser.cn
    )

    try:
        published = mintRequest.publish()

        return JSONResponse(
            content={"published": published}, 
            status_code=202
            )
    except Exception as e:

        return JSONResponse(
            content={
                "message": "error minting rocrate identifiers",
                "error": str(e)
                }, 
            status_code=500
            )

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
    zippedObjectName = Path(fairscapeConfig.minio.default_bucket_path) / currentUser.cn / 'rocrates' / zipFilename

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

    # create a mongo record of the upload job
    uploadJob = createUploadJob(
        asyncCollection,
        userCN=currentUser.cn,
        transactionFolder=str(transactionUUID), 
        zippedCratePath=str(zippedObjectName)
        )

    # start rocrate processing task 
    AsyncRegisterROCrate.apply_async(args=(
        currentUser.cn,
        str(transactionUUID),
        str(zippedObjectName)
        ))


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

    jobMetadata = getUploadJob(asyncCollection, submissionUUID)

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
            {
                "$or": [
                    {"permissions.group": currentUser.memberOf[0]},
                    {"owner": currentUser.cn}
                ]
            },
            projection={"_id": 0}
        )

    responseJSON = {
        "rocrates": [
        {
            "@id": f"{fairscapeConfig.url}/{crate['metadata']['@graph'][1].get('@id')}",
            "name": crate['metadata']['@graph'][1].get("name"),
            "description": crate['metadata']['@graph'][1].get("description"),
            "keywords": crate['metadata']['@graph'][1].get("keywords"),
            "sourceOrganization": crate['metadata']['@graph'][1].get("sourceOrganization"),
            "contentURL": f"{fairscapeConfig.url}/rocrate/download/{crate['metadata']['@graph'][1].get('@id')}",
            "@graph":crate['metadata']['@graph'][2:]
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
            {"$or": [
        {"@id": rocrateGUID},
        {"@id": f"{rocrateGUID}/"}
    ]}, 
        projection={"_id":0}
        ) 

    if rocrateMetadata is None:
        return JSONResponse(
            status_code=404,
            content={"@id": rocrateGUID, "error": "ROCrate not found"}
        )


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
        
