from fastapi.responses import StreamingResponse
import json
import pathlib
from pathlib import Path

import io
import os
from io import BytesIO
import zipfile
from zipfile import ZipFile

import minio.api
from minio.deleteobjects import DeleteObject
from datetime import datetime
import pymongo

import sys
import logging

import uuid
import urllib
import re
import hashlib
from pydantic import (
    Field,
    constr,
    BaseModel,
    ValidationError, 
    computed_field
)
from typing import (
    Optional, 
    Union, 
    Dict, 
    List, 
    Literal,
    Generator,
    Tuple
)

from fairscape_mds.models.schema import Schema
from fairscape_mds.models.fairscape_base import (
    FairscapeBaseModel, 
    FairscapeEVIBaseModel, 
    IdentifierValue
)
from fairscape_mds.models.dataset import (
        DatasetDistribution, 
        MinioDistribution, 
        DistributionTypeEnum,
        URLDistribution
        )

from fairscape_mds.utilities.operation_status import OperationStatus
from fairscape_mds.models.user import UserLDAP


# setup logger for minio operations
#rocrateLoggerOutput="/tmp/logs/rocrate.log"
# touch file?
#logging.basicConfig(filename=rocrateLoggerOutput, level=logging.INFO)
rocrate_logger = logging.getLogger("rocrate")

##fh = logging.FileHandler(rocrateLoggerOutput)
#rocrate_logger.addHandler(fh)
#rocrate_logger.info("started rocrate logger")


DATASET_TYPE = "Dataset"
DATASET_CONTAINER_TYPE = "DatasetContainer"
SOFTWARE_TYPE = "Software"
COMPUTATION_TYPE = "Computation"
ROCRATE_TYPE = "ROCrate"



class ROCrateDataset(BaseModel):
    guid: str = Field(alias="@id")
    metadataType: Optional[str] = Field(default="https://w3id.org/EVI#Dataset")
    additionalType: Optional[str] = Field(default=DATASET_TYPE)
    author: Union[str, List[str]]
    datePublished: str = Field(...)
    version: str = Field(default="0.1.0")
    description: str = Field(min_length=10)
    keywords: List[str] = Field(...)
    associatedPublication: Optional[str] = Field(default=None)
    additionalDocumentation: Optional[str] = Field(default=None)
    fileFormat: str = Field(alias="format")
    dataSchema: Optional[IdentifierValue] = Field(alias="evi:Schema", default=None)
    generatedBy: Optional[Union[IdentifierValue, List[IdentifierValue]]] = Field(default=[])
    derivedFrom: Optional[List[IdentifierValue]] = Field(default=[])
    usedByComputation: Optional[List[IdentifierValue]] = Field(default=[])
    contentUrl: Optional[str] = Field(default=None)


class ROCrateSoftware(BaseModel): 
    guid: str = Field(alias="@id")
    metadataType: Optional[str] = Field(default="https://w3id.org/EVI#Software")
    additionalType: Optional[str] = Field(default=SOFTWARE_TYPE)
    author: str = Field(min_length=4)
    dateModified: str
    version: str = Field(default="0.1.0")
    description: str =  Field(min_length=10)
    associatedPublication: Optional[str] = Field(default=None)
    additionalDocumentation: Optional[str] = Field(default=None)
    fileFormat: str = Field(title="fileFormat", alias="format")
    usedByComputation: Optional[List[IdentifierValue]] = Field(default=[])
    contentUrl: Optional[str] = Field(default=None)


class ROCrateComputation(BaseModel):
    guid: str = Field(alias="@id")
    metadataType: Optional[str] = Field(default="https://w3id.org/EVI#Computation")
    additionalType: Optional[str] = Field(default=COMPUTATION_TYPE)
    runBy: str
    dateCreated: str
    associatedPublication: Optional[str] = Field(default=None)
    additionalDocumentation: Optional[str] = Field(default=None)
    command: Optional[Union[List[str], str]] = Field(default=None)
    usedSoftware: Optional[List[IdentifierValue]] = Field(default=[])
    usedDataset: Optional[List[IdentifierValue]] = Field(default=[])
    generated: Optional[List[IdentifierValue]] = Field(default=[])


class ROCrateDistribution(BaseModel):
    extractedROCrateBucket: Optional[str] = Field(default=None)
    archivedROCrateBucket: Optional[str] = Field(default=None)
    extractedObjectPath: Optional[List[str]] = Field(default=[])
    archivedObjectPath: Optional[str] = Field(default=None)


class ROCrate(BaseModel):
    guid: str = Field(alias="@id")
    metadataType: Optional[str] = Field(default="https://schema.org/Dataset", alias="@type")
    additionalType: Optional[str] = Field(default=ROCRATE_TYPE)
    name: str = constr(max_length=100)
    sourceOrganization: Optional[str] = Field(default=None)
    metadataGraph: List[Union[
        ROCrateDataset,
        ROCrateSoftware,
        ROCrateComputation,
    ]] = Field(alias="@graph", 
               # TODO causes TypeError: list is not a valid discriminator
               #discriminator='additionalType'
               )
    contentURL: Optional[str] = Field(
        default=None, 
        description="Value for ROCrate S3 URI of zip location"
        )
    distribution: Optional[ROCrateDistribution] = Field(default=None)


    def entailment(self):
        """ Run entailment on EVI Provenance properties
        """

        computations = list(filter(lambda x: x.additionalType == "Computation", self.metadataGraph))

        def filterCrateByGUID(guid):
            return list(filter(lambda x: x.guid==guid, self.metadataGraph))

        def inverseUsedDataset(used_dataset_guid, computation_guid):
            used_dataset_list = filterCrateByGUID(used_dataset_guid)
            
            # update each dataset as 
            for used_dataset in used_dataset_list:
                used_dataset.usedByComputation.append(computation_guid)

        def inverseUsedSoftware(used_software_guid, computation_guid):
            used_software_list = filterCrateByGUID(used_software_guid) 
            
            for used_software in used_software_list:
                used_software.usedByComputation.append(computation_guid)


        def inverseGenerated(generated_guid, computation_guid):
            generated_list = filterCrateByGUID(generated_guid)

            for generated_element in generated_list:
                generated_element.generatedBy.append(computation_guid)


        for computation_element in computations:

            [ 
                inverseUsedDataset(used_dataset, computation_element.guid) for 
                used_dataset in computation_element.usedDataset
                ]

            [ 
                inverseUsedSoftware(used_software, computation_element.guid) for 
                used_software in computation_element.usedSoftware
                ]

            [ 
                inverseGenerated(generated, computation_element.guid) for 
                generated in computation_element.generated
                ]


    def validateObjectReference(
            self, 
            MinioClient,
            MinioConfig,
            TransactionFolder, 
            CrateName,
            ) -> OperationStatus:

        # List instances of Dataset, Software in the ROCrate metadata
        object_instances_in_metadata = list(filter(
            lambda x: (x.additionalType == "Dataset"
                       or x.additionalType == "Software"),
            self.metadataGraph)
        )

        # List full object paths specified in the ROCrate metadata
        objects_in_metadata = [ 
            Path(metadata_elem.contentUrl).name for metadata_elem in 
            object_instances_in_metadata if metadata_elem.contentUrl is not None
            ]

        rocrate_logger.info(
            "ParsingROCrate message='found files in rocrate metadata'\t" +
            f"transaction={TransactionFolder}\t" +
            f"objects={objects_in_metadata}"
            )

        object_path = f"{TransactionFolder}/{CrateName}"

        try:
            object_instances_in_crate = MinioClient.list_objects(
                bucket_name = MinioConfig.default_bucket, 
                prefix=object_path, 
                recursive=True
                )

            object_paths_in_crate = [
                obj_instance.object_name for obj_instance in object_instances_in_crate
                ]
            objects_in_crate = [
                Path(obj).name for obj in object_paths_in_crate
                ]

            rocrate_logger.info(
                "ParsingROCrate\t" +
                "message='found objects in minio'\t" +
                f"transaction={TransactionFolder}\t" +
                f"objects={objects_in_crate}"
                )

            # Check if metadata objects exist in the crate
            if set(objects_in_metadata).issubset(set(objects_in_crate)):
                # calculate filesize
                # file_size = os.fstat(Object.fileno()).st_size
                # print(file_size)

                rocrate_logger.info(
                    "validateObjectReference\t" +
                    f"transaction_folder={str(TransactionFolder)}\t" +
                    "message='validation successfull'\t" +
                    "success=true"
                )

                # insert the metadata onto the mongo metadata store
                self.distribution = ROCrateDistribution(**{
                    "extractedObjectPath": object_paths_in_crate,
                    "archivedObjectPath": f"{object_path}.zip"
                })

                zip_bucket = MinioConfig.rocrate_bucket
                self.contentURL =  f"s3a://{zip_bucket}/{TransactionFolder}/{CrateName}.zip"

                return OperationStatus(True, "", 200)

            else:
                missing_objects = set(objects_in_metadata) - set(objects_in_crate)

                rocrate_logger.error(
                    "ParsingROCrate\t" +
                    "message='Objects Missing Annotation'\t" +
                    f"transaction={TransactionFolder}\t" +
                    f"objects={missing_objects}"
                    )

                return OperationStatus(
                    False,
                    f"ROCrate ERROR: Missing Objects {str(missing_objects)}",
                    404
                    )

        except Exception as e:
            return OperationStatus(
                False, 
                f"exception validating objects in ROCrate: {str(e)}", 
                500
                )


class ROCrateOrganization(IdentifierValue):
    metadataType: Literal['Organization'] = Field(alias="@type")
    name: str


class ROCrateProject(IdentifierValue):
    metadataType: Literal['Project'] = Field(alias="@type")
    name: str


class ROCrateMetadataFileElem(BaseModel):
    """Metadata Element of an ROCrate cooresponding to the `ro-crate-metadata.json` file itself

    Example

        ```
        {
            "@id": "ro-crate-metadata.json",
            "@type": "CreativeWork",
            "conformsTo": {
                "@id": "https://w3id.org/ro/crate/1.2-DRAFT"
            },
            "about": {
                "@id": "https://fairscape.net/ark:59852/rocrate-2.cm4ai_chromatin_mda-mb-468_untreated_apmsembed_initialrun0.1alpha"
            }
        }
        ```
    """
    guid: str = Field(alias="@id")
    metadataType: Literal["CreativeWork"] = Field(alias="@type")
    conformsTo: IdentifierValue
    about: IdentifierValue


class ROCrateMetadataElem(BaseModel):
    """Metadata Element of ROCrate that represents the crate as a whole

    Example
        ```
        {
            '@id': 'https://fairscape.net/ark:59852/rocrate-2.cm4ai_chromatin_mda-mb-468_untreated_imageembedfold1_initialrun0.1alpha',
            '@type': ['Dataset', 'https://w3id.org/EVI#ROCrate'],
            'name': 'Initial integration run',
            'description': 'Ideker Lab CM4AI 0.1 alpha MDA-MB-468 untreated chromatin Initial integration run IF Image Embedding IF microscopy images embedding fold1',
            'keywords': ['Ideker Lab', 'fold1'],
            'isPartOf': [
                {'@id': 'ark:/Ideker_Lab'}, 
                {'@id': 'ark:/Ideker_Lab/CM4AI'}
                ],
            'version': '0.5alpha',
            'license': 'https://creativecommons.org/licenses/by-nc-sa/4.0/deed.en',
            'associatedPublication': 'Clark T, Schaffer L, Obernier K, Al Manir S, Churas CP, Dailamy A, Doctor Y, Forget A, Hansen JN, Hu M, Lenkiewicz J, Levinson MA, Marquez C, Mohan J, Nourreddine S, Niestroy J, Pratt D, Qian G, Thaker S, Belisle-Pipon J-C, Brandt C, Chen J, Ding Y, Fodeh S, Krogan N, Lundberg E, Mali P, Payne-Foster P, Ratcliffe S, Ravitsky V, Sali A, Schulz W, Ideker T. Cell Maps for Artificial Intelligence: AI-Ready Maps of Human Cell Architecture from Disease-Relevant Cell Lines. BioRXiv 2024.',
            'author': ['Test']
            'conditionsOfAccess': 'This dataset was created by investigators and staff of the Cell Maps for Artificial Intelligence project (CM4AI - https://cm4ai.org), a Data Generation Project of the NIH Bridge2AI program, and is copyright (c) 2024 by The Regents of the University of California and, for cellular imaging data, by The Board of Trustees of the Leland Stanford Junior University. It is licensed for reuse under the Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International (CC-BY-NC-SA 4.0) license, whose terms are summarized here: https://creativecommons.org/licenses/by-nc-sa/4.0/deed.en.  Proper attribution credit as required by the license includes citation of the copyright holders and of the attribution parties, which includes citation of the following article: Clark T, Schaffer L, Obernier K, Al Manir S, Churas CP, Dailamy A, Doctor Y, Forget A, Hansen JN, Hu M, Lenkiewicz J, Levinson MA, Marquez C, Mohan J, Nourreddine S, Niestroy J, Pratt D, Qian G, Thaker S, Belisle-Pipon J-C, Brandt C, Chen J, Ding Y, Fodeh S, Krogan N, Lundberg E, Mali P, Payne-Foster P, Ratcliffe S, Ravitsky V, Sali A, Schulz W, Ideker T. Cell Maps for Artificial Intelligence: AI-Ready Maps of Human Cell Architecture from Disease-Relevant Cell Lines. BioRXiv 2024.”',
            'copyrightNotice': 'Copyright (c) 2024 by The Regents of the University of California',
            'hasPart': [
                {'@id': 'https://fairscape.net/ark:59852/software-cellmaps_image_embedding-N2ux5jg'},
                {'@id': 'https://fairscape.net/ark:59852/dataset-cellmaps_image_embedding-output-file-N2ux5jg'},
                {'@id': 'https://fairscape.net/ark:59852/dataset-Densenet-model-file-N2ux5jg'},
                {'@id': 'https://fairscape.net/ark:59852/computation-IF-Image-Embedding-N2ux5jg'}
            ]
        }
        ```
    """ 
    guid: str = Field(alias="@id")
    metadataType: List[str] = Field(alias="@type")
    name: str
    keywords: List[str]
    isPartOf: List[IdentifierValue]
    version: str
    dataLicense: str = Field(alias="license")
    associatedPublication: str
    author: Union[str, List[str]]
    conditionsOfAccess: str
    copyrightNotice: str
    hasPart: List[IdentifierValue]
    

class ROCrateV1_2(BaseModel):
    context: Optional[Dict] = Field(alias="@context")
    metadataGraph: List[Union[
        ROCrateDataset,
        ROCrateSoftware,
        ROCrateComputation,
        ROCrateMetadataElem,
        ROCrateMetadataFileElem,
        ROCrateProject,
        ROCrateOrganization,
        Schema
    ]] = Field(alias="@graph")


    def cleanIdentifiers(self):
        """ Clean metadata guid property from full urls to ark:{NAAN}/{postfix} 
        """

        def cleanGUID(metadata):
            """ Clean metadata guid property from full urls to ark:{NAAN}/{postfix} 
            """
            if "http" in metadata.guid:
                metadata.guid = urllib.parse.urlparse(metadata.guid).path.lstrip('/')
 
        #clean ROCrate metadata identifier
        rocrateMetadata = self.getCrateMetadata()
        cleanGUID(rocrateMetadata)
        
        # clean identifiers and evi properties
        for elem in self.getEVIElements():
            cleanGUID(elem)

            if isinstance(elem, ROCrateDataset):
                # usedByComputation
                for usedByComputation in elem.usedByComputation:
                    cleanGUID(usedByComputation)
                
                # generatedBy
                for generatedBy in elem.generatedBy:
                    cleanGUID(generatedBy)

            if isinstance(elem, ROCrateSoftware):
                for usedByElem in elem.usedByComputation:
                    cleanGUID(usedByElem)

            if isinstance(elem, ROCrateComputation):
                #elem.usedDataset
                for usedDataset in elem.usedDataset:
                    cleanGUID(usedDataset)
                #elem.generated
                for generated in elem.generated:
                    cleanGUID(generated)
                #elem.usedSoftware
                for usedSoftware in elem.usedSoftware:
                    cleanGUID(usedSoftware)


    def getCrateMetadata(self)-> ROCrateMetadataElem:
        """ Filter the Metadata Graph for the Metadata Element Describing the Toplevel ROCrate

        :param self
        :return: The RO Crate Metadata Elem describing the toplevel ROCrate
        :rtype fairscape_mds.models.rocrate.ROCrateMetadataElem
        """
        filterResults = list(filter(
            lambda x: isinstance(x, ROCrateMetadataElem),
            self.metadataGraph
        ))

        # TODO support for nested crates 
        # must find the ROCrateMetadataElem with '@id' == 'ro-crate-metadata.json'
        if len(filterResults) != 1:
            # TODO more detailed exception
            raise Exception
        else:
            return filterResults[0]

    def getSchemas(self) -> List[Schema]:
        # TODO filter schemas
        filterResults = list(filter(
            lambda x: isinstance(x, Schema), 
            self.metadataGraph
        ))

        return filterResults

    def getDatasets(self) -> List[ROCrateDataset]:
        """ Filter the Metadata Graph for Dataset Elements

        :param self
        :return: All dataset metadata records within the ROCrate
        :rtype List[fairscape_mds.models.rocrate.ROCrateDataset]
        """
        filterResults = list(filter(
            lambda x: isinstance(x, ROCrateDataset), 
            self.metadataGraph
        ))

        return filterResults


    def getSoftware(self) -> List[ROCrateSoftware]:
        """ Filter the Metadata Graph for Software Elements

        :param self
        :return: All Software metadata records within the ROCrate
        :rtype List[fairscape_mds.models.rocrate.ROCrateSoftware]
        """
        filterResults = list(filter(
            lambda x: isinstance(x, ROCrateSoftware), 
            self.metadataGraph
        ))

        return filterResults


    def getComputations(self) -> List[ROCrateComputation]:
        """ Filter the Metadata Graph for Computation Elements

        :param self
        :return: All Computation metadata records within the ROCrate
        :rtype List[fairscape_mds.models.rocrate.ROCrateComputation]
        """
        filterResults = list(filter(
            lambda x: isinstance(x, ROCrateComputation), 
            self.metadataGraph
        ))

        return filterResults

    def getEVIElements(self) -> List[Union[ROCrateComputation, ROCrateDataset, ROCrateSoftware, Schema]]:
        return self.getDatasets() + self.getSoftware() + self.getComputations() + self.getSchemas()


def UploadZippedCrate(
        MinioClient: minio.api.Minio, 
        BucketName: str, 
        ObjectName: str,
        ZippedObject, 
        Filename: str,
        ) -> OperationStatus:
    """ Upload A Zipped ROCrate
    """
    
    upload_result = MinioClient.put_object(
        bucket_name= BucketName, 
        object_name= ObjectName,
        data= ZippedObject, 
        length= -1,
        part_size= 5 * 1024 * 1024 ,
        content_type= "application/zip"
        )                

    # log upload of zipped rocrate
    rocrate_logger.info(
        "UploadZippedCrate\t" +
        "message='Uploaded Zipped Crate Minio'\t" +
        f"object_name='{upload_result.object_name}\t' " +
        f"object_etag='{upload_result.etag}'"
        )

    return OperationStatus(True, "", 200)

def ExtractCrate(
        fairscapeConfig,
        transactionFolder: str,
        userCN: str,
        objectPath: str
    ) -> dict:
    """
    Extract the ro-crate-metadata.json file from a zipped ROCrate

    :param minio.Minio minioClient: Active minio client
    :param str bucketName: Name of the bucket to make the request against
    :param str bucketRootPath: Name of the bucket root path
    :param str userCN: Uploading User's LDAP CN
    :param file zippedObject: File like object for reading the zipped ROCrate

    :returns: Metadata Extracted from Crate
    :rtype: dict
    """
    
    # connect to ldap and get user
    ldapConnection = fairscapeConfig.ldap.connectAdmin()
    currentUser = getUserByCN(ldapConnection, userCN)
    ldapConnection.unbind()


    minioClient = fairscapeConfig.minio.CreateClient()
    mongoClient = fairscapeConfig.mongo.CreateClient()

    # crate zip name  
    tmpZipFilepath = jobDir / filePath.name
    with tmpZipFilepath.open("wb") as tmpZipfile:
        try:
            tmpZipfile.write(zippedContents)

        except:
            rocrate_logger.error(f"Transaction: {transactionFolder}\tError: failed to write zipped contents to tmp file")

            updateUploadJob(
                transactionFolder,
                {
                    "completed": True,
                    "success": False,
                    "error": "error copying rocrate to tmp file",
                    "status": "Failed"
                }
            )

    tmpExtractFolder = jobDir / 'extract'
    tmpExtractFolder.mdkir(exists_ok=True)

    # read in zipped crate
    with tmpZipFilepath.open('rb') as zippedCrateFileObj:
        crateZipFile = ZipFile(zippedCrateFileObj)
        # extract all files in temporary directory
        crateZipFile.extractall(path=tmpExtractFolder)
        metadataSearch = list(pathlib.Path(extractTempDir).glob("*ro-crate-metadata.json"))


    with tempfile.TemporaryDirectory() as tempDir:
        # create a directory for extracted files
        with tempfile.TemporaryDirectory(prefix=tempDir) as extractTempDir:




            # get the json ld
            metadataSearch = list(pathlib.Path(extractTempDir).glob("*ro-crate-metadata.json"))
            if len(metadataSearch) != 1:
                raise Exception("ro-crate-metadata.json not found in crate")	

            crateMetadataPath = metadataSearch[0]

            with crateMetadataPath.open("r") as crateMetadataFileObj:
                crateMetadata = json.load(crateMetadataFileObj)

            # TODO modify metadata
            crateMetadata = ProcessMetadata(crateMetadata)

            # TODO validate ROCrate

            # TODO reassign identifiers if there is conflict
            # format identifiers for storage

            # TODO get the name of the zip in rocrate
            # zippedFilepath = Path()

            # overwrite extracted rocrate metadata 
            with crateMetadataPath.open("w") as crateMetadataFileObj:
                json.dump(crateMetadata, crateMetadataFileObj, indent=2)

            # compress directory to new zipfile
            crateFolder = crateMetadataPath.parent

            pathlib.Path(crateTemp.name).unlink()

            with ZipFile(crateTemp.name, "w") as overwriteZip:
                addFiles = list(pathlib.Path(extractTempDir).rglob("*"))
                for fileElem in addFiles:
                    if fileElem.is_file():
                        keyName = fileElem.relative_to(crateFolder.parent)
                        print(fileElem)
                        overwriteZip.write(filename=fileElem, arcname=keyName)


            archiveName = crateFolder.name + '.zip' 
            archiveUploadFilepath = Path(bucketRootPath) / userCN / 'rocrates' / archiveName
            print(f"UPLOADING ARCHIVE FILEPATH AT: {archiveUploadFilepath}")


            minioClient.fput_object(
                bucket_name=bucketName,
                object_name=str(archiveUploadFilepath),
                file_path = crateTemp.name
            )




    
    # TODO remove, usefull for debugging transactions
    # upload the ro-crate-metadata.json to the rocrates 
    uploadFilepath = Path(bucketRootPath) / currentUser.cn / 'rocrates' / transactionFolder / 'ro-crate-metadata.json'
 
    uploadResult = minioClient.put_object(
        bucket_name= bucketName, 
        object_name=str(uploadFilepath), 
        data=io.BytesIO(fileContents), 
        length=len(fileContents)
        )
    
    rocrate_logger.info(
        "UploadExtractedCrate\t" +
        f"transaction={transactionFolder}\t" +
        "message='Overwriting Metadata' " +
        f"success='{uploadResult.success}'"                
        )

    # TODO move extraction inside of temp file context
    # read the zip and filter all datasets
    with zipfile.ZipFile(io.BytesIO(zipContents), "r") as crateZip:

        # filter the rocrate for all datasets with files to extract
        crateDatasets = filter(
            lambda crateElem: crateElem.get("@type") == "EVI:Dataset" and  crateElem.get("contentUrl") is not None,
            roCrateMetadata['@graph']
            )

        for crateDataset in crateDatasets:
            contentURL = crateDataset.get("contentUrl") 

            # file to read from within the zipfile 
            sourcePath = Path(contentURL.strip('file:///'))
           
            # get the path of the file relative to inside the crate 
            # i.e. at the same level of the ro-crate-metadata.json file
            fileWithinCrate = sourcePath.relative_to(crateParentPath)

            # create the object_name for the upload in minio
            uploadPath = Path(bucketRootPath) / currentUser.cn / 'datasets' / fileWithinCrate

            # upload the extracted dataset
            datasetContents = crateZip.read(str(sourcePath))

            uploadResult = minioClient.put_object(
                    bucket_name=bucketName,
                    object_name=str(uploadPath),
                    data=io.BytesIO(datasetContents),
                    length=len(datasetContents),
                    metadata={
                        "guid": crateDataset.get("@id"),
                        "owner": currentUser.cn
                        }
                    )

            rocrate_logger.info(
                "UploadExtractedCrate\t" +
                f"transaction={transactionFolder}\t" +
                "message='Uploaded File to minio' " +
                f"object_name='{uploadResult.object_name}' " +
                f"object_etag='{uploadResult.etag}'"
                )

            # set the distribution on the metadata
            datasetDistribution = DatasetDistribution(
                    distributionType=DistributionTypeEnum.MINIO,
                    location=MinioDistribution(path=str(uploadPath)),
                    )
            
            # preserve metadata
            crateDataset['distribution'] = datasetDistribution.model_dump()


    return roCrateMetadata


def UploadExtractedCrate(
        MinioClient, 
        BucketName: str, 
        BucketRootPath: str | None,
        TransactionFolder: str,
        userCN: str,
        ZippedObject, 
        ) -> Tuple[OperationStatus, List[str]]:
    """
    Accepts zipped ROCrate, unzip and upload onto MinIO.

    :param minio.Minio MinioClient: MinIO client
    :param str ROCrateBucketName: Name of S3 Bucket to upload zip archive of ROCrate,
    :param str BucketRootPath: Root of the ROCrate Path to Upload To
    :param str ZippedObject: zipped ROCrate file like object
    :param str TransactionFolder: UUID created for this upload request
    :param str userCN: User ID for 

    Returns:
        OperationStatus: Message
    """

    extractedPaths = []

    zip_contents = ZippedObject.read()
            
    with zipfile.ZipFile(io.BytesIO(zip_contents), "r") as crateZip:
        
        crateInfoList = crateZip.infolist()

        # extract the ro-crate-metadata.json from the zipfile
        metadataInfo = filter(lambda info: 'ro-crate-metadata.json' in info.filename, crateInfoList)

        # extract and upload the content
        source_filepath = Path(metadataInfo.filename)
        upload_filepath = Path(BucketRootPath) / userCN / 'rocrates' / TransactionFolder / 'ro-crate-metadata.json'

        # skip first entry on the infolist which is for the folder itself
        # minio will error if there exist an object which is both a 
        # directory containing objects and an object itself
        for file_info in zip_file.infolist()[1::]:
            file_contents = zip_file.read(file_info.filename)

            # TODO the source filepath will not start at the root of the rocrate
            # but rather the full passed filename
            source_filepath = Path(file_info.filename)

            # TODO file_info.filename can be 
            # Extracted/1.cm4ai_chromatin_mda-mb-468_untreated_imageloader_initialrun0.1alpha/ro-crate-metadata.json
            # this causes the GetROCrateMetadata function to fail later on becaues it looks only in
            # 1.cm4ai_chromatin_mda-mb-468_untreated_imageloader_initialrun0.1alpha/ 
            if BucketRootPath:
                upload_filepath = Path(BucketRootPath) / userCN / 'rocrates' / TransactionFolder / source_filepath
            else: 
                upload_filepath = Path(userCN) / 'rocrates' / TransactionFolder / source_filepath

            upload_result = MinioClient.put_object(
                bucket_name= BucketName, 
                object_name=str(upload_filepath), 
                data=io.BytesIO(file_contents), 
                length=len(file_contents)
                )
                

            extractedPaths.append(str(upload_filepath))

            rocrate_logger.info(
                "UploadExtractedCrate\t" +
                f"transaction={TransactionFolder}\t" +
                "message='Uploaded File to minio' " +
                f"object_name='{upload_result.object_name}' " +
                f"object_etag='{upload_result.etag}'"
                )

    return (OperationStatus(True, "", 200), extractedPaths)


def DeleteExtractedCrate(
    minioClient, 
    bucketName: str, 
    transactionFolder: str,
    cratePath: str
    ) -> OperationStatus:
    """
    Delete an Extracted ROCrate
    :param minio.Minio minioClient: Active minio client
    :param str bucketName: Name of the bucket to preform the operation on
    :param str transactionFolder: UUID for the transaction folder
    :param str cratePath: Path of the Crate

    :returns: A status for the operation
    :rtype: fairscape_mds.utilities.OperationStatus
    :raises fastapi.HTTPException:  
    """


    try:
        # remove all listed files
        minio_listed_objects = minioClient.list_objects(
            bucket_name = bucketName, 
            prefix=cratePath, 
            recursive=True
            )

        object_names = [
            obj_instance.object_name for obj_instance in minio_listed_objects
            ]

        delete_list = [DeleteObject(obj) for obj in object_names]
        
        delete_errors = minioClient.remove_objects(
                bucket_name=BucketName, 
                delete_object_list=delete_list
        )

        # if errors occur
        if len(delete_errors) != 0:

            for error in delete_errors:
                rocrate_logger.error(
                    "DeleteExtractedCrate\t"+
                    f"transaction_folder={transactionFolder}\t" +
                    f"bucket_name={bucketName}\t" +
                    f"object_names={object}\t" +
                    f"error={str(error)}"
                )

            return OperationStatus(False, f"ERROR Deleting ROCrate: {delete_errors}", 400)
        
        
        rocrate_logger.info(
            "DeleteExtractedCrate\t" +
            f"transaction_folder={transactionFolder}\t" +
            f"bucket_name={bucketName}\t" +
            f"objects={object_names}\t"
        )

    except Exception as e:
        return OperationStatus(False, f"Exception removing ROCrate: {str(e)}", 404)

    return OperationStatus(True, "", 200)


def GetMetadataFromCrate(
        MinioClient, 
        BucketName, 
        BucketRootPath: str | None,
        TransactionFolder, 
        CratePath, 
        Distribution
    ):
    """Extract metadata from the unzipped ROCrate onto MinIO
    
    Args:
        MinioClient (Any): MinIO client
        BucketName (str): name for bucket to search for the crate metadata
        BucketRootPath(str): folder for bucket root path
        TransactionFolder (str): UUID for this transaction 
        CratePath (str): name of expanded crate path
        Distribution (ROCrateDistribution): Distribution information for use within Fairscape

    Returns:
        ro_crate_json (dict): contents of the ro-crate-metadata.json file as a dictionary
    """
    if BucketRootPath:
        object_path = f"{BucketRootPath}/{TransactionFolder}/{CratePath}/ro-crate-metadata.json"
    else:
        object_path = f"{TransactionFolder}/{CratePath}/ro-crate-metadata.json"

    rocrate_logger.debug(
        "GetMetadataFromCrate\t" +
        f"transaction_folder={TransactionFolder}" +
        f"object_path={object_path}" 
    )

    try:
        ro_crate_response = MinioClient.get_object(
            bucket_name= BucketName, 
            object_name=object_path, 
            )

        # read all metadata as json
        ro_crate_json = ro_crate_response.read()

        # parse file contents into dictionary
        try:
            #TODO Come back and load into ROCrate Class
            ro_crate_dict = json.loads(ro_crate_json)
            ro_crate_dict['distribution'] = Distribution.dict(by_alias=True)
            ro_crate_dict['additionalType'] = ROCRATE_TYPE
            return ro_crate_dict
        except Exception as json_exception:
            rocrate_logger.debug(
                "GetMetadataFromCrate\t" +
                f"transaction_folder={TransactionFolder} " + 
                f"object_path={object_path} " +
                f"exception={str(json_exception)} "
            )
            return None

        # parse dictionary into ROCrate pydantic model
        #try:
        #    crate = ROCrate(**ro_crate_dict)
        #    return crate
        #except ValidationError:

            # TODO try to parse gracefully
            # additionalType generation
        #    return None

    except Exception as e:
        raise Exception(f"ROCRATE ERROR: ro-crate-metadata.json not found exception={str(e)}")


def zip_extracted_rocrate(bucket_name: str, object_loc_in_bucket, minio_client):

    zip_data = io.BytesIO()

    try:
        with ZipFile(zip_data, 'w') as zip_file:
            for obj_path in object_loc_in_bucket:
                file_path = Path(obj_path).name
                obj_data = minio_client.get_object(bucket_name=bucket_name, object_name=obj_path)
                zip_file.writestr(file_path, obj_data.read())

        headers = {
            "Content-Type": "application/zip",
            "Content-Disposition": "attachment;filename=downloaded-rocrate.zip"
        }

        return StreamingResponse(get_data_from_stream(zip_data), headers=headers, media_type="application/zip")

    except Exception as e:
        raise Exception("Unable to zip objects: ", e)


def StreamZippedROCrate(MinioClient, BucketName: str, ObjectPath: str):

    headers = {
            "Content-Type": "application/zip",
            "Content-Disposition": "attachment;filename=downloaded-rocrate.zip"
    }

    file_stream = MinioClient.get_object(
        bucket_name=BucketName, 
        object_name=ObjectPath
        ).read()

    return StreamingResponse(
        generator_iterfile(file_stream), 
        headers=headers, 
        media_type="application/zip"
        )


# generator function to iterate over that file-like object
def generator_iterfile(file_stream) -> Generator:
    yield file_stream

def get_data_from_file(file_path: str) -> Generator:
    with open(file=file_path, mode="rb") as file_like:
        yield file_like.read()


def get_data_from_stream(file_data) -> Generator:
    yield file_data.getvalue()



def GetROCrateMetadata(rocrate_collection: pymongo.collection, rocrate_id):
    # ignore _id in mongo documents
    query_projection = {'_id': False}
    # find rocrate metadata by the unique @id
    query = rocrate_collection.find_one(
        {'@id': rocrate_id},
        projection=query_projection
        )
 
    if query:
        try:
            parsed_crate = ROCrate.construct(**query)
            return parsed_crate
        except Exception as e:
            raise Exception(message=f"ROCRATE Metadata not valid: {str(e)}")
    else:
        return None    


class ROCrateException(Exception):
    """ Exception class for all ROCrate Exceptions"""

    def __init__(self, message, errors):
        super().__init__(message) 
        self.errors = errors

    def __str__(self):
        return self.message


class ROCrateMetadataExistsException(ROCrateException):
    """ Raised when metadata already exists in mongoDB """
    pass


def PublishMetadata(
    currentUser: UserLDAP,
    crateJSON,
    transactionFolder: str,
    rocrateCollection: pymongo.collection.Collection,
    identifierCollection: pymongo.collection.Collection
    ) -> List[str] | None:
    """
    Publish ROCrate Metadata into Mongo Database
    """
    
    # Check if @id already exsists
    rocrateFound = rocrateCollection.find_one(
            {"@id": crateJSON['@id']}
            )

    if rocrateFound:
        raise ROCrateMetadataExistsException(
            f"ROCrate with @id == {crateJSON['@id']} found", None)
    

    # set default permissions for uploaded crate
    crateJSON['permissions'] = {
            "owner": currentUser.dn,
            "group": currentUser.memberOf[0]
            }

    # set default permissions for all datasets
    for crateElem in crateJSON['@graph']:
        # set permissions on all rocrate identifiers
        crateElem['permissions'] = {
            "owner": currentUser.dn,
            "group": currentUser.memberOf[0]
            }

    publishProvResult = PublishProvMetadata(
        currentUser = currentUser,
        rocrateJSON = crateJSON,
        transactionFolder = transactionFolder,
        identifierCollection = identifierCollection
        )
    
    publishCrateResult = PublishROCrateMetadata(
        crateJSON,
        rocrateCollection
        )

    if publishCrateResult is None or publishProvResult is None:
        # TODO log errors
        return None
    else:
        return publishProvResult


def PublishROCrateMetadata(
        rocrateJSON,
        rocrateCollection: pymongo.collection.Collection
    ) -> bool:  
    """ 
    Insert ROCrate metadata into mongo rocrate collection

    :param fairscape.models.user.UserLDAP currentUser: User metadata from the uploader
    :param dict rocrateJSON: ROCrate metadata to instantiate
    :param pymongo.collection.Collection rocrateCollection: Mongo Collection for storing ROCrate metadata
    """ 

    insertResult = rocrateCollection.insert_one(rocrateJSON)
    if insertResult.inserted_id is None:
        return False
    else:
        return True


def PublishProvMetadata(
        currentUser: UserLDAP,
        rocrateJSON: dict,
        transactionFolder: str,
        identifierCollection: pymongo.collection.Collection
        ) -> List[str] | None:
    """ 
    Insert ROCrate metadata and metadata for all identifiers into the identifier collection

    :param fairscape_mds.models.user.UserLDAP currentUser: Metadata about user submitting the ROCrate
    :param dict rocrateJSON: Metadata for the ROCrate
    :param pymongo.collection.Collection identifierCollection: Collection to submit identifier metadata
    """

    # for every element in the rocrate model dump json
    insertMetadata = [ elem for elem in rocrateJSON.get("@graph", [])]
    # insert rocrate json into identifier collection
    insertMetadata.append(rocrateJSON)

    insertedIdentifiers = [ elem.get("@id") for elem in insertMetadata]

    # insert all identifiers into the identifier collection
    insertResult = identifierCollection.insert_many(insertMetadata)

    if len(insertResult.inserted_ids) != len(insertMetadata):
        return None
    else:
        return insertedIdentifiers
