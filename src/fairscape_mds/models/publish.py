from abc import ABC, abstractmethod
from typing import Dict, Any
from fastapi import HTTPException
import requests
from datetime import datetime
import hashlib

LICENSE_MAP = {
    "CC0 1.0": {
        "name": "CC0 1.0",
        "uri": "http://creativecommons.org/publicdomain/zero/1.0"
    },
    "CC BY 4.0": {
        "name": "CC BY 4.0",
        "uri": "https://creativecommons.org/licenses/by/4.0"
    },
    "CC BY-SA 4.0": {
        "name": "CC BY-SA 4.0",
        "uri": "https://creativecommons.org/licenses/by-sa/4.0"
    },
    "CC BY-NC 4.0": {
        "name": "CC BY-NC 4.0",
        "uri": "https://creativecommons.org/licenses/by-nc/4.0"
    },
    "CC BY-NC-SA 4.0": {
        "name": "CC BY-NC-SA 4.0",
        "uri": "https://creativecommons.org/licenses/by-nc-sa/4.0"
    },
    "CC BY-ND 4.0": {
        "name": "CC BY-ND 4.0",
        "uri": "https://creativecommons.org/licenses/by-nd/4.0"
    },
    "CC BY-NC-ND 4.0": {
        "name": "CC BY-NC-ND 4.0",
        "uri": "https://creativecommons.org/licenses/by-nc-nd/4.0"
    }
}

DEFAULT_LICENSE = "CC BY 4.0"
DEFAULT_DATAVERSE_URL = "https://dataversedev.internal.lib.virginia.edu/"
DEFAULT_DATAVERSE_DB = "libradata"

class PublishingTarget(ABC):
    """Abstract base class for different publishing targets"""
    
    @abstractmethod
    async def create_dataset(self, metadata: Dict, api_token: str) -> Dict:
        """Create a new dataset on the target platform"""
        pass
    
    @abstractmethod
    async def upload_files(self, dataset_id: str, file_data: Any, filename: str, api_token: str) -> Dict:
        """Upload files to an existing dataset"""
        pass
    
    @abstractmethod
    def transform_metadata(self, metadata: Dict) -> Dict:
        """Transform metadata into platform-specific format"""
        pass

class DataversePublisher(PublishingTarget):
    def __init__(self, base_url: str, database: str):
        self.base_url = base_url.rstrip('/')
        self.database = database
    
    def transform_metadata(self, metadata: Dict) -> Dict:
        """Transform metadata into Dataverse format"""
        license_info = LICENSE_MAP.get(metadata.get("license", DEFAULT_LICENSE), LICENSE_MAP[DEFAULT_LICENSE])
        
        return {
            "datasetVersion": {
                "license": license_info,
                "metadataBlocks": {
                    "citation": {
                        "fields": [
                            {
                                "value": metadata.get("name"),
                                "typeClass": "primitive",
                                "multiple": False,
                                "typeName": "title"
                            },
                            {
                                "value": [
                                    {
                                        "authorName": {"value": author.strip(), "typeClass": "primitive", "multiple": False, "typeName": "authorName"},
                                        "authorAffiliation": {"value": metadata.get("authorAffiliation", ""), "typeClass": "primitive", "multiple": False, "typeName": "authorAffiliation"}
                                    } for author in metadata.get("author", "").split(',')
                                ],
                                "typeClass": "compound",
                                "multiple": True,
                                "typeName": "author"
                            },
                            {
                                "value": [
                                    {
                                        "datasetContactName": {"value": metadata.get("contactName"), "typeClass": "primitive", "multiple": False, "typeName": "datasetContactName"},
                                        "datasetContactEmail": {"value": metadata.get("contactEmail"), "typeClass": "primitive", "multiple": False, "typeName": "datasetContactEmail"}
                                    }
                                ],
                                "typeClass": "compound",
                                "multiple": True,
                                "typeName": "datasetContact"
                            },
                            {
                                "value": [
                                    {
                                        "dsDescriptionValue": {"value": metadata.get("description"), "typeClass": "primitive", "multiple": False, "typeName": "dsDescriptionValue"}
                                    }
                                ],
                                "typeClass": "compound",
                                "multiple": True,
                                "typeName": "dsDescription"
                            },
                            {
                                "value": metadata.get("subjects", ["Computer and Information Science"]),
                                "typeClass": "controlledVocabulary",
                                "multiple": True,
                                "typeName": "subject"
                            },
                            {
                                "value": [
                                    {
                                        "keywordValue": {"value": keyword.strip(), "typeClass": "primitive", "multiple": False, "typeName": "keywordValue"}
                                    } for keyword in metadata.get("keywords", "").split(',')
                                ],
                                "typeClass": "compound",
                                "multiple": True,
                                "typeName": "keyword"
                            },
                            {
                                "value": metadata.get("notes", "This dataset is an ROCrate."),
                                "typeClass": "primitive",
                                "multiple": False,
                                "typeName": "notesText"
                            },
                            {
                                "typeName": "datasetPublicationDate",
                                "multiple": False,
                                "typeClass": "primitive",
                                "value": metadata.get("datePublished", datetime.today().strftime("%Y-%m-%d"))
                            }
                        ]
                    }
                }
            }
        }
    
    async def create_dataset(self, metadata: Dict, api_token: str) -> Dict:
        headers = {
            "X-Dataverse-key": api_token,
            "Content-Type": "application/json"
        }
        url = f"{self.base_url}/api/dataverses/{self.database}/datasets"
        
        transformed_metadata = self.transform_metadata(metadata)
        response = requests.post(url, headers=headers, json=transformed_metadata)
        
        if response.status_code != 201:
            raise HTTPException(status_code=response.status_code, detail=response.text)
        
        return {
            "persistent_id": response.json()['data']['persistentId'],
            "platform": "dataverse"
        }
    
    async def upload_files(self, dataset_id: str, file_data: Any, filename: str, api_token: str) -> Dict:
        url = f"{self.base_url}/api/datasets/:persistentId/add?persistentId={dataset_id}"
        headers = {"X-Dataverse-key": api_token}
        
        files = {'file': (filename, file_data, 'application/zip')}

        response = requests.post(url, headers=headers, files=files)
        
        if response.status_code != 200:
            raise HTTPException(status_code=response.status_code, detail=response.text)
            
        return {
            "file_id": response.json()['data']['files'][0]['dataFile']['id'],
            "platform": "dataverse"
        }

class ZenodoPublisher(PublishingTarget):
    def __init__(self, base_url: str = "https://zenodo.org/api"):
        self.base_url = base_url
    
    def transform_metadata(self, metadata: Dict) -> Dict:
        """Transform metadata into Zenodo format"""
        authors = metadata.get("author", [])
        if isinstance(authors, str):
            authors = [author.strip() for author in authors.split(',')]
        
        return {
            "metadata": {
                "title": metadata.get("name"),
                "description": metadata.get("description"),
                "creators": [{"name": author} for author in authors],
                "publication_date": metadata.get("datePublished", datetime.today().strftime("%Y-%m-%d")),
                "keywords": metadata.get("keywords", "").split(',') if isinstance(metadata.get("keywords"), str) else metadata.get("keywords", []),
                "access_right": "open",
                "license": metadata.get("license", "cc-by-4.0")
            }
        }
    
    async def create_dataset(self, metadata: Dict, api_token: str) -> Dict:
        headers = {
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json"
        }
        transformed_metadata = self.transform_metadata(metadata)

        response = requests.post(f"{self.base_url}/deposit/depositions", headers=headers, json=transformed_metadata)
        
        if response.status_code != 201:
            raise HTTPException(status_code=response.status_code, detail=response.text)
            
        return {
            "persistent_id":response.json()['metadata']['prereserve_doi']['doi'],
            "transaction_id": response.json()['id'],
            "platform": "zenodo"
        }
    
    async def upload_files(self, dataset_id: str, file_data: Any, filename: str, api_token: str) -> Dict:
        headers = {"Authorization": f"Bearer {api_token}"}
        url = f"{self.base_url}/deposit/depositions/{dataset_id}/files"
        
        files = {'file': (filename, file_data, 'application/zip')}
        response = requests.post(url, headers=headers, files=files)
        
        if response.status_code != 201:
            raise HTTPException(status_code=response.status_code, detail=response.text)
            
        return {
            "file_id": response.json()['id'],
            "platform": "zenodo"
        }
    
class FigsharePublisher(PublishingTarget):
    def __init__(self, base_url: str = "https://api.figshare.com/v2"):
        self.base_url = base_url

    def transform_metadata(self, metadata: Dict) -> Dict:
        """Transform metadata into Figshare format"""
        authors = metadata.get("author", [])
        if isinstance(authors, str):
            authors = [{"name": author.strip()} for author in authors.split(',')]

        # Figshare license IDs from API
        license_map = {
            "CC BY 4.0": 1,      
            "CC0 1.0": 2,       
            "MIT": 3,            
            "GPL": 4,          
            "GPL 2.0+": 5,      
            "GPL 3.0+": 6,       
            "Apache 2.0": 7      
        }

        # For licenses that don't map directly, default to CC BY 4.0
        input_license = metadata.get("license", "CC BY 4.0")
        if input_license in license_map:
            license_id = license_map[input_license]
        else:
            # Default to CC BY 4.0 for any unmatched license
            license_id = 1

        return {
            "title": metadata.get("name"),
            "description": metadata.get("description"),
            "authors": authors,
            "categories": metadata.get("subjects", [29872]),  # Using Biostats come back to this there's thousands
            "keywords": metadata.get("keywords", "").split(',') if isinstance(metadata.get("keywords"), str) else metadata.get("keywords", []),
            "license": license_id,
            "defined_type": "dataset"
        }

    async def create_dataset(self, metadata: Dict, api_token: str) -> Dict:
        """Create a new dataset on Figshare"""
        headers = {
            "Authorization": f"token {api_token}",
            "Content-Type": "application/json"
        }
        
        # Create new article
        create_url = f"{self.base_url}/account/articles"
        transformed_metadata = self.transform_metadata(metadata)
        response = requests.post(create_url, headers=headers, json=transformed_metadata)
        
        if response.status_code != 201:
            raise HTTPException(status_code=response.status_code, detail=response.text)
        
        article_id = response.json()['location'].split('/')[-1]

        # Reserve DOI
        #reserve_doi_url = f"{self.base_url}/account/articles/{article_id}/reserve_doi"
        #doi_response = requests.post(reserve_doi_url, headers=headers)
        
        # if doi_response.status_code != 201:
        #     raise HTTPException(status_code=doi_response.status_code, detail=doi_response.text)
            
        return {
            "persistent_id": "test",#doi_response.json()['doi'],
            "transaction_id": article_id,
            "platform": "figshare"
        }

    async def upload_files(self, dataset_id: str, file_data: Any, filename: str, api_token: str) -> Dict:
        """Upload files to Figshare dataset"""
        headers = {
            "Authorization": f"token {api_token}",
            "Content-Type": "application/json"
        }

        # Calculate MD5 hash of the file data
        md5_hash = hashlib.md5(file_data).hexdigest()
        file_size = len(file_data)

        # Initiate file upload with required metadata
        init_url = f"{self.base_url}/account/articles/{dataset_id}/files"
        file_metadata = {
            "name": filename,
            "size": file_size,
            "md5": md5_hash
        }
        
        init_response = requests.post(init_url, headers=headers, json=file_metadata)

        if init_response.status_code != 201:
            raise HTTPException(status_code=init_response.status_code, detail=init_response.text)
        
        file_info = init_response.json()
        
        # Upload file parts
        upload_url = file_info['location']
        files = {'file': (filename, file_data, 'application/zip')}
        upload_response = requests.put(upload_url, files=files)
        
        if upload_response.status_code != 200:
            raise HTTPException(status_code=upload_response.status_code, detail=upload_response.text)
        
        return {
            "file_id": file_info['id'],
            "platform": "figshare"
        }
class PublishingService:
    """Service class to manage different publishing targets"""
    
    def __init__(self):
        self._publishers: Dict[str, PublishingTarget] = {}
        
    def register_publisher(self, name: str, publisher: PublishingTarget):
        self._publishers[name] = publisher
        
    def get_publisher(self, platform_url: str) -> tuple[PublishingTarget, str]:
        """Get publisher based on platform URL"""
        if "dataverse" in platform_url.lower():
            return self._publishers["dataverse"], "dataverse"
        elif "zenodo" in platform_url.lower():
            return self._publishers["zenodo"], "zenodo"
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported platform URL: {platform_url}")
