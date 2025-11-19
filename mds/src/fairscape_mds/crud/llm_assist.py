import os
import tempfile
import json
import re
import io
import uuid
import datetime
import requests
import yaml
from typing import List, Dict, Any
from werkzeug.utils import secure_filename
import google.generativeai as genai
import PyPDF2
from fastapi import UploadFile

from fairscape_mds.crud.fairscape_request import FairscapeRequest
from fairscape_mds.crud.fairscape_response import FairscapeResponse
from fairscape_mds.crud.dataset import FairscapeDatasetRequest
from fairscape_mds.models.user import UserWriteModel
from fairscape_mds.models.llm_assist import LLMAssistTask, D4DFromIssueRequest
from fairscape_mds.models.identifier import StoredIdentifier, MetadataTypeEnum, PublicationStatusEnum
from fairscape_models.dataset import Dataset
from fairscape_models.computation import Computation
from fairscape_models.conversion import TargetToROCrateConverter
from fairscape_models.conversion.mapping.d4d_to_rocrate import (
    DATASET_COLLECTION_TO_RELEASE_MAPPING,
    DATASET_TO_SUBCRATE_MAPPING
)

ALLOWED_EXTENSIONS = {'pdf'}

SYSTEM_PROMPT = """### ROLE

You are an expert research assistant specializing in creating FAIR (Findable, Accessible, Interoperable, and Reusable) data packages. Your primary skill is analyzing and synthesizing information from multiple scientific documents to generate a complete and valid `ro-crate-metadata.json` file that conforms to the RO-Crate v1.1 and Croissant RAI v1.0 specifications.

### TASK

Your task is to analyze all provided scientific texts and generate a single, consolidated `ro-crate-metadata.json` file. You must strictly adhere to the full RO-Crate JSON structure provided in the template below, including the `@context` and the two required elements within the `@graph` array. The final output MUST be a single, valid JSON object and nothing else. Do not include any explanatory text, greetings, or markdown formatting before or after the JSON block.

### INPUT FORMAT

You will be given one or more documents separated by a clear marker:
`--- DOCUMENT SEPARATOR ---`
You must read and consider all documents before generating the final JSON output.

### CORE INSTRUCTIONS FOR MULTI-DOCUMENT ANALYSIS

1.  **Synthesize and Consolidate:** Do not just copy-paste from one source. For descriptive fields, synthesize a comprehensive summary that combines unique and relevant points from all provided documents.
2.  **Merge Lists:** For fields that are lists (e.g., `keywords`), aggregate the values from all documents and then de-duplicate the list.
3.  **Handle Conflicts:** If documents provide conflicting information for a single-value field (e.g., `license`), prioritize information from what appears to be the primary publication or include all conflicting details in the final string.

### JSON TEMPLATE & FIELD INSTRUCTIONS

You must generate a JSON object that follows this exact structure. For each field in the second graph element, follow the specific instructions to derive its value from _all_ the input texts. If information cannot be found or reasonably inferred, you MUST use the JSON value `null`.
```json
{
  "@context": {
    "@language": "en",
    "rai": "http://mlcommons.org/croissant/RAI/",
    "sc": "https://schema.org/",
    "dct": "http://purl.org/dc/terms/"
  },
  "@graph": [
    {
      "@id": "ro-crate-metadata.json",
      "@type": "CreativeWork",
      "conformsTo": {
        "@id": "https://w3id.org/ro/crate/1.1"
      },
      "about": {
        "@id": "./"
      }
    },
    {
      "@id": "./",
      "@type": "sc:Dataset",
      "name": null,
      "dct:conformsTo": "http://mlcommons.org/croissant/RAI/1.0",
      "description": null,
      "keywords": [],
      "author": null,
      "associatedPublication": null,
      "version": "1.0",
      "license": null,
      "conditionsOfAccess": null,
      "copyrightNotice": null,
      "rai:dataCollection": null,
      "rai:dataCollectionType": [],
      "rai:dataCollectionMissingData": null,
      "rai:dataCollectionRawData": null,
      "rai:dataCollectionTimeframe": [],
      "rai:dataImputationProtocol": null,
      "rai:dataManipulationProtocol": null,
      "rai:dataPreprocessingProtocol": [],
      "rai:dataAnnotationProtocol": null,
      "rai:dataAnnotationPlatform": [],
      "rai:dataAnnotationAnalysis": [],
      "rai:dataReleaseMaintenancePlan": null,
      "rai:personalSensitiveInformation": [],
      "rai:dataSocialImpact": null,
      "rai:dataBiases": [],
      "rai:dataLimitations": [],
      "rai:dataUseCases": [],
      "rai:annotationsPerItem": null,
      "rai:annotatorDemographics": [],
      "rai:machineAnnotationTools": []
    }
  ]
}
```

### INSTRUCTIONS FOR EACH FIELD IN THE SECOND GRAPH ELEMENT (sc:Dataset):

**Standard Metadata:**

- **name**: Extract the primary title from the main publication.
- **description**: Synthesize a summary from the abstracts/summaries of all documents.
- **keywords**: Aggregate unique keywords from all sources into a list of strings.
- **author**: Aggregate unique author names from all sources. Format the final list as a single, comma-separated string. For example: "Sami Nourreddine, Yesh Doctor, Amir Dailamy, et al.".
- **associatedPublication**: List the primary publication's citation. If multiple key papers are provided, you may list them all in the string.
- **license**: Find any mention of a license (e.g., "CC-BY-NC-ND 4.0 International license").
- **conditionsOfAccess**: Synthesize any text describing data access from all sources.
- **copyrightNotice**: Find the copyright holder information from any of the documents.

**RAI - Data Lifecycle Fields:**

- **rai:dataCollection**: Synthesize a detailed description of the data collection process from the 'Methods' sections of all relevant documents.
- **rai:dataCollectionType**: Aggregate the types of data collection (e.g., "Experiments", "Software Collection") from all sources and de-duplicate.
- **rai:dataCollectionMissingData**: Combine any mentions of missing data or data loss from any document.
- **rai:dataCollectionRawData**: Synthesize a description of the original source data from all relevant documents.
- **rai:dataCollectionTimeframe**: Consolidate any mentions of study dates or duration.

**RAI - Data Processing & Labeling Fields:**

- **rai:dataImputationProtocol**: Search all documents for mentions of data imputation.
- **rai:dataManipulationProtocol** & **rai:dataPreprocessingProtocol**: Combine all data cleaning, filtering, and normalization steps from all documents into a comprehensive list.
- **rai:dataAnnotationProtocol**, **rai:dataAnnotationPlatform**, **rai:dataAnnotationAnalysis**, **rai:annotationsPerItem**, **rai:annotatorDemographics**: Fill these fields only if human labeling is described. Synthesize information from all sources.
- **rai:machineAnnotationTools**: Aggregate all software tools used for automated feature extraction from all documents and de-duplicate.

**RAI - Compliance, Safety, and Fairness Fields:**

- **rai:dataReleaseMaintenancePlan**: Synthesize any information about future plans for the dataset.
- **rai:personalSensitiveInformation**: Conclude if PII is present based on the collective information from all papers.
- **rai:dataSocialImpact**: Synthesize the potential broader benefits from the 'Introduction' and 'Discussion' sections of all papers.
- **rai:dataBiases**: Create a comprehensive list of all inherent biases mentioned or implied across all documents.
- **rai:dataLimitations**: Create a comprehensive list of all known limitations acknowledged across all documents.
- **rai:dataUseCases**: Synthesize a list of all intended applications and future directions from all papers.

### INPUT TEXT

Now, analyze the following texts, separated by the separator, and generate the single, complete JSON object as instructed. OUTPUT SHOULD ONLY BE THE JSON OBJECT."""



class FairscapeLLMAssistRequest(FairscapeRequest):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def allowed_file(self, filename: str) -> bool:
        return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

    def extract_text_from_pdf(self, pdf_path: str) -> str:
        text = ""
        with open(pdf_path, 'rb') as file:
            reader = PyPDF2.PdfReader(file)
            for page in reader.pages:
                text += page.extract_text() + "\n"
        return text

    def extract_text_from_pdf_bytes(self, pdf_bytes: bytes) -> str:
        text = ""
        pdf_file = io.BytesIO(pdf_bytes)
        reader = PyPDF2.PdfReader(pdf_file)
        for page in reader.pages:
            text += page.extract_text() + "\n"
        return text

    def clean_llm_response(self, response_text: str) -> str:
        response_text = response_text.strip()
        
        json_match = re.search(r'```json\s*(\{.*?\})\s*```', response_text, re.DOTALL)
        if json_match:
            return json_match.group(1)
        
        code_match = re.search(r'```\s*(\{.*?\})\s*```', response_text, re.DOTALL)
        if code_match:
            return code_match.group(1)
        
        if response_text.startswith('{') and response_text.endswith('}'):
            return response_text
        
        json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
        if json_match:
            return json_match.group(0)
        
        return response_text

    def create_input_text_dataset(
        self,
        document_texts: List[str],
        filenames: List[str],
        requesting_user: UserWriteModel,
        task_guid: str
    ) -> str:
        combined_text = "\n\n--- DOCUMENT SEPARATOR ---\n\n".join(document_texts)
        
        dataset_ark = f"ark:59853/dataset-input-{uuid.uuid4()}"

        dataset = Dataset.model_validate({
            "@id": dataset_ark,
            "name": f"LLM Input Text for Task {task_guid}",
            "author": requesting_user.email,
            "datePublished": datetime.datetime.now().isoformat(),
            "version": "1.0",
            "description": f"Combined extracted text from {len(filenames)} documents: {', '.join(filenames)}",
            "keywords": ["llm-input", "extracted-text", "pdf-text"],
            "format": "text/plain",
            "additionalType": "https://w3id.org/EVI#Dataset",
            "contentUrl": None
        })
        
        text_bytes = io.BytesIO(combined_text.encode('utf-8'))
        upload_file = UploadFile(filename=f"{task_guid}.txt", file=text_bytes)
        
        dataset_request = FairscapeDatasetRequest(self.config)
        response = dataset_request.createDataset(
            userInstance=requesting_user,
            inputDataset=dataset,
            datasetContent=upload_file
        )
        
        if not response.success:
            raise Exception(f"Failed to create input dataset: {response.error}")
        
        return dataset_ark

    def create_llm_computation(
        self,
        input_dataset_ark: str,
        requesting_user: UserWriteModel,
        task_guid: str
    ) -> str:
        computation_ark = f"ark:59853/computation-{uuid.uuid4()}"

        computation = Computation.model_validate({
            "@id": computation_ark,
            "name": f"Gemini LLM Processing for Task {task_guid}",
            "runBy": requesting_user.email,
            "dateCreated": datetime.datetime.now().isoformat(),
            "description": "LLM processing of extracted PDF text to generate RO-Crate metadata using Google Gemini 2.0 Flash",
            "command": ["gemini-2.5-flash", "generate_rocrate_metadata"],
            "usedSoftware": [{"@id": "ark:59853/software-fairscape-llm-direct-v1"}],
            "usedMLModel": [{"@id": "ark:59853/model-gemini-2.5-flash"}],
            "usedDataset": [{"@id": input_dataset_ark}],
            "generated": []
        })
        
        permissions_set = requesting_user.getPermissions()
        now = datetime.datetime.now()

        stored_computation = StoredIdentifier.model_validate({
            "@id": computation_ark,
            "@type": MetadataTypeEnum.COMPUTATION,
            "metadata": computation,
            "permissions": permissions_set,
            "publicationStatus": PublicationStatusEnum.DRAFT,
            "dateCreated": now,
            "dateModified": now,
            "distribution": None
        })
        
        self.config.identifierCollection.insert_one(
            stored_computation.model_dump(by_alias=True, mode="json")
        )
        
        self.config.userCollection.update_one(
            {"email": requesting_user.email},
            {"$push": {"identifiers": computation_ark}}
        )
        
        return computation_ark

    def create_output_json_dataset(
        self,
        json_content: str,
        computation_ark: str,
        requesting_user: UserWriteModel,
        task_guid: str
    ) -> str:
        dataset_ark = f"ark:59853/dataset-ouput-{uuid.uuid4()}"

        dataset = Dataset.model_validate({
            "@id": dataset_ark,
            "name": f"LLM Generated RO-Crate for Task {task_guid}",
            "author": requesting_user.email,
            "datePublished": datetime.datetime.now().isoformat(),
            "version": "1.0",
            "description": "RO-Crate metadata generated by Gemini LLM from extracted PDF text",
            "keywords": ["llm-generated", "ro-crate", "metadata"],
            "format": "application/json",
            "generatedBy": [{"@id": computation_ark}],
            "additionalType": "https://w3id.org/EVI#Dataset",
            "contentUrl": None
        })
        
        json_bytes = io.BytesIO(json_content.encode('utf-8'))
        upload_file = UploadFile(filename=f"output_{task_guid}.json", file=json_bytes)
        
        dataset_request = FairscapeDatasetRequest(self.config)
        response = dataset_request.createDataset(
            userInstance=requesting_user,
            inputDataset=dataset,
            datasetContent=upload_file
        )
        
        if not response.success:
            raise Exception(f"Failed to create output dataset: {response.error}")
        
        self.config.identifierCollection.update_one(
            {"@id": computation_ark},
            {"$push": {"metadata.generated": {"@id": dataset_ark}}}
        )
        
        return dataset_ark

    def process_pdfs_with_llm(self, task_guid: str) -> str:
        task_doc = self.config.asyncCollection.find_one({"@id": task_guid})
        if not task_doc:
            raise Exception(f"Task {task_guid} not found")
        
        task = LLMAssistTask.model_validate(task_doc)
        
        user_doc = self.config.userCollection.find_one({"email": task.owner_email})
        if not user_doc:
            raise Exception(f"User {task.owner_email} not found")
        
        requesting_user = UserWriteModel.model_validate(user_doc)
        
        input_dataset_ark = None
        computation_ark = None
        output_dataset_ark = None
        
        try:
            self.config.asyncCollection.update_one(
                {"@id": task_guid},
                {
                    "$set": {
                        "status": "PROCESSING",
                        "time_started": datetime.datetime.utcnow()
                    }
                }
            )
            
            input_dataset_ark = self.create_input_text_dataset(
                document_texts=task.document_texts,
                filenames=task.filenames,
                requesting_user=requesting_user,
                task_guid=task_guid
            )
            
            computation_ark = self.create_llm_computation(
                input_dataset_ark=input_dataset_ark,
                requesting_user=requesting_user,
                task_guid=task_guid
            )
            
            combined_text = "\n\n--- DOCUMENT SEPARATOR ---\n\n".join(task.document_texts)
            
            genai.configure(api_key=os.environ.get("GEMINI_API_KEY"))
            model = genai.GenerativeModel('gemini-2.5-flash')
            
            response = model.generate_content(
                SYSTEM_PROMPT + "\n\n" + combined_text,
                generation_config=genai.types.GenerationConfig(
                    response_mime_type="application/json",
                    temperature=0.1,
                    max_output_tokens=8192,
                )
            )
            
            cleaned_response = self.clean_llm_response(response.text)
            
            try:
                json.loads(cleaned_response)
            except json.JSONDecodeError as e:
                raise ValueError(f"LLM returned invalid JSON: {str(e)}")
            
            output_dataset_ark = self.create_output_json_dataset(
                json_content=cleaned_response,
                computation_ark=computation_ark,
                requesting_user=requesting_user,
                task_guid=task_guid
            )
            
            self.config.asyncCollection.update_one(
                {"@id": task_guid},
                {
                    "$set": {
                        "status": "SUCCESS",
                        "result": cleaned_response,
                        "time_finished": datetime.datetime.utcnow(),
                        "input_dataset_ark": input_dataset_ark,
                        "output_dataset_ark": output_dataset_ark,
                        "computation_ark": computation_ark
                    }
                }
            )
            
            return cleaned_response
            
        except Exception as e:
            error_info = {
                "message": str(e),
                "input_dataset_ark": input_dataset_ark,
                "computation_ark": computation_ark
            }
            
            self.config.asyncCollection.update_one(
                {"@id": task_guid},
                {
                    "$set": {
                        "status": "ERROR",
                        "error": error_info,
                        "time_finished": datetime.datetime.utcnow()
                    }
                }
            )
            
            raise

    def create_llm_assist_task(
        self,
        requesting_user: UserWriteModel,
        files: List[any],
        task_guid: str
    ) -> FairscapeResponse:
        
        pdf_files = [f for f in files if f and self.allowed_file(f.filename)]
        
        if not pdf_files:
            return FairscapeResponse(
                success=False,
                statusCode=400,
                error={"message": "No valid PDF files provided"}
            )
        
        try:
            document_texts = []
            filenames = []
            
            for file in pdf_files:
                filename = secure_filename(file.filename)
                filenames.append(filename)
                
                pdf_bytes = file.file.read()
                text = self.extract_text_from_pdf_bytes(pdf_bytes)
                document_texts.append(text)
            
            task_data = {
                "@id": task_guid,
                "owner_email": requesting_user.email,
                "document_texts": document_texts,
                "filenames": filenames,
                "status": "PENDING"
            }
            
            task_model = LLMAssistTask.model_validate(task_data)
            self.config.asyncCollection.insert_one(task_model.model_dump(by_alias=True))
            
            return FairscapeResponse(
                success=True,
                statusCode=202,
                model=task_model
            )
            
        except Exception as e:
            return FairscapeResponse(
                success=False,
                statusCode=500,
                error={"message": f"Error creating task: {str(e)}"}
            )

    def get_task_status(self, task_guid: str) -> FairscapeResponse:
        task_doc = self.config.asyncCollection.find_one({"@id": task_guid}, {"_id": 0})

        if not task_doc:
            return FairscapeResponse(
                success=False,
                statusCode=404,
                error={"message": "Task not found"}
            )

        try:
            task_model = LLMAssistTask.model_validate(task_doc)
            return FairscapeResponse(
                success=True,
                statusCode=200,
                model=task_model
            )
        except Exception as e:
            return FairscapeResponse(
                success=False,
                statusCode=500,
                error={"message": f"Error validating task: {str(e)}"}
            )

    def parse_issue_body(self, issue_body: str) -> Dict[str, str]:
        """Extract project name and other metadata from issue body"""
        project_name = "Unknown Project"
        urls = []

        # Try to extract project name from common patterns
        project_match = re.search(r'(?:Project|Dataset):\s*(.+)', issue_body, re.IGNORECASE)
        if project_match:
            project_name = project_match.group(1).strip()

        # Extract URLs
        url_pattern = r'https?://[^\s<>"{}|\\^`\[\]]+'
        urls = re.findall(url_pattern, issue_body)

        return {
            "project_name": project_name,
            "urls": urls
        }

    def create_d4d_request_dataset(
        self,
        issue_number: int,
        issue_title: str,
        issue_body: str,
        issue_comments: List[Dict[str, Any]],
        issue_url: str,
        requesting_user: UserWriteModel
    ) -> str:
        """Create a dataset representing the user's D4D request from GitHub issue"""

        parsed_info = self.parse_issue_body(issue_body)
        project_name = parsed_info["project_name"]

        # Format the request text
        request_text = f"User Request for D4D Generation\n"
        request_text += f"Issue #{issue_number}: {issue_title}\n"
        request_text += f"Project: {project_name}\n\n"
        request_text += f"=== Issue Body ===\n{issue_body}\n\n"

        if issue_comments:
            request_text += f"=== Comments ({len(issue_comments)}) ===\n"
            for comment in issue_comments:
                user = comment.get('user', 'Unknown')
                body = comment.get('body', '')
                request_text += f"\n--- Comment by {user} ---\n{body}\n"

        dataset_ark = f"ark:59853/dataset-input-{uuid.uuid4()}"

        dataset = Dataset.model_validate({
            "@id": dataset_ark,
            "name": f"User Request for D4D: {project_name}",
            "author": requesting_user.email,
            "datePublished": datetime.datetime.now().isoformat(),
            "version": "1.0",
            "description": f"User request from GitHub issue #{issue_number} for D4D generation: {issue_title}",
            "keywords": ["d4d-request", "github-issue", "user-input"],
            "format": "text/plain",
            "additionalType": "https://w3id.org/EVI#Dataset",
            "contentUrl": None,
            "url": issue_url
        })

        text_bytes = io.BytesIO(request_text.encode('utf-8'))
        upload_file = UploadFile(filename="d4d_request.txt", file=text_bytes)

        dataset_request = FairscapeDatasetRequest(self.config)
        response = dataset_request.createDataset(
            userInstance=requesting_user,
            inputDataset=dataset,
            datasetContent=upload_file
        )

        if not response.success:
            raise Exception(f"Failed to create request dataset: {response.error}")

        return dataset_ark

    def create_d4d_computation(
        self,
        request_dataset_ark: str,
        issue_number: int,
        issue_url: str,
        requesting_user: UserWriteModel
    ) -> str:
        """Create a computation representing D4D generation from GitHub issue"""

        computation_ark = f"ark:59853/computation-{uuid.uuid4()}"

        computation = Computation.model_validate({
            "@id": computation_ark,
            "name": f"D4D Generation for Issue #{issue_number}",
            "runBy": "d4dassistant",
            "dateCreated": datetime.datetime.now().isoformat(),
            "description": f"D4D generation process for GitHub issue #{issue_number} using the D4D Assistant bot",
            "command": ["d4d-assistant", "generate_yaml"],
            "usedSoftware": [{"@id": "ark:59853/software-d4d-assistant-bot-v1"}],
            "usedMLModel": [{"@id": "ark:59853/model-sonnet-4-5"}],
            "usedDataset": [{"@id": request_dataset_ark}],
            "generated": [],
            "url": issue_url
        })

        permissions_set = requesting_user.getPermissions()
        now = datetime.datetime.now()

        stored_computation = StoredIdentifier.model_validate({
            "@id": computation_ark,
            "@type": MetadataTypeEnum.COMPUTATION,
            "metadata": computation,
            "permissions": permissions_set,
            "publicationStatus": PublicationStatusEnum.DRAFT,
            "dateCreated": now,
            "dateModified": now,
            "distribution": None
        })

        self.config.identifierCollection.insert_one(
            stored_computation.model_dump(by_alias=True, mode="json")
        )

        return computation_ark

    def create_d4d_yaml_dataset(
        self,
        yaml_url: str,
        yaml_content: str,
        computation_ark: str,
        project_name: str,
        issue_url: str,
        requesting_user: UserWriteModel
    ) -> str:
        """Create a dataset for the D4D YAML file"""

        dataset_ark = f"ark:59853/dataset-ouput-{uuid.uuid4()}"

        dataset = Dataset.model_validate({
            "@id": dataset_ark,
            "name": f"D4D YAML for {project_name}",
            "author": requesting_user.email,
            "datePublished": datetime.datetime.now().isoformat(),
            "version": "1.0",
            "description": f"D4D YAML generated by d4dassistant bot.",
            "keywords": ["d4d", "yaml", "generated", "data-sheet"],
            "format": "application/x-yaml",
            "generatedBy": [{"@id": computation_ark}],
            "additionalType": "https://w3id.org/EVI#Dataset",
            "contentUrl": None,
            "url": issue_url
        })

        yaml_bytes = io.BytesIO(yaml_content.encode('utf-8'))
        upload_file = UploadFile(filename="d4d.yaml", file=yaml_bytes)

        dataset_request = FairscapeDatasetRequest(self.config)
        response = dataset_request.createDataset(
            userInstance=requesting_user,
            inputDataset=dataset,
            datasetContent=upload_file
        )

        if not response.success:
            raise Exception(f"Failed to create YAML dataset: {response.error}")

        # Update computation's generated field
        self.config.identifierCollection.update_one(
            {"@id": computation_ark},
            {"$push": {"metadata.generated": {"@id": dataset_ark}}}
        )

        return dataset_ark

    def convert_yaml_to_rocrate(self, yaml_content: str) -> Dict[str, Any]:
        """Convert D4D YAML to RO-Crate JSON"""
        try:
            # Parse YAML
            d4d_data = yaml.safe_load(yaml_content)



            # Try to validate as DatasetCollection or Dataset
            try:
                d4d_collection = DatasetCollection.model_validate(d4d_data)
            except Exception:
                try:
                    d4d_collection = D4DDataset.model_validate(d4d_data)
                except Exception:
                    # Flexible fallback
                    class FlexibleData:
                        def __init__(self, data_dict):
                            self.__dict__.update(data_dict)

                        def model_dump(self):
                            return self.__dict__

                    d4d_collection = FlexibleData(d4d_data)

            converter = TargetToROCrateConverter(
                source_collection=d4d_collection,
                dataset_mappings=DATASET_TO_SUBCRATE_MAPPING,
                collection_mapping=DATASET_COLLECTION_TO_RELEASE_MAPPING
            )

            rocrate = converter.convert()
            rocrate_dict = rocrate.model_dump(by_alias=True, exclude_none=True)

            return rocrate_dict

        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML content: {str(e)}")
        except Exception as e:
            raise ValueError(f"Conversion failed: {str(e)}")

    def process_d4d_issue_with_provenance(
        self,
        request: D4DFromIssueRequest,
        requesting_user: UserWriteModel
    ) -> Dict[str, Any]:
        """
        Process a D4D issue and create full provenance chain:
        1. Create user request dataset from issue
        2. Create computation for D4D generation
        3. Fetch and create YAML dataset
        4. Convert YAML to RO-Crate
        5. Return RO-Crate + provenance
        """

        try:
            # Step 1: Create user request dataset
            request_dataset_ark = self.create_d4d_request_dataset(
                issue_number=request.issue_number,
                issue_title=request.issue_title,
                issue_body=request.issue_body,
                issue_comments=request.issue_comments,
                issue_url=request.issue_url,
                requesting_user=requesting_user
            )

            # Step 2: Create computation
            computation_ark = self.create_d4d_computation(
                request_dataset_ark=request_dataset_ark,
                issue_number=request.issue_number,
                issue_url=request.issue_url,
                requesting_user=requesting_user
            )

            # Step 3: Fetch YAML from URL
            response = requests.get(request.yaml_url, timeout=30)
            response.raise_for_status()
            yaml_content = response.text

            # Parse to get project name
            parsed_info = self.parse_issue_body(request.issue_body)
            project_name = parsed_info["project_name"]

            # Step 4: Create YAML dataset
            yaml_dataset_ark = self.create_d4d_yaml_dataset(
                yaml_url=request.yaml_url,
                yaml_content=yaml_content,
                computation_ark=computation_ark,
                project_name=project_name,
                issue_url=request.issue_url,
                requesting_user=requesting_user
            )

            # Step 5: Convert YAML to RO-Crate
            rocrate_json = self.convert_yaml_to_rocrate(yaml_content)

            # Return RO-Crate + provenance
            return {
                "rocrate": rocrate_json,
                "provenance": {
                    "inputArk": request_dataset_ark,
                    "outputArk": yaml_dataset_ark,
                    "computationArk": computation_ark,
                    "yamlUrl": request.yaml_url,
                    "requiresGithubPush": True,
                    "sourceFlow": "chatbot"
                }
            }

        except requests.RequestException as e:
            raise Exception(f"Failed to fetch YAML from URL: {str(e)}")
        except Exception as e:
            raise Exception(f"Failed to process D4D issue: {str(e)}")