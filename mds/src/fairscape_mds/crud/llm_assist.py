import os
import tempfile
import json
import re
from typing import List
from werkzeug.utils import secure_filename
import google.generativeai as genai
import PyPDF2

from fairscape_mds.crud.fairscape_request import FairscapeRequest
from fairscape_mds.crud.fairscape_response import FairscapeResponse
from fairscape_mds.models.user import UserWriteModel
from fairscape_mds.models.llm_assist import LLMAssistTask


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
        import io
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

    def process_pdfs_with_llm(self, document_texts: List[str]) -> str:
        combined_text = "\n\n--- DOCUMENT SEPARATOR ---\n\n".join(document_texts)
        
        genai.configure(api_key=os.environ.get("GEMINI_API_KEY"))
        model = genai.GenerativeModel('gemini-2.0-flash-exp')
        
        response = model.generate_content(
            SYSTEM_PROMPT + "\n\n" + combined_text,
            generation_config=genai.types.GenerationConfig(
                temperature=0.2,
                max_output_tokens=8192,
            )
        )
        
        cleaned_response = self.clean_llm_response(response.text)
        
        try:
            json.loads(cleaned_response)
        except json.JSONDecodeError as e:
            raise ValueError(f"LLM returned invalid JSON: {str(e)}")
        
        return cleaned_response

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