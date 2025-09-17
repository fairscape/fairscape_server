from typing import (
	Annotated
)
from fastapi import (
	APIRouter, 
	Depends, 
	HTTPException, 
	Request, 
	UploadFile
)
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.encoders import jsonable_encoder
from fairscape_mds.crud.rocrate import FairscapeROCrateRequest

from fairscape_mds.models.user import UserWriteModel
from fairscape_mds.core.config import appConfig
from fairscape_models.rocrate import ROCrateV1_2, ROCrateMetadataElem
from fairscape_mds.deps import getCurrentUser
from fairscape_mds.worker import processROCrate

from fairscape_models.conversion.converter import ROCToTargetConverter
from fairscape_models.conversion.mapping.croissant import MAPPING_CONFIGURATION as CROISSANT_MAPPING

import pathlib

rocrateRequest = FairscapeROCrateRequest(appConfig)

rocrateRouter = APIRouter(prefix="", tags=['evi', 'rocrate'])


@rocrateRouter.post("/rocrate/upload-async")
def uploadROCrate(
	currentUser: Annotated[UserWriteModel, Depends(getCurrentUser)],
	crate: UploadFile
):

	uploadOperation = rocrateRequest.uploadROCrate(
		userInstance=currentUser,
		rocrate=crate
	)

	if uploadOperation.success:

		uploadJob = uploadOperation.model

		# start backend job
		processROCrate.apply_async(args=(uploadJob.guid,), )

		return uploadJob

	else:
		return JSONResponse(
			status_code=400,
			content={"error": uploadOperation.error}
		)
  
@rocrateRouter.post(
    "/rocrate/metadata",
    summary="Mint metadata-only ROCrate records without file content",
    status_code=201
)
def publishMetadataOnly(
    currentUser: Annotated[UserWriteModel, Depends(getCurrentUser)],
    crateMetadata: ROCrateV1_2
):
    try:
        # Call the mintMetadataOnlyROCrate method on the existing rocrateRequest
        result = rocrateRequest.mintMetadataOnlyROCrate(
            requestingUser=currentUser,
            crateModel=crateMetadata
        )
        
        if result.success:
            return JSONResponse(
                content=result.model,
                status_code=result.statusCode
            )
        else:
            return JSONResponse(
                content=result.error,
                status_code=result.statusCode
            )
    except Exception as e:
        return JSONResponse(
            content={
                "message": "Error minting metadata-only ROCrate identifiers",
                "error": str(e)
            },
            status_code=500
        )
        
@rocrateRouter.get(
    "/rocrate",
    summary="List all ROCrates accessible by the current user",
    response_description="A list of RO-Crates with their basic metadata"
)
def list_rocrates_endpoint(
    currentUser: Annotated[UserWriteModel, Depends(getCurrentUser)],
):
    fairscape_response = rocrateRequest.list_crates(requestingUser=currentUser)

    if fairscape_response.success:
        return JSONResponse(
            status_code=fairscape_response.statusCode,
            content=fairscape_response.model
        )
    else:
        raise HTTPException(
            status_code=fairscape_response.statusCode,
            detail=fairscape_response.error
        )


@rocrateRouter.get("/rocrate/upload/status/{submissionUUID}")
def getUploadStatus(
	currentUser: Annotated[UserWriteModel, Depends(getCurrentUser)],
	submissionUUID: str
):

	response = rocrateRequest.getUploadMetadata(
		currentUser,
		submissionUUID
	)

	if response.success:
		return response.model

	else:
		return JSONResponse(
			status_code = response.statusCode,
			content = response.error
		)


@rocrateRouter.get("/rocrate/download/ark:{NAAN}/{postfix}")
def getROCrateArchive(
	currentUser: Annotated[UserWriteModel, Depends(getCurrentUser)],
	NAAN: str,
	postfix: str
):

	rocrateGUID = f"ark:{NAAN}/{postfix}"

	response = rocrateRequest.downloadROCrateArchive(
		currentUser,
		rocrateGUID
	)
	
	if response.success:

		object_key = response.model.distribution.location.path
		filename = pathlib.Path(object_key).name

		zip_headers = {
			"Content-Type": "application/zip",
			"Content-Disposition": f'attachment; filename="{filename}"'
    	}
        
		return StreamingResponse(
			response.fileResponse,
			headers=zip_headers
		)

	else:
		return JSONResponse(
			status_code = response.statusCode,
			content = response.error
		)

@rocrateRouter.get("/rocrate/ark:{NAAN}/{postfix}")
def getROCrateMetadata(
    request: Request,
    NAAN: str,
    postfix: str,
):
    """
    Retrieve RO-Crate metadata.  
    Supports content negotiation:  
    - `application/json` (default, raw RO-Crate JSON)  
    - `application/vnd.mlcommons-croissant+json` (Croissant JSON-LD)  
    """
    guid = f"ark:{NAAN}/{postfix}"
    response = rocrateRequest.getROCrateMetadata(guid)

    if not response.success:
        return JSONResponse(
            status_code=response.statusCode,
            content=response.error
        )

    accept_header = request.headers.get("accept", "application/json")

    if "application/vnd.mlcommons-croissant+json" in accept_header.lower():
        try:
            source_crate = ROCrateV1_2(**response.model["metadata"])
            croissant_converter = ROCToTargetConverter(source_crate, CROISSANT_MAPPING)
            croissant_result = croissant_converter.convert()

            return JSONResponse(
                status_code=200,
                content=croissant_result.model_dump(by_alias=True, exclude_none=True),
                media_type="application/vnd.mlcommons-croissant+json"
            )
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Error converting RO-Crate to Croissant: {str(e)}"
            )

    return JSONResponse(
        status_code=200,
        content=response.model
    )

@rocrateRouter.get("/rocrate/ai-ready-score/ark:{NAAN}/{postfix}")
def getAIReadyScore(
    NAAN: str,
    postfix: str
):
    
    hard_coded_score = {
        'name': 'CM4AI June 2025 Release',
        'characterization': {
            'data_quality': {
                'details': 'Data quality procedures not documented',
                'has_content': False
            },
            'potential_sources_of_bias': {
                'details': 'Data in this release was derived from commercially available de-identified human cell lines, and does not represent all biological variants which may be seen in the population at large. ',
                'has_content': True
            },
            'semantics': {
                'details': 'Data is semantically described using the schema.org vocabulary within a machine-readable RO-Crate.',
                'has_content': True
            },
            'standards': {
                'details': 'This dataset adheres to the RO-Crate 1.2 and Croissant RAI 1.0 community standards.',
                'has_content': True
            },
            'statistics': {
                'details': 'Total size: 54.5 TB, Summary statistics available for 1 dataset(s)',
                'has_content': True
            }
        },
        'computability': {
            'computationally_accessible': {
                'details': 'Data is hosted in public repositories (e.g., NCBI, MassIVE, Dataverse) that support programmatic access.',
                'has_content': True
            },
            'contextualized': {
                'details': "Context is provided by the RO-Crate's graph structure and detailed in properties such as rai:dataLimitations.",
                'has_content': True
            },
            'portable': {
                'details': 'The dataset is packaged as a self-contained RO-Crate, a standard designed for portability across systems.',
                'has_content': True
            },
            'standardized': {
                'details': 'Formats: .d, .d directory group, .tsv, .xml, TSV...',
                'has_content': True
            }
        },
        'ethics': {
            'ethically_acquired': {
                'details': 'Human subject info: No',
                'has_content': True
            },
            'ethically_disseminated': {
                'details': 'License: https://creativecommons.org/licenses/by-nc-sa/4.0/, Prohibited uses: These laboratory data are not to be used in clinical decision-making or in any context involving patient care without appropriate regulatory oversight and approval.',
                'has_content': True
            },
            'ethically_managed': {
                'details': 'Ethical review: Vardit Ravistky ravitskyv@thehastingscenter.org and Jean-Christophe Belisle-Pipon jean-christophe_belisle-pipon@sfu.ca., Governance: Jillian Parker; jillianparker@health.ucsd.edu',
                'has_content': True
            },
            'secure': {
                'details': 'Confidentiality level: Unrestricted',
                'has_content': True
            }
        },
        'fairness': {
            'accessible': {
                'details': "The RO-Crate's JSON-LD metadata is machine-readable and publicly accessible by design.",
                'has_content': True
            },
            'findable': {
                'details': 'Dataset has DOI: https://doi.org/10.18130/V3/B35XWX',
                'has_content': True
            },
            'interoperable': {
                'details': 'The dataset uses the schema.org vocabulary within the RO-Crate framework and conforms to the Croissant RAI specification for interoperability.',
                'has_content': True
            },
            'reusable': {
                'details': 'License: https://creativecommons.org/licenses/by-nc-sa/4.0/',
                'has_content': True
            }
        },
        'pre_model_explainability': {
            'data_documentation_template': {
                'details': "Documentation is provided via the RO-Crate's structured JSON-LD metadata, this HTML Datasheet, and Croissant RAI properties.",
                'has_content': True
            },
            'fit_for_purpose': {
                'details': 'Use cases: AI-ready datasets to support research in functional genomics, AI model training, cellular process analysis, cell architectural changes, and interactions in presence of specific disease processes, treatment conditions, or genetic perturbations. A major goal is to enable visible machine learning applications, as proposed in Ma et al. (2018) Nature Methods., Limitations: This is an interim release. It does not contain predicted cell maps, which will be added in future releases. The current release is most suitable for bioinformatics analysis of the individual datasets. Requires domain expertise for meaningful analysis.',
                'has_content': True
            },
            'verifiable': {
                'details': '0% of files have checksums (16/51797)',
                'has_content': True
            }
        },
        'provenance': {
            'interpretable': {
                'details': '11 software instances documented',
                'has_content': True
            },
            'key_actors_identified': {
                'details': 'Author specified, Publisher: https://dataverse.lib.virginia.edu/, PI: Trey Ideker',
                'has_content': True
            },
            'traceable': {
                'details': '1716 computation/experiment steps documented',
                'has_content': True
            },
            'transparent': {
                'details': '51771 dataset(s) documented',
                'has_content': True
            }
        },
        'sustainability': {
            'associated': {
                'details': "All data, software, and computations are explicitly linked within the RO-Crate's provenance graph.",
                'has_content': True
            },
            'domain_appropriate': {
                'details': 'Maintenance plan: Dataset will be regularly updated and augmented through the end of the project in November 2026, on a quarterly basis. Long term preservation in the https://dataverse.lib.virginia.edu/, supported by committed institutional funds.',
                'has_content': True
            },
            'persistent': {
                'details': 'Dataset has DOI: https://doi.org/10.18130/V3/B35XWX',
                'has_content': True
            },
            'well_governed': {
                'details': 'Governance committee: Jillian Parker; jillianparker@health.ucsd.edu',
                'has_content': True
            }
        }
    }
    
    return JSONResponse(
        status_code=200,
        content=hard_coded_score
    )