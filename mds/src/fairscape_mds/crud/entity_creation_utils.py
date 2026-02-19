"""
RO-Crate Entity Management Helper Functions

Provides reusable utilities for managing bidirectional relationships between
RO-Crates and their constituent entities (Datasets, Software, Computations).
"""

from typing import List, Optional, Dict, Any
from pymongo.collection import Collection
from fairscape_models.fairscape_base import IdentifierValue
from fairscape_mds.models.identifier import StoredIdentifier, MetadataTypeEnum
from fairscape_mds.models.rocrate import ROCrateContentSummary, ContentSummaryItem, ContentCounts
from fairscape_mds.models.user import UserWriteModel, checkPermissions
import datetime


def validateROCrateParents(
    identifierCollection: Collection,
    isPartOf: List[IdentifierValue],
    requestingUser: UserWriteModel
) -> Dict[str, Any]:
    """
    Validate that all parent RO-Crates in isPartOf list exist and user has permissions.

    This function checks:
    1. Each parent entity exists in the database
    2. Each parent is actually a RO-Crate
    3. User has write permissions on each parent RO-Crate

    Args:
        identifierCollection: MongoDB collection containing all identifiers
        isPartOf: List of IdentifierValue objects representing parent entities
        requestingUser: User making the request

    Returns:
        Dictionary with:
            'valid': bool - True if all validations passed
            'errors': list - List of error message strings (empty if valid)

    Example:
        validation = validateROCrateParents(collection, dataset.isPartOf, user)
        if not validation['valid']:
            return error_response(validation['errors'])
    """
    errors = []

    for parent in isPartOf:
        parent_guid = parent.guid

        # Check if parent exists
        parent_doc = identifierCollection.find_one(
            {"@id": parent_guid},
            projection={"_id": False}
        )

        if not parent_doc:
            errors.append(f"Parent entity not found: {parent_guid}")
            continue

        try:
            parent_stored = StoredIdentifier.model_validate(parent_doc)
        except Exception as e:
            errors.append(f"Invalid parent entity structure: {parent_guid} - {str(e)}")
            continue

        if parent_stored.metadataType != MetadataTypeEnum.ROCRATE:
            errors.append(f"Parent is not a RO-Crate (type: {parent_stored.metadataType}): {parent_guid}")
            continue

        if not checkPermissions(parent_stored.permissions, requestingUser):
            errors.append(f"User unauthorized to modify RO-Crate: {parent_guid}")

    return {
        'valid': len(errors) == 0,
        'errors': errors
    }


def addEntityToROCrate(
    identifierCollection: Collection,
    rocrate_guid: str,
    entity_guid: str,
    entity_type: str,
    entity_name: str
) -> bool:
    """
    Add an entity to a RO-Crate's hasPart list and regenerate contentSummary.

    Uses MongoDB's atomic $push operation to add the entity reference.
    Automatically triggers contentSummary regeneration to keep it in sync.

    Args:
        identifierCollection: MongoDB collection
        rocrate_guid: ARK identifier of the parent RO-Crate
        entity_guid: ARK identifier of the entity to add
        entity_type: Type string (e.g., "https://w3id.org/EVI#Dataset")
        entity_name: Display name of the entity

    Returns:
        bool: True if successful, False if update failed

    Example:
        success = addEntityToROCrate(
            collection,
            "ark:99999/rocrate1",
            "ark:99999/dataset1",
            MetadataTypeEnum.DATASET.value,
            "My Dataset"
        )
    """
    entity_identifier = IdentifierValue.model_validate({
        "@id": entity_guid,
        "@type": entity_type,
        "name": entity_name
    })

    update_result = identifierCollection.update_one(
        {"@id": rocrate_guid},
        {
            "$push": {
                "metadata.hasPart": entity_identifier.model_dump(by_alias=True, mode='json')
            },
            "$set": {
                "dateModified": datetime.datetime.now(tz=datetime.timezone.utc)
            }
        }
    )

    if update_result.modified_count != 1:
        return False

    regenerateContentSummary(identifierCollection, rocrate_guid)

    return True


def removeEntityFromROCrate(
    identifierCollection: Collection,
    rocrate_guid: str,
    entity_guid: str
) -> bool:
    """
    Remove an entity from a RO-Crate's hasPart list and regenerate contentSummary.

    Uses MongoDB's atomic $pull operation to remove the entity reference.
    Automatically triggers contentSummary regeneration to keep it in sync.

    Args:
        identifierCollection: MongoDB collection
        rocrate_guid: ARK identifier of the parent RO-Crate
        entity_guid: ARK identifier of the entity to remove

    Returns:
        bool: True if successful, False if update failed

    Example:
        success = removeEntityFromROCrate(
            collection,
            "ark:99999/rocrate1",
            "ark:99999/dataset1"
        )
    """
    update_result = identifierCollection.update_one(
        {"@id": rocrate_guid},
        {
            "$pull": {
                "metadata.hasPart": {"@id": entity_guid}
            },
            "$set": {
                "dateModified": datetime.datetime.now(tz=datetime.timezone.utc)
            }
        }
    )

    if update_result.modified_count != 1:
        return False

    regenerateContentSummary(identifierCollection, rocrate_guid)

    return True


def regenerateContentSummary(
    identifierCollection: Collection,
    rocrate_guid: str
) -> Optional[Dict[str, Any]]:
    """
    Regenerate the contentSummary for a RO-Crate by fetching and categorizing all hasPart entities.

    This function:
    1. Fetches the RO-Crate document
    2. Extracts all entity GUIDs from hasPart
    3. Queries for all entities in a single bulk operation
    4. Categorizes entities by type (datasets, software, computations, etc.)
    5. Counts each category
    6. Updates the RO-Crate with new contentSummary

    Args:
        identifierCollection: MongoDB collection
        rocrate_guid: ARK identifier of the RO-Crate

    Returns:
        Dict: The new contentSummary, or None if RO-Crate not found

    Example:
        new_summary = regenerateContentSummary(collection, "ark:99999/rocrate1")
    """
    rocrate_doc = identifierCollection.find_one(
        {"@id": rocrate_guid},
        projection={"_id": False}
    )

    if not rocrate_doc:
        return None

    rocrate_stored = StoredIdentifier.model_validate(rocrate_doc)

    has_part = []
    if hasattr(rocrate_stored.metadata, 'hasPart') and rocrate_stored.metadata.hasPart:
        has_part = rocrate_stored.metadata.hasPart

    if not has_part or len(has_part) == 0:
        empty_summary = {
            "datasets": [],
            "software": [],
            "computations": [],
            "schemas": [],
            "samples": [],
            "mlModels": [],
            "rocrates": [],
            "other": [],
            "counts": {
                "datasets": 0,
                "software": 0,
                "computations": 0,
                "schemas": 0,
                "samples": 0,
                "mlModels": 0,
                "rocrates": 0,
                "other": 0,
                "total": 0
            },
            "generatedAt": datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
        }

        identifierCollection.update_one(
            {"@id": rocrate_guid},
            {"$set": {"contentSummary": empty_summary}}
        )
        return empty_summary

    entity_guids = [part.guid for part in has_part]
    entities_cursor = identifierCollection.find(
        {"@id": {"$in": entity_guids}},
        projection={"_id": False}
    )

    datasets = []
    software = []
    computations = []
    schemas = []
    samples = []
    ml_models = []
    rocrates = []
    other = []

    for entity_doc in entities_cursor:
        entity_stored = StoredIdentifier.model_validate(entity_doc)
        entity_type = entity_stored.metadataType

        entity_name = "Unnamed"
        if hasattr(entity_stored.metadata, 'name'):
            entity_name = entity_stored.metadata.name

        summary_item = ContentSummaryItem(
            **{
                "@id": entity_stored.guid,
                "name": entity_name,
                "@type": str(entity_type.value) if hasattr(entity_type, 'value') else str(entity_type)
            }
        )

        # Categorize by type
        if entity_type == MetadataTypeEnum.DATASET:
            datasets.append(summary_item)
        elif entity_type == MetadataTypeEnum.SOFTWARE:
            software.append(summary_item)
        elif entity_type == MetadataTypeEnum.COMPUTATION:
            computations.append(summary_item)
        elif entity_type == MetadataTypeEnum.SCHEMA:
            schemas.append(summary_item)
        elif entity_type == MetadataTypeEnum.SAMPLE:
            samples.append(summary_item)
        elif entity_type == MetadataTypeEnum.ML_MODEL:
            ml_models.append(summary_item)
        elif entity_type == MetadataTypeEnum.ROCRATE:
            rocrates.append(summary_item)
        else:
            other.append(summary_item)

    counts = ContentCounts(
        datasets=len(datasets),
        software=len(software),
        computations=len(computations),
        schemas=len(schemas),
        samples=len(samples),
        mlModels=len(ml_models),
        rocrates=len(rocrates),
        other=len(other),
        total=len(datasets) + len(software) + len(computations) +
              len(schemas) + len(samples) + len(ml_models) +
              len(rocrates) + len(other)
    )

    content_summary = ROCrateContentSummary(
        datasets=datasets,
        software=software,
        computations=computations,
        schemas=schemas,
        samples=samples,
        mlModels=ml_models,
        rocrates=rocrates,
        other=other,
        counts=counts,
        generatedAt=datetime.datetime.now(tz=datetime.timezone.utc)
    )

    identifierCollection.update_one(
        {"@id": rocrate_guid},
        {"$set": {"contentSummary": content_summary.model_dump(mode='json', by_alias=True)}}
    )

    return content_summary.model_dump(mode='json', by_alias=True)


def findFirstROCrateInIsPartOf(
    identifierCollection: Collection,
    isPartOf: List[IdentifierValue]
) -> Optional[str]:
    """
    Find the first RO-Crate GUID in an isPartOf list.

    The isPartOf list may contain Organizations, Projects, and RO-Crates.
    This function returns the first entity that is actually a RO-Crate.

    Args:
        identifierCollection: MongoDB collection
        isPartOf: List of parent entities

    Returns:
        str: RO-Crate GUID if found, None otherwise

    Example:
        rocrate_guid = findFirstROCrateInIsPartOf(collection, dataset.isPartOf)
        if rocrate_guid:
            # Use RO-Crate-specific storage path
    """
    for parent in isPartOf:
        parent_doc = identifierCollection.find_one(
            {"@id": parent.guid},
            projection={"@type": 1, "_id": 0}
        )

        if parent_doc and parent_doc.get('@type') == MetadataTypeEnum.ROCRATE.value:
            return parent.guid

    return None
