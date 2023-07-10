from typing import List, Optional

from pyatlan.cache.custom_metadata_cache import CustomMetadataCache
from pyatlan.client.atlan import AtlanClient
from pyatlan.error import NotFoundError, ConflictError
from pyatlan.events.atlan_lambda_handler import process_event
from pyatlan.events.atlan_event_handler import (
    AtlanEventHandler,
    get_current_view_of_asset,
    has_description,
    has_lineage,
    has_owner,
)
from pyatlan.exceptions import AtlanException
from pyatlan.model.assets import Asset, Badge, AtlasGlossaryTerm, Readme
from pyatlan.model.enums import CertificateStatus, AtlanCustomAttributePrimitiveType, BadgeComparisonOperator, \
    BadgeConditionColor
from pyatlan.model.events import AtlanEvent, AtlanEventPayload
from pyatlan.model.structs import BadgeCondition
from pyatlan.model.typedef import CustomMetadataDef, AttributeDef

CM_DAAP = "DaaP"
CM_ATTR_DAAP_SCORE = "Score"

SCORED_ATTRS = [
    "description",
    "userDescription",
    "ownerUsers",
    "ownerGroups",
    "meanings",
    "__hasLineage",
    "classifications",
    "inputToProcesses",
    "outputFromProcesses",
    "assignedEntities",
    "seeAlso",
    "links",
]
client = AtlanClient()


def _create_cm_if_not_exists() -> str:
    try:
        return CustomMetadataCache.get_id_for_name(CM_DAAP)
    except NotFoundError:
        try:
            cm_def = CustomMetadataDef.create(display_name=CM_DAAP)
            cm_def.attribute_defs = [
                AttributeDef.create(
                    display_name=CM_ATTR_DAAP_SCORE,
                    attribute_type=AtlanCustomAttributePrimitiveType.DECIMAL,
                )
            ]
            cm_def.options = CustomMetadataDef.Options.with_logo_as_emoji("🔖")
            client.create_typedef(cm_def)
            print("Created DaaP custom metadata structure.")
            badge = Badge.create(
                name=CM_ATTR_DAAP_SCORE,
                cm_name=CM_DAAP,
                cm_attribute=CM_ATTR_DAAP_SCORE,
                badge_conditions=[
                    BadgeCondition.create(
                        badge_condition_operator=BadgeComparisonOperator.GTE,
                        badge_condition_value="75",
                        badge_condition_colorhex=BadgeConditionColor.GREEN,
                    ),
                    BadgeCondition.create(
                        badge_condition_operator=BadgeComparisonOperator.LT,
                        badge_condition_value="75",
                        badge_condition_colorhex=BadgeConditionColor.YELLOW,
                    ),
                    BadgeCondition.create(
                        badge_condition_operator=BadgeComparisonOperator.LTE,
                        badge_condition_value="25",
                        badge_condition_colorhex=BadgeConditionColor.RED,
                    ),
                ]
            )
            try:
                client.upsert(badge)
                print("Created DaaP completeness score badge.")
            except AtlanException:
                print("Unable to create badge over DaaP score.")
            return CustomMetadataCache.get_id_for_name(CM_DAAP)
        except ConflictError:
            # Handle cross-thread race condition that the typedef has since been created
            try:
                return CustomMetadataCache.get_id_for_name(CM_DAAP)
            except AtlanException:
                print("Unable to look up DaaP custom metadata, even though it should already exist.")
        except AtlanException:
            print("Unable to create DaaP custom metadata structure.")
    except AtlanException:
        print("Unable to look up DaaP custom metadata.")


class LambdaScorer(AtlanEventHandler):
    def validate_prerequisites(self, event: AtlanEvent) -> bool:
        return (
            _create_cm_if_not_exists() is not None
            and isinstance(event.payload, AtlanEventPayload)
            and isinstance(event.payload.asset, Asset)
        )

    def get_current_state(self, from_event: Asset) -> Optional[Asset]:
        search_attrs = SCORED_ATTRS
        search_attrs.extend(CustomMetadataCache.get_attributes_for_search_results(CM_DAAP))
        print(f"Searching with: {search_attrs}")
        return get_current_view_of_asset(
            self.client,
            from_event,
            search_attrs,
            include_meanings=True,
            include_atlan_tags=True
        )

    def has_changes(self, original: Asset, modified: Asset) -> bool:
        score_original = -1.0
        score_modified = -1.0
        if cm_original := original.get_custom_metadata(CM_DAAP):
            score_original = cm_original.get(CM_ATTR_DAAP_SCORE)
        if cm_modified := modified.get_custom_metadata(CM_DAAP):
            score_modified = cm_modified.get(CM_ATTR_DAAP_SCORE)
        print(f"Existing score = {score_original}, while new score = {score_modified}")
        return score_original != score_modified

    def calculate_changes(self, asset: Asset) -> List[Asset]:
        score = 1.0

        if isinstance(asset, AtlasGlossaryTerm):
            s_description = 15 if has_description(asset) else 0
            s_related_term = 10 if asset.see_also else 0
            s_links = 10 if asset.links else 0
            s_related_asset = 20 if asset.assigned_entities else 0
            s_certificate = 0
            if asset.certificate_status == CertificateStatus.DRAFT:
                s_certificate = 15
            elif asset.certificate_status == CertificateStatus.VERIFIED:
                s_certificate = 25
            s_readme = 0
            readme = asset.readme
            if readme and readme.guid:
                readme = client.get_asset_by_guid(readme.guid, asset_type=Readme)
                if description := readme.description:
                    if len(description) > 1000:
                        s_readme = 20
                    elif len(description) > 500:
                        s_readme = 10
                    elif len(description) > 100:
                        s_readme = 5
            score = s_description + s_related_term + s_links + s_related_asset + s_certificate + s_readme
        elif not asset.type_name.startswith("AtlasGlossary"):
            # We will not score glossaries or categories
            s_description = 15 if has_description(asset) else 0
            s_owner = 20 if has_owner(asset) else 0
            s_terms = 20 if asset.assigned_terms else 0
            s_tags = 20 if asset.atlan_tags else 0
            s_lineage = 20 if has_lineage(asset) else 0
            score = s_description + s_owner + s_lineage + s_terms + s_tags

        if score >= 0:
            revised = asset.trim_to_required()
            cma = revised.get_custom_metadata(CM_DAAP)
            cma[CM_ATTR_DAAP_SCORE] = score
            return [revised] if self.has_changes(asset, revised) else []
        return []


def lambda_handler(event, context):
    process_event(LambdaScorer(client), event, context)
