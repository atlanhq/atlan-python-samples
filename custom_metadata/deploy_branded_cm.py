# SPDX-License-Identifier: Apache-2.0
# Copyright 2023 Atlan Pte. Ltd.
from pyatlan.cache.custom_metadata_cache import CustomMetadataCache
from pyatlan.cache.enum_cache import EnumCache
from pyatlan.client.atlan import AtlanClient
from pyatlan.error import NotFoundError
from pyatlan.model.assets import Badge
from pyatlan.model.enums import (
    AtlanCustomAttributePrimitiveType,
    BadgeComparisonOperator,
    BadgeConditionColor,
)
from pyatlan.model.structs import BadgeCondition
from pyatlan.model.typedef import AttributeDef, CustomMetadataDef, EnumDef
from pyatlan.utils import get_logger

ENUM_NAME = "QDRating"
CUSTOM_METADATA_NAME = "Quality Data"

client = AtlanClient()
logger = get_logger(level="INFO")


def create_custom_metadata_options():
    """
    Create custom metadata options (enumeration), if it does not yet exist.
    """
    if EnumCache.get_by_name(ENUM_NAME):
        logger.info(f"{ENUM_NAME} enumeration has already been created.")
        return

    enum_def = EnumDef.create(name=ENUM_NAME, values=["Poor", "OK", "Great"])
    response = client.create_typedef(enum_def)
    assert response  # noqa: S101
    logger.info(f"{ENUM_NAME} enumeration created.")


def create_custom_metadata_structure():
    """
    Create a logo-branded custom metadata structure, if it does not yet exist.
    """
    try:
        CustomMetadataCache.get_id_for_name(CUSTOM_METADATA_NAME)
        logger.info(
            f"{CUSTOM_METADATA_NAME} custom metadata structure has "
            f"already been created."
        )
    except NotFoundError:
        cm_def = CustomMetadataDef.create(display_name=CUSTOM_METADATA_NAME)
        cm_def.attribute_defs = [
            AttributeDef.create(
                display_name="Rating",
                attribute_type=AtlanCustomAttributePrimitiveType.OPTIONS,
                options_name=ENUM_NAME,
            ),
            AttributeDef.create(
                display_name="Passed count",
                attribute_type=AtlanCustomAttributePrimitiveType.INTEGER,
            ),
            AttributeDef.create(
                display_name="Failed count",
                attribute_type=AtlanCustomAttributePrimitiveType.INTEGER,
            ),
            AttributeDef.create(
                display_name="Detailed reports",
                attribute_type=AtlanCustomAttributePrimitiveType.URL,
                multi_valued=True,
            ),
        ]
        cm_def.options = CustomMetadataDef.Options.with_logo_from_url(
            url="http://assets.atlan.com/assets/atlan-a-logo-blue-background.png"
        )
        result = client.create_typedef(cm_def)
        assert result  # noqa: S101
        logger.info(f"{CUSTOM_METADATA_NAME} custom metadata structure created.")


def create_badge():
    """
    Create a badge for the enumerated values of the "Rating" attribute in the
    custom metadata structure.
    """
    badge = Badge.create(
        name="Rating",
        cm_name=CUSTOM_METADATA_NAME,
        cm_attribute="Rating",
        badge_conditions=[
            BadgeCondition.create(
                badge_condition_operator=BadgeComparisonOperator.EQ,
                badge_condition_value='"Great"',
                badge_condition_colorhex=BadgeConditionColor.GREEN,
            ),
            BadgeCondition.create(
                badge_condition_operator=BadgeComparisonOperator.EQ,
                badge_condition_value='"OK"',
                badge_condition_colorhex=BadgeConditionColor.YELLOW,
            ),
            BadgeCondition.create(
                badge_condition_operator=BadgeComparisonOperator.EQ,
                badge_condition_value='"Poor"',
                badge_condition_colorhex=BadgeConditionColor.RED,
            ),
        ],
    )
    badge.user_description = "Overall quality rating for the asset."
    response = client.upsert(badge)
    assert response.assets_created(asset_type=Badge)  # noqa: S101
    logger.info(f"Badge for {CUSTOM_METADATA_NAME}:Rating created / updated.")


def main():
    create_custom_metadata_options()
    create_custom_metadata_structure()
    create_badge()


if __name__ == "__main__":
    main()
