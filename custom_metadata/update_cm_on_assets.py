# SPDX-License-Identifier: Apache-2.0
# Copyright 2023 Atlan Pte. Ltd.
from typing import Optional

from pyatlan.cache.custom_metadata_cache import CustomMetadataCache
from pyatlan.client.atlan import AtlanClient
from pyatlan.model.assets import Asset
from pyatlan.model.enums import AtlanConnectorType
from pyatlan.model.search import DSL, IndexSearchRequest, TermAttributes, Terms
from pyatlan.utils import get_logger

CUSTOM_METADATA_NAME = "Quality Data"
client = AtlanClient()
logger = get_logger(level="INFO")


def find_asset(
    connector_type: AtlanConnectorType,
    connection_name: str,
    asset_name: str,
    attributes: Optional[list[str]] = None,
) -> Asset:
    """
    Given a connector type and otherwise-qualified name (not including the
    connection portion of the qualified_name), finds and returns the asset in
    question.

    :param connector_type: the type of connector in which the asset can be found
    :param connection_name: the simple name of the connection
    :param asset_name: the qualified_name of the asset, not including the
        connection portion
    :param attributes: a list of attributes to retrieve for the asset
    :returns: the asset, if found
    """

    connections = client.find_connections_by_name(
        name=connection_name, connector_type=connector_type
    )
    qualified_names = []
    for connection in connections:
        qualified_names.append(f"{connection.qualified_name}/{asset_name}")
    by_name = Terms(field=TermAttributes.QUALIFIED_NAME.value, values=qualified_names)
    dsl = DSL(query=by_name)
    search_request = IndexSearchRequest(
        dsl=dsl,
        attributes=attributes,
    )
    results = client.search(search_request)
    return results.current_page()[0] if results else None


def update_custom_metadata(
    asset: Asset,
    rating: str,
    passed: int = 0,
    failed: int = 0,
    reports: Optional[list[str]] = None,
) -> Asset:
    """
    Update the custom metadata on the provided asset.

    :param asset: the asset on which to update the custom metadata
    :param rating: the overall quality rating to give the asset
    :param passed: numer of checks that passed
    :param failed: number of checks that failed
    :param reports: URLs to detailed quality reports
    :returns: the result of the update
    """

    cma = asset.get_custom_metadata(CUSTOM_METADATA_NAME)
    cma["Rating"] = rating
    cma["Passed count"] = passed
    cma["Failed count"] = failed
    cma["Detailed reports"] = reports
    to_update = asset.trim_to_required()
    to_update.set_custom_metadata(cma)
    result = client.upsert_merging_cm(to_update)
    updates = result.assets_updated(asset_type=type(asset))
    return updates[0] if updates else None


def main():
    asset = find_asset(
        connector_type=AtlanConnectorType.SNOWFLAKE,
        connection_name="development",
        asset_name="RAW/WIDEWORLDIMPORTERS_PURCHASING/SUPPLIERS",
        attributes=CustomMetadataCache.get_attributes_for_search_results(
            CUSTOM_METADATA_NAME
        ),
    )
    logger.info(f"Found asset: {asset}")
    updated = update_custom_metadata(
        asset=asset,
        rating="OK",
        passed=10,
        failed=5,
        reports=["https://www.example.com", "https://www.atlan.com"],
    )
    # Note that the updated asset will NOT show the custom metadata, if you want
    # to see the custom metadata you need to re-retrieve the asset itself
    assert updated  # noqa: S101
    result = client.get_asset_by_guid(
        guid=updated.guid, asset_type=type(updated), ignore_relationships=True
    )
    logger.info(f"Asset's custom metadata was updated: {result}")


if __name__ == "__main__":
    main()
