# SPDX-License-Identifier: Apache-2.0
# Copyright 2023 Atlan Pte. Ltd.
from pyatlan.client.atlan import AtlanClient
from pyatlan.model.assets import Asset, SigmaWorkbook
from pyatlan.model.enums import (
    AtlanComparisonOperator,
    CertificateStatus,
    LineageDirection,
)
from pyatlan.model.fluent_search import FluentSearch
from pyatlan.model.lineage import EntityFilter, FilterList, LineageListRequest
from pyatlan.utils import get_logger

client = AtlanClient()
logger = get_logger(level="INFO")


def find_all(asset_type: type) -> AtlanClient.SearchResults:
    """
    This query will find all assets of the specified type
    that are active (not archived or soft-deleted).

    :param asset_type: type of assets to find
    :returns: results of the search
    """
    search_request = (
        FluentSearch()
        .where(FluentSearch.asset_type(asset_type))
        .where(FluentSearch.active_assets())
        .page_size(100)
    ).to_request()
    return client.search(search_request)


def upstream_certified_sources(guid: str) -> list[Asset]:
    """
    Given the GUID of an asset, this will look for all upstream assets in
    lineage that have a certificate of VERIFIED. If any Table, View or
    MaterialisedView is found that is VERIFIED, this function will return a
    list of all such assets; otherwise it will return an empty list.

    :param guid: unique identifier (GUID) of the asset from which to start
    :returns: all upstream assets with a VERIFIED certificate
    """
    request = LineageListRequest.create(guid=guid)
    request.depth = 1000000
    request.direction = LineageDirection.UPSTREAM
    request.offset = 0
    request.size = 100
    request.attributes = [
        Asset.NAME.atlan_field_name,
        Asset.CERTIFICATE_STATUS.atlan_field_name,
    ]
    request.entity_filters = FilterList(
        condition="AND",
        criteria=[
            EntityFilter(
                attribute_name=Asset.CERTIFICATE_STATUS.atlan_field_name,
                operator=AtlanComparisonOperator.CONTAINS,
                attribute_value=CertificateStatus.VERIFIED.value,
            )
        ],
    )
    response = client.get_lineage_list(request)
    verified_assets: list[Asset] = []
    for asset in response:
        if asset.type_name in {"Table", "View", "MaterialisedView"}:
            verified_assets.append(asset)
    return verified_assets


def main():
    results = find_all(SigmaWorkbook)
    for workbook in results:
        if isinstance(workbook, SigmaWorkbook):
            verified_sources = upstream_certified_sources(workbook.guid)
            if verified_sources:
                logger.info(
                    f"Workbook '{workbook.name}' ({workbook.guid}) "
                    f"has upstream verified sources: "
                )
                for asset in verified_sources:
                    logger.info(f" . {asset.type_name}: {asset.qualified_name}")
            else:
                logger.info(
                    f"Workbook '{workbook.name}' ({workbook.guid}) does "
                    f"NOT have any upstream verified sources."
                )


if __name__ == "__main__":
    main()
