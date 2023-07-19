# SPDX-License-Identifier: Apache-2.0
# Copyright 2023 Atlan Pte. Ltd.
from pyatlan.client.atlan import AtlanClient
from pyatlan.model.assets import SigmaWorkbook, Asset
from pyatlan.model.enums import LineageDirection, AtlanComparisonOperator
from pyatlan.model.lineage import LineageListRequest, FilterList, EntityFilter
from pyatlan.client.atlan import IndexSearchRequest
from pyatlan.model.search import DSL, Term
from pyatlan.utils import get_logger

client = AtlanClient()
logger = get_logger(level="INFO")


def find_all(type_name: str):
    """
    This query will find all assets of the specified type
    that are active (not archived or soft-deleted).
    """
    are_active = Term.with_state("ACTIVE")
    are_term = Term.with_type_name(type_name)
    dsl = DSL(query=are_active + are_term, from_=0, size=100)
    search_request = IndexSearchRequest(dsl=dsl)
    return client.search(search_request)


def upstream_certified_sources(guid: str) -> list[Asset]:
    """
    Given the GUID of an asset, this will look for all upstream assets in lineage that
    have a certificate of VERIFIED. If any Table, View or MaterialisedView is found that
    is VERIFIED, this function will return a list of all such assets; otherwise
    it will return an empty list.
    """
    request = LineageListRequest.create(guid=guid)
    request.depth = 1000000
    request.direction = LineageDirection.UPSTREAM
    request.offset = 0
    request.size = 100
    request.attributes = ["name", "certificateStatus"]
    request.entity_filters = FilterList(
        condition="AND",
        criteria=[
            EntityFilter(
                attribute_name="certificateStatus",
                operator=AtlanComparisonOperator.CONTAINS,
                attribute_value="VERIFIED",
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
    results = find_all(type_name="SigmaWorkbook")
    for workbook in results:
        if isinstance(workbook, SigmaWorkbook):
            verified_sources = upstream_certified_sources(workbook.guid)
            if verified_sources:
                logger.info(
                    f"Workbook '{workbook.name}' ({workbook.guid}) has upstream verified sources: "
                )
                for asset in verified_sources:
                    logger.info(f" ... {asset.type_name}: {asset.qualified_name}")
            else:
                logger.info(
                    f"Workbook '{workbook.name}' ({workbook.guid}) does NOT have any upstream verified sources."
                )


if __name__ == "__main__":
    main()
