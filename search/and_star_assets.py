# SPDX-License-Identifier: Apache-2.0
# Copyright 2023 Atlan Pte. Ltd.
import time

from pyatlan.client.atlan import AtlanClient, IndexSearchRequest
from pyatlan.model.assets import Asset, AtlasGlossaryTerm
from pyatlan.model.search import DSL, Term
from pyatlan.model.structs import StarredDetails
from pyatlan.utils import get_logger

client = AtlanClient()
logger = get_logger(level="INFO")


def find_assets() -> AtlanClient.SearchResults:
    """
    In this method you would code the logic for determining which assets
    you want to star. (This example will start all glossary terms in the
    "Metrics" glossary.)

    :returns: results of the search
    """
    are_active = Term.with_state("ACTIVE")
    are_term = Term.with_type_name("AtlasGlossaryTerm")
    glossary = client.find_glossary_by_name("Metrics")
    in_glossary = Term.with_glossary(glossary.qualified_name)
    dsl = DSL(query=are_active + are_term + in_glossary, from_=0, size=100)
    search_request = IndexSearchRequest(
        dsl=dsl, attributes=["starredDetailsList", "starredBy", "anchor"]
    )
    return client.search(search_request)


def list_users_in_group(name: str) -> list[str]:
    """
    Given the name of a group in Atlan, return a list of all the usernames
    of users in that group.

    :param name: human-readable name of the group in Atlan
    :returns: list of all the usernames of users in that group in Atlan
    """
    usernames = []
    if groups := client.get_group_by_name(alias=name):
        if response := client.get_group_members(guid=groups[0].id):
            if response.records and len(response.records) > 0:
                for user in response.records:
                    usernames.append(user.username)
    return usernames


def star_asset(asset: Asset, usernames: list[str]) -> None:
    """
    Given an asset and a list of usernames, ensure all the users listed
    have starred the asset.

    :param asset: to be starred
    :param usernames: to ensure have starred the asset
    :return: nothing (void)
    """
    now = round(time.time() * 1000)
    starred_details_list = asset.starred_details_list
    starred_count = len(starred_details_list)
    starred_by = asset.starred_by
    for user in usernames:
        if user not in starred_by:
            starred_by.add(user)
            starred_count += 1
            starred_details_list.append(
                StarredDetails(asset_starred_by=user, asset_starred_at=now)
            )
    to_update = asset.trim_to_required()
    to_update.starred_details_list = starred_details_list
    to_update.starred_count = starred_count
    to_update.starred_by = starred_by
    logger.info(
        f"Updating '{asset.name}' ({asset.guid}) with total stars: {starred_count}"
    )
    client.save(to_update)


def main():
    assets = find_assets()
    usernames = list_users_in_group("Admins")
    for asset in assets:
        if isinstance(asset, AtlasGlossaryTerm):
            star_asset(asset, usernames)


if __name__ == "__main__":
    main()
