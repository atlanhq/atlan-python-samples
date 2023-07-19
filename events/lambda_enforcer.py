# SPDX-License-Identifier: Apache-2.0
# Copyright 2023 Atlan Pte. Ltd.
from typing import List, Optional

from pyatlan.client.atlan import AtlanClient
from pyatlan.events.atlan_lambda_handler import process_event
from pyatlan.events.atlan_event_handler import (
    AtlanEventHandler,
    get_current_view_of_asset,
    has_description,
    has_lineage,
    has_owner,
)
from pyatlan.model.assets import Asset
from pyatlan.model.enums import CertificateStatus

REQUIRED_ATTRS = [
    "description",
    "userDescription",
    "ownerUsers",
    "ownerGroups",
    "__hasLineage",
    "inputToProcesses",
    "outputFromProcesses",
    "certificateStatus",
]
ENFORCEMENT_MESSAGE = (
    "To be verified, an asset must have a description, at least one owner, and lineage."
)
client = AtlanClient()


class LambdaEnforcer(AtlanEventHandler):
    def get_current_state(self, from_event: Asset) -> Optional[Asset]:
        return get_current_view_of_asset(self.client, from_event, REQUIRED_ATTRS)

    def calculate_changes(self, asset: Asset) -> List[Asset]:
        if asset.certificate_status == CertificateStatus.VERIFIED:
            if (
                not has_description(asset)
                or not has_owner(asset)
                or not has_lineage(asset)
            ):
                trimmed = asset.trim_to_required()
                trimmed.certificate_status = CertificateStatus.DRAFT
                trimmed.certificate_status_message = ENFORCEMENT_MESSAGE
                return [trimmed]
            else:
                print(
                    f"Asset has all required information present to be verified, no enforcement required: {asset.qualified_name}"
                )
        else:
            print(
                f"Asset is no longer verified, no enforcement action to consider: {asset.qualified_name}"
            )
        return []


def lambda_handler(event, context):
    process_event(LambdaEnforcer(client), event, context)
