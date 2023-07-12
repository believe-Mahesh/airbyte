#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
import logging
from types import MappingProxyType
from typing import Any, List, Mapping, MutableMapping, Tuple, Dict
from urllib.parse import urlsplit, urlunsplit

import requests
from airbyte_cdk.sources.streams.http.requests_native_auth import Oauth2Authenticator

logger = logging.getLogger(__name__)


class ZohoOauth2Authenticator(Oauth2Authenticator):

    _DC_REGION_TO_ACCESS_URL = MappingProxyType(
        {
            "US": "https://accounts.zoho.com",
            "AU": "https://accounts.zoho.com.au",
            "EU": "https://accounts.zoho.eu",
            "IN": "https://accounts.zoho.in",
            "CN": "https://accounts.zoho.com.cn",
            "JP": "https://accounts.zoho.jp",
        }
    )
    _DC_REGION_TO_API_URL = MappingProxyType(
        {
            "US": "https://zohoapis.com",
            "AU": "https://zohoapis.com.au",
            "EU": "https://zohoapis.eu",
            "IN": "https://zohoapis.in",
            "CN": "https://zohoapis.com.cn",
            "JP": "https://zohoapis.jp",
        }
    )
    _API_ENV_TO_URL_PREFIX = MappingProxyType({"production": "", "developer": "developer", "sandbox": "sandbox"})
    _CONCURRENCY_API_LIMITS = MappingProxyType({"Free": 5, "Standard": 10, "Professional": 15, "Enterprise": 20, "Ultimate": 25})



    def _prepare_refresh_token_params(self) -> Dict[str, str]:
        return {
            "refresh_token": self.get_refresh_token(),
            "client_id": self.get_client_id(),
            "client_secret": self.get_client_secret(),
            "grant_type": "refresh_token",
        }

    def get_auth_header(self) -> Mapping[str, Any]:
        token = self.get_access_token()
        return {"Authorization": f"Zoho-oauthtoken {token}"}

    def refresh_access_token(self) -> Tuple[str, int]:
        """
        This method is overridden because token parameters should be passed via URL params, not via the request payload.
        Returns a tuple of (access_token, token_lifespan_in_seconds)
        """
        try:
            response = requests.request(method="POST", url=self.get_token_refresh_endpoint(), params=self._prepare_refresh_token_params())
            response.raise_for_status()
            response_json = response.json()
            print(response.json())
            return response_json[self.get_access_token_name()], response_json[self.get_expires_in_name()]
        except Exception as e:
            raise Exception(f"Error while refreshing access token: {e}") from e

    @property
    def _access_url(self) -> str:
        print(self._DC_REGION_TO_ACCESS_URL[self.config["dc_region"].upper()])
        return self._DC_REGION_TO_ACCESS_URL[self.config["dc_region"].upper()]

    @property
    def max_concurrent_requests(self) -> int:
        return self._CONCURRENCY_API_LIMITS[self.config["edition"]]

    @property
    def api_url(self) -> str:
        schema, domain, *_ = urlsplit(self._DC_REGION_TO_API_URL[self.config["dc_region"].upper()])
        prefix = self._API_ENV_TO_URL_PREFIX[self.config["environment"].lower()]
        if prefix:
            domain = f"{prefix}.{domain}"
        return urlunsplit((schema, domain, *_))

    def _json_from_path(self, path: str, key: str, params: MutableMapping[str, str] = None) -> List[MutableMapping[Any, Any]]:
        response = requests.get(url=f"{self.api_url}{path}", headers=self.authenticator.get_auth_header(), params=params or {})
        logger.info(f"{self.api_url}{path}")
        if response.status_code == 204:
            # Zoho CRM returns `No content` for Metadata of some modules
            logger.warning(f"{key.capitalize()} Metadata inaccessible: {response.content} [HTTP status {response.status_code}]")
            return []
        return response.json()[key]

