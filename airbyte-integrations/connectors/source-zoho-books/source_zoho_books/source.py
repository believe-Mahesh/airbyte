#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from types import MappingProxyType
import time
from abc import ABC, abstractmethod
import json
import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator
from airbyte_cdk.models import AirbyteCatalog, AirbyteMessage, ConfiguredAirbyteCatalog, Type, AirbyteStream
from .auth import ZohoOauth2Authenticator
from airbyte_cdk.sources.streams.http.requests_native_auth import Oauth2Authenticator


"""
TODO: Most comments in this class are instructive and should be deleted after the source is implemented.

This file provides a stubbed example of how to use the Airbyte CDK to develop both a source connector which supports full refresh or and an
incremental syncs from an HTTP API.

The various TODOs are both implementation hints and steps - fulfilling all the TODOs should be sufficient to implement one basic and one incremental
stream from a source. This pattern is the same one used by Airbyte internally to implement connectors.

The approach here is not authoritative, and devs are free to use their own judgement.

There are additional required TODOs in the files within the integration_tests folder and the spec.yaml file.
"""



# Basic full refresh stream
class ZohoBooksStream(HttpStream, ABC):

    _REGION_TO_API_URL = MappingProxyType(
        {
            "US": "https://www.zohoapis.com/books/v3/",
            "AU": "https://www.zohoapis.com.au/books/v3/",
            "EU": "https://www.zohoapis.eu/books/v3/",
            "IN": "https://www.zohoapis.in/books/v3/",
            "CN": "https://www.zohoapis.com.cn/books/v3/",
            "JP": "https://www.zohoapis.jp/books/v3/",
        }
    )
    
    url_base = None


    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:

        try:
            json_response = response.json()
            has_more_page = json_response.get('page_context', None).get('has_more_page', None)
            if has_more_page:
                print(f"Moving onto page - {int(json_response.get('page_context').get('page')) + 1}")
                return int(json_response.get('page_context').get('page')) + 1
            else:
                print("No more page")
                return None
        except Exception as e:
            return None

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:

        page = 1
        if next_page_token:
            page = next_page_token
        else:
            page = 1
        return {"organization_id":{self.config['organization_id']}, "page":page}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        
        return {}


class Invoices(ZohoBooksStream):

    # TODO: Fill in the primary key. Required. This is usually a unique field in the stream, like an ID or a timestamp.
    primary_key = "invoice_id"

    def __init__(self, config, **args):
        super(Invoices, self).__init__(**args)
        self.url_base = self._REGION_TO_API_URL[config['location'].upper()]
        self.config = config

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        """
        TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/customers then this
        should return "customers". Required.
        """
        return "invoices"
    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        TODO: Override this method to define how a response is parsed.
        :return an iterable containing each record in the response
        """
        
        try:
            if response.json().get('invoices') != None:
                for record in response.json()['invoices']:
                    record['line_items'] = {}
                    invoice_id = record.get('invoice_id')
                    
                    headers = { 'Authorization': f"Zoho-oauthtoken {self.config['access_token']}" }
                    
                    invoice_line = requests.get(self.url_base+f'invoices/{invoice_id}?organization_id={self.config["organization_id"]}', headers=headers)
                    data = invoice_line.json().get('invoice', None)
                    record['line_items'] = data
                    yield record

            else:
                print("No records")
                yield {}
        except Exception as e:
            print(e)

class Contacts(ZohoBooksStream):

    # TODO: Fill in the primary key. Required. This is usually a unique field in the stream, like an ID or a timestamp.
    primary_key = "contact_id"

    def __init__(self, config, **args):
        super(Contacts, self).__init__(**args)
        self.url_base = self._REGION_TO_API_URL[config['location'].upper()]

        self.config = config

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        """
        TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/customers then this
        should return "customers". Required.
        """
        return "contacts"
    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        TODO: Override this method to define how a response is parsed.
        :return an iterable containing each record in the response
        """
        
        try:
            if response.json().get('contacts') != None:
                for record in response.json()['contacts']:
                    yield record
            else:
                print("No records")
                yield {}
        except Exception as e:
            print(e)

class BankTransactions(ZohoBooksStream):

    # TODO: Fill in the primary key. Required. This is usually a unique field in the stream, like an ID or a timestamp.
    primary_key = "transaction_id"

    def __init__(self, config, **args):
        super(BankTransactions, self).__init__(**args)
        self.url_base = self._REGION_TO_API_URL[config['location'].upper()]

        self.config = config

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        """
        TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/customers then this
        should return "customers". Required.
        """
        return "banktransactions"
    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        TODO: Override this method to define how a response is parsed.
        :return an iterable containing each record in the response
        """
        
        try:
            if response.json().get('banktransactions') != None:
                for record in response.json()['banktransactions']:
                    yield record
            else:
                print("No records")
                yield {}
        except Exception as e:
            print(e)

class BankAccounts(ZohoBooksStream):

    # TODO: Fill in the primary key. Required. This is usually a unique field in the stream, like an ID or a timestamp.
    primary_key = "account_id"

    def __init__(self, config, **args):
        super(BankAccounts, self).__init__(**args)

        self.url_base = self._REGION_TO_API_URL[config['location'].upper()]
        self.config = config

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        """
        TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/customers then this
        should return "customers". Required.
        """
        return "bankaccounts"
    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        TODO: Override this method to define how a response is parsed.
        :return an iterable containing each record in the response
        """
        
        try:
            if response.json().get('bankaccounts') != None:
                for record in response.json()['bankaccounts']:
                    yield record
            else:
                print("No records")
                yield {}
        except Exception as e:
            print(e)


class Bills(ZohoBooksStream):

    # TODO: Fill in the primary key. Required. This is usually a unique field in the stream, like an ID or a timestamp.
    primary_key = "bill_id"

    def __init__(self, config, **args):
        super(Bills, self).__init__(**args)

        self.url_base = self._REGION_TO_API_URL[config['location'].upper()]
        self.config = config
    

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        """
        TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/customers then this
        should return "customers". Required.
        """
        return "bills"
    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        TODO: Override this method to define how a response is parsed.
        :return an iterable containing each record in the response
        """

        _REGION_TO_ACCESS_URL = MappingProxyType(
            {
            "US": "https://accounts.zoho.com/oauth/v2/token",
            "AU": "https://accounts.zoho.com.au/oauth/v2/token",
            "EU": "https://accounts.zoho.eu/oauth/v2/token?",
            "IN": "https://accounts.zoho.in/oauth/v2/token",
            "CN": "https://accounts.zoho.com.cn/oauth/v2/token",
            "JP": "https://accounts.zoho.jp/oauth/v2/token",
            }
        )
        
        try:
            if response.json().get('bills') != None:
                for record in response.json()['bills']:
                    record['line_items'] = {}
                    bill_id = record.get('bill_id')
                    
                    headers = { 'Authorization': f"Zoho-oauthtoken {self.config['access_token']}" }
                    
                    bill_line = requests.get(self.url_base+f'bills/{bill_id}?organization_id={self.config["organization_id"]}', headers=headers)
                    data = bill_line.json().get('bill', None)
                    record['line_items'] = data
                    yield record
            else:
                print("No records")
                yield {}
        except Exception as e:
            print(e)

class CreditNotes(ZohoBooksStream):

    # TODO: Fill in the primary key. Required. This is usually a unique field in the stream, like an ID or a timestamp.
    primary_key = "creditnote_id"

    def __init__(self, config, **args):
        super(CreditNotes, self).__init__(**args)

        self.url_base = self._REGION_TO_API_URL[config['location'].upper()]
        self.config = config

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        """
        TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/customers then this
        should return "customers". Required.
        """
        return "creditnotes"
    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        TODO: Override this method to define how a response is parsed.
        :return an iterable containing each record in the response
        """
        
        try:
            if response.json().get('creditnotes') != None:
                for record in response.json()['creditnotes']:
                    yield record
            else:
                print("No records")
                yield {}
        except Exception as e:
            print(e)


class CustomerPayments(ZohoBooksStream):

    # TODO: Fill in the primary key. Required. This is usually a unique field in the stream, like an ID or a timestamp.
    primary_key = "payment_id"

    def __init__(self, config, **args):
        super(CustomerPayments, self).__init__(**args)

        self.url_base = self._REGION_TO_API_URL[config['location'].upper()]
        self.config = config

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        """
        TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/customers then this
        should return "customers". Required.
        """
        return "customerpayments"
    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        TODO: Override this method to define how a response is parsed.
        :return an iterable containing each record in the response
        """
        
        # try:
        #     if response.json().get('customerpayments') != None:
        #         for record in response.json()['customerpayments']:
        #             yield record
        #     else:
        #         print("No records")
        #         yield {}
        # except Exception as e:
        #     print(e)

        try:
            if response.json().get('customerpayments') != None:
                for record in response.json()['customerpayments']:
                    record['line_items'] = {}
                    payment_id = record.get('payment_id')
                    
                    headers = { 'Authorization': f"Zoho-oauthtoken {self.config['access_token']}" }
                    
                    payment_line = requests.get(self.url_base+f'customerpayments/{payment_id}?organization_id={self.config["organization_id"]}', headers=headers)
                    data = payment_line.json().get('payment', None)
                    record['line_items'] = data
                    yield record
            else:
                print("No records")
                yield {}
        except Exception as e:
            print(e)

class Estimates(ZohoBooksStream):

    # TODO: Fill in the primary key. Required. This is usually a unique field in the stream, like an ID or a timestamp.
    primary_key = "estimate_id"

    def __init__(self, config, **args):
        super(Estimates, self).__init__(**args)

        self.url_base = self._REGION_TO_API_URL[config['location'].upper()]
        self.config = config

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        """
        TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/customers then this
        should return "customers". Required.
        """
        return "estimates"
    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        TODO: Override this method to define how a response is parsed.
        :return an iterable containing each record in the response
        """
        
        try:
            if response.json().get('estimates') != None:
                for record in response.json()['estimates']:
                    yield record
            else:
                print("No records")
                yield {}
        except Exception as e:
            print(e)

class Expenses(ZohoBooksStream):

    # TODO: Fill in the primary key. Required. This is usually a unique field in the stream, like an ID or a timestamp.
    primary_key = "expense_id"

    def __init__(self, config, **args):
        super(Expenses, self).__init__(**args)

        self.url_base = self._REGION_TO_API_URL[config['location'].upper()]
        self.config = config

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        """
        TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/customers then this
        should return "customers". Required.
        """
        return "expenses"
    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        TODO: Override this method to define how a response is parsed.
        :return an iterable containing each record in the response
        """
        
        try:
            if response.json().get('expenses') != None:
                for record in response.json()['expenses']:
                    yield record
            else:
                print("No records")
                yield {}
        except Exception as e:
            print(e)

class Items(ZohoBooksStream):

    # TODO: Fill in the primary key. Required. This is usually a unique field in the stream, like an ID or a timestamp.
    primary_key = "item_id"

    def __init__(self, config, **args):
        super(Items, self).__init__(**args)

        self.url_base = self._REGION_TO_API_URL[config['location'].upper()]
        self.config = config

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        """
        TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/customers then this
        should return "customers". Required.
        """
        return "items"
    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        TODO: Override this method to define how a response is parsed.
        :return an iterable containing each record in the response
        """
        
        try:
            if response.json().get('items') != None:
                for record in response.json()['items']:
                    yield record
            else:
                print("No records")
                yield {}
        except Exception as e:
            print(e)

class Journals(ZohoBooksStream):

    # TODO: Fill in the primary key. Required. This is usually a unique field in the stream, like an ID or a timestamp.
    primary_key = "journal_id"

    def __init__(self, config, **args):
        super(Journals, self).__init__(**args)

        self.url_base = self._REGION_TO_API_URL[config['location'].upper()]
        self.config = config

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        """
        TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/customers then this
        should return "customers". Required.
        """
        return "journals"
    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        TODO: Override this method to define how a response is parsed.
        :return an iterable containing each record in the response
        """
        
        try:
            if response.json().get('journals') != None:
                for record in response.json()['journals']:
                    record['line_items'] = {}
                    journal_id = record.get('journal_id')
                    
                    headers = { 'Authorization': f"Zoho-oauthtoken {self.config['access_token']}" }
                    
                    journal_line = requests.get(self.url_base+f'journals/{journal_id}?organization_id={self.config["organization_id"]}', headers=headers)
                    data = journal_line.json().get('journal', None)
                    record['line_items'] = data
                    yield record
            else:
                print("No records")
                yield {}
        except Exception as e:
            print(e)

class PurchaseOrders(ZohoBooksStream):

    # TODO: Fill in the primary key. Required. This is usually a unique field in the stream, like an ID or a timestamp.
    primary_key = "purchaseorder_id"

    def __init__(self, config, **args):
        super(PurchaseOrders, self).__init__(**args)

        self.url_base = self._REGION_TO_API_URL[config['location'].upper()]
        self.config = config

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        """
        TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/customers then this
        should return "customers". Required.
        """
        return "purchaseorders"
    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        TODO: Override this method to define how a response is parsed.
        :return an iterable containing each record in the response
        """
        
        try:
            if response.json().get('purchaseorders') != None:
                for record in response.json()['purchaseorders']:
                    yield record
            else:
                print("No records")
                yield {}
        except Exception as e:
            print(e)

class SalesOrders(ZohoBooksStream):

    # TODO: Fill in the primary key. Required. This is usually a unique field in the stream, like an ID or a timestamp.
    primary_key = "salesorder_id"

    def __init__(self, config, **args):
        super(SalesOrders, self).__init__(**args)

        self.url_base = self._REGION_TO_API_URL[config['location'].upper()]
        self.config = config

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        """
        TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/customers then this
        should return "customers". Required.
        """
        return "salesorders"
    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        TODO: Override this method to define how a response is parsed.
        :return an iterable containing each record in the response
        """
        
        try:
            if response.json().get('salesorders') != None:
                for record in response.json()['salesorders']:
                    yield record
            else:
                print("No records")
                yield {}
        except Exception as e:
            print(e)

class Taxes(ZohoBooksStream):

    # TODO: Fill in the primary key. Required. This is usually a unique field in the stream, like an ID or a timestamp.
    primary_key = "tax_id"

    def __init__(self, config, **args):
        super(Taxes, self).__init__(**args)

        self.url_base = self._REGION_TO_API_URL[config['location'].upper()]
        self.config = config

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        """
        TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/customers then this
        should return "customers". Required.
        """
        return "settings/taxes"
    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        TODO: Override this method to define how a response is parsed.
        :return an iterable containing each record in the response
        """
        
        try:
            if response.json().get('taxes') != None:
                for record in response.json()['taxes']:
                    yield record
            else:
                print("No records")
                yield {}
        except Exception as e:
            print(e)

class Users(ZohoBooksStream):

    # TODO: Fill in the primary key. Required. This is usually a unique field in the stream, like an ID or a timestamp.
    primary_key = "user_id"

    def __init__(self, config, **args):
        super(Users, self).__init__(**args)

        self.url_base = self._REGION_TO_API_URL[config['location'].upper()]
        self.config = config

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        """
        TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/customers then this
        should return "customers". Required.
        """
        return "users"
    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        TODO: Override this method to define how a response is parsed.
        :return an iterable containing each record in the response
        """
        
        try:
            if response.json().get('users') != None:
                for record in response.json()['users']:
                    yield record
            else:
                print("No records")
                yield {}
        except Exception as e:
            print(e)

class VendorCredits(ZohoBooksStream):

    # TODO: Fill in the primary key. Required. This is usually a unique field in the stream, like an ID or a timestamp.
    primary_key = "vendor_credit_id"

    def __init__(self, config, **args):
        super(VendorCredits, self).__init__(**args)

        self.url_base = self._REGION_TO_API_URL[config['location'].upper()]
        self.config = config

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        """
        TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/customers then this
        should return "customers". Required.
        """
        return "vendorcredits"
    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        TODO: Override this method to define how a response is parsed.
        :return an iterable containing each record in the response
        """
        try:
            if response.json().get('vendorcredits') != None:
                for record in response.json()['vendorcredits']:
                    yield record
            else:
                print("No records")
                yield {}
        except Exception as e:
            print(e)

class VendorPayments(ZohoBooksStream):

    # TODO: Fill in the primary key. Required. This is usually a unique field in the stream, like an ID or a timestamp.
    primary_key = "payment_id"

    def __init__(self, config, **args):
        super(VendorPayments, self).__init__(**args)

        self.url_base = self._REGION_TO_API_URL[config['location'].upper()]
        self.config = config

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        """
        TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/customers then this
        should return "customers". Required.
        """
        return "vendorpayments"
    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        TODO: Override this method to define how a response is parsed.
        :return an iterable containing each record in the response
        """
        
        # try:
        #     if response.json().get('vendorpayments') != None:
        #         for record in response.json()['vendorpayments']:
        #             yield record
        #     else:
        #         print("No records")
        #         yield {}
        # except Exception as e:
        #     print(e)

        try:
            if response.json().get('vendorpayments') != None:
                for record in response.json()['vendorpayments']:
                    record['line_items'] = {}
                    payment_id = record.get('payment_id')
                    
                    headers = { 'Authorization': f"Zoho-oauthtoken {self.config['access_token']}" }
                    
                    payment_line = requests.get(self.url_base+f'vendorpayments/{payment_id}?organization_id={self.config["organization_id"]}', headers=headers)
                    data = payment_line.json().get('vendorpayment', None)
                    record['line_items'] = data
                    yield record
            else:
                print("No records")
                yield {}
        except Exception as e:
            print(e)

class ChartOfAccounts(ZohoBooksStream):

    # TODO: Fill in the primary key. Required. This is usually a unique field in the stream, like an ID or a timestamp.
    primary_key = "account_id"

    def __init__(self, config, **args):
        super(ChartOfAccounts, self).__init__(**args)

        self.url_base = self._REGION_TO_API_URL[config['location'].upper()]
        self.config = config

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        """
        TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/customers then this
        should return "customers". Required.
        """
        return "chartofaccounts"
    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        TODO: Override this method to define how a response is parsed.
        :return an iterable containing each record in the response
        """
        
        try:
            if response.json().get('chartofaccounts') != None:
                for record in response.json()['chartofaccounts']:
                    yield record
            else:
                print("No records")
                yield {}
        except Exception as e:
            print(e)


# Basic incremental stream
class IncrementalZohoBooksStream(ZohoBooksStream, ABC):
    """
    TODO fill in details of this class to implement functionality related to incremental syncs for your connector.
         if you do not need to implement incremental sync for any streams, remove this class.
    """

    # TODO: Fill in to checkpoint stream reads after N records. This prevents re-reading of data if the stream fails for any reason.
    state_checkpoint_interval = None

    @property
    def cursor_field(self) -> str:
        """
        TODO
        Override to return the cursor field used by this stream e.g: an API entity might always use created_at as the cursor field. This is
        usually id or date based. This field's presence tells the framework this in an incremental stream. Required for incremental.

        :return str: The name of the cursor field.
        """
        return []

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        """
        Override to determine the latest state after reading the latest record. This typically compared the cursor_field from the latest record and
        the current state and picks the 'most' recent cursor. This is how a stream's state is determined. Required for incremental.
        """
        return {}


# class Employees(IncrementalZohoBooksStream):
#     """
#     TODO: Change class name to match the table/data source this stream corresponds to.
#     """

#     # TODO: Fill in the cursor_field. Required.
#     cursor_field = "start_date"

#     # TODO: Fill in the primary key. Required. This is usually a unique field in the stream, like an ID or a timestamp.
#     primary_key = "employee_id"

#     def path(self, **kwargs) -> str:
#         """
#         TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/employees then this should
#         return "single". Required.
#         """
#         return "employees"

#     def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
#         """
#         TODO: Optionally override this method to define this stream's slices. If slicing is not needed, delete this method.

#         Slices control when state is saved. Specifically, state is saved after a slice has been fully read.
#         This is useful if the API offers reads by groups or filters, and can be paired with the state object to make reads efficient. See the "concepts"
#         section of the docs for more information.

#         The function is called before reading any records in a stream. It returns an Iterable of dicts, each containing the
#         necessary data to craft a request for a slice. The stream state is usually referenced to determine what slices need to be created.
#         This means that data in a slice is usually closely related to a stream's cursor_field and stream_state.

#         An HTTP request is made for each returned slice. The same slice can be accessed in the path, request_params and request_header functions to help
#         craft that specific request.

#         For example, if https://example-api.com/v1/employees offers a date query params that returns data for that particular day, one way to implement
#         this would be to consult the stream state object for the last synced date, then return a slice containing each date from the last synced date
#         till now. The request_params function would then grab the date from the stream_slice and make it part of the request by injecting it into
#         the date query param.
#         """
#         raise NotImplementedError("Implement stream slices or delete this method!")


# Source
class SourceZohoBooks(AbstractSource):

    # @staticmethod
    # def _get_bill_ids(authenticator: str = None, config: str = None) -> Mapping[str, str]:
    #     """
    #     Most of the Webflow APIs require the collection id, but the streams that we are generating use the collection name.
    #     This function will return a dictionary containing collection_name: collection_id entries.
    #     """

    #     bills = []

    #     bills_stream = Bills(authenticator=authenticator, config=config)
    #     bills_record = bills_stream.read_records(sync_mode="full_refresh")
    #     # Loop over the list of records and create a dictionary with name as key, and _id as value
    #     try:
    #         for collection_obj in list(bills_record):
    #             bills.append(collection_obj["bill_id"])
    #         return bills
    #     except Exception as e:
    #         print(e)

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        """
        TODO: Implement a connection check to validate that the user-provided config can be used to connect to the underlying API

        See https://github.com/airbytehq/airbyte/blob/master/airbyte-integrations/connectors/source-stripe/source_stripe/source.py#L232
        for an example.

        :param config:  the user-input config object conforming to the connector's spec.yaml
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """
        locations = ['IN', 'EU', 'US', 'JP', 'AU']
        input_location = config['location']
        if input_location not in locations:
            return False, f"Input location {input_location} is invalid. Please input one of the following locations: {locations}"
        else:
            return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        define streams here
        """
        _REGION_TO_ACCESS_URL = MappingProxyType(
            {
            "US": "https://accounts.zoho.com/oauth/v2/token",
            "AU": "https://accounts.zoho.com.au/oauth/v2/token",
            "EU": "https://accounts.zoho.eu/oauth/v2/token?",
            "IN": "https://accounts.zoho.in/oauth/v2/token",
            "CN": "https://accounts.zoho.com.cn/oauth/v2/token",
            "JP": "https://accounts.zoho.jp/oauth/v2/token",
            }
        )
        
        client_id = config.get('client_id', None)
        client_secret = config.get('client_secret', None)
        refresh_token = config.get('refresh_token', None)
        location = config.get('location', None)
        token_refresh_endpoint = _REGION_TO_ACCESS_URL[config["location"].upper()]
        config['access_token'] = ''

        auth = ZohoOauth2Authenticator(token_refresh_endpoint, client_id, client_secret, refresh_token, location)
        
        config['access_token'] = auth.get_access_token()
        stream_endpoint = [Invoices(authenticator=auth, config=config),
                Contacts(authenticator=auth, config=config),
                BankAccounts(authenticator=auth, config=config),
                BankTransactions(authenticator=auth, config=config),
                Bills(authenticator=auth, config=config),
                CustomerPayments(authenticator=auth, config=config),
                CreditNotes(authenticator=auth, config=config),
                Estimates(authenticator=auth, config=config),
                Expenses(authenticator=auth, config=config),
                Items(authenticator=auth, config=config),
                Journals(authenticator=auth, config=config),
                PurchaseOrders(authenticator=auth, config=config),
                SalesOrders(authenticator=auth, config=config),
                Taxes(authenticator=auth, config=config),
                VendorCredits(authenticator=auth, config=config),
                Users(authenticator=auth, config=config),
                VendorPayments(authenticator=auth, config=config),
                ChartOfAccounts(authenticator=auth, config=config)]
        
        
        return stream_endpoint