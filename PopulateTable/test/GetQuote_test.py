import unittest
from PopulateTable.src.populatetable import CryptoPricing

class TestQuoteAPI(unittest.TestCase):

    def test_GetLatestQuote(self):
      CryptoPricing.CryptoPricing('./PopulateTable/config/config.toml').GetLatestPrice()