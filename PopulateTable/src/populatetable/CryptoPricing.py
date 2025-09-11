import requests as r
import polars as pl 
from io import BytesIO, StringIO
from functools import wraps
import boto3
import tomllib

class CryptoPricing:
    def __init__(self, ConfigFilePath: str) -> None:
        with open(ConfigFilePath, "rb") as configFile:
            configData = tomllib.load(configFile)
            configFile.close()
        self.DestinationBucket = str(configData.get("destination").get("blobstore")) # type: ignore
        self.PricingAPI = str(configData.get("crypto").get("api").get("url")) # type: ignore
        self.EndpointURL = str(configData.get("destination").get("filesystem").get("endpoint_url")) # type: ignore
        self.AWSKey = str(configData.get('destination').get('filesystem').get("aws_access_key_id")) #type: ignore
        self.AWSSecret = str(configData.get('destination').get('filesystem').get("aws_secret_access_key")) #type: ignore
        self.AWSRegion = str(configData.get('destination').get('filesystem').get("region_name")) #type: ignore

    @staticmethod
    def WriteLatestPrice(func):
        @wraps(func)
        def wrapper(self): 
            r2_client = boto3.client(
                's3',
                region_name=self.AWSRegion,
                endpoint_url=self.EndpointURL,
                aws_access_key_id=self.AWSKey,
                aws_secret_access_key=self.AWSSecret,
                verify = False
            )
            buffer = BytesIO()
            returnSet, unixTimestamp = func(self)
            returnSet.write_parquet(buffer)
            buffer.seek(0)

            r2_client.put_object(
                    Bucket = self.DestinationBucket,
                    Key = f'{unixTimestamp}.parquet',
                    Body = buffer.getvalue()
            )
        return wrapper
            
    @staticmethod
    def LocalParquet(func):
        @wraps(func)
        def wrapper(self):
            returnSet, unixTimestamp = func(self)
            returnSet.write_parquet(rf'./{unixTimestamp}.parquet')
        return wrapper


    @WriteLatestPrice
    def GetLatestPrice(self):
        session = r.Session()
        quoteURL = self.PricingAPI
        response = session.get(quoteURL, verify=False)
        responseStream = StringIO(response.text)
        
        BTCpricingDF = pl.read_json(responseStream).unnest('Data').unnest(['BTC-USD']).with_columns(pl.lit("BTC").alias("COIN"))
        ETHpricingDF = pl.read_json(responseStream).unnest('Data').unnest(['ETH-USD']).with_columns(pl.lit("ETH").alias("COIN"))
        XRPpricingDF = pl.read_json(responseStream).unnest('Data').unnest(['XRP-USD']).with_columns(pl.lit("XRP").alias("COIN"))

        pricingDF = pl.concat([
            BTCpricingDF.select([pl.col("COIN"), pl.col("VALUE"), pl.col("VALUE_LAST_UPDATE_TS")]), 
            ETHpricingDF.select([pl.col("COIN"), pl.col("VALUE"), pl.col("VALUE_LAST_UPDATE_TS")]), 
            XRPpricingDF.select([pl.col("COIN"), pl.col("VALUE"), pl.col("VALUE_LAST_UPDATE_TS")])], 
            how='align'
        )
        currentPrice = pl.DataFrame({
            "COIN_ID": [1,2,3],
            "COIN": pricingDF.select(pl.col("COIN")).to_series(),
            "COIN_NAME": ['BITCOIN', 'ETHERIUM', 'XRP LEDGER'],
            "price": pricingDF.select(pl.col('VALUE')).to_series(),
            "price_time": pl.from_epoch(pricingDF.select(pl.col("VALUE_LAST_UPDATE_TS")).to_series())
        })
    
        return currentPrice, pricingDF.select(pl.col("VALUE_LAST_UPDATE_TS")).to_series().first()