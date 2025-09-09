from populatetable import CryptoPricing
import time

def main():
    cryptoPrice = CryptoPricing.CryptoPricing('./PopulateTable/config/config.toml')
    while True:
        cryptoPrice.GetLatestPrice()
        time.sleep(30)


if __name__ == '__main__':
    main()

