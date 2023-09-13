class ZapItem:
    """
    Zap Imoveis listing object
    """
    def __init__(self, listing):
        self.price = int(listing['listing']['pricingInfos'][0].get('price', None)) if len(
            listing['listing']['pricingInfos']) > 0 else 0
        self.condo_fee = int(listing['listing']['pricingInfos'][0].get('monthlyCondoFee', 0)) if len(
            listing['listing']['pricingInfos']) > 0 else 0
        self.bedrooms = int(listing['listing']['bedrooms'][0] if len(listing['listing']['bedrooms']) > 0 else 0)
        self.bathrooms = int(listing['listing']['bathrooms'][0] if len(listing['listing']['bathrooms']) > 0 else 0)
        self.vacancies = listing['listing']['parkingSpaces'][0] if len(listing['listing']['parkingSpaces']) > 0 else 0
        self.total_area_m2 = int(
            listing['listing']['usableAreas'][0] if len(listing['listing']['usableAreas']) > 0 else 0)
        self.price_per_area = self.price / self.total_area_m2
        self.address = (listing['link']['data']['street'] + ", " + listing['link']['data']['neighborhood']).strip(
            ',').strip()
        self.description = listing['listing']['title']
        self.link = 'https://www.zapimoveis.com.br' + listing['link']['href']