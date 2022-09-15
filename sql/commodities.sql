SELECT
    *
FROM
    prices
    JOIN commodities ON commodities.guid = prices.commodity_guid
WHERE
    commodities.fullname IN ('Yellow Corn', 'Soybeans')
    AND prices.type = 'bid'