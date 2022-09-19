SELECT
    *
FROM
    prices
    JOIN commodities ON commodities.guid = prices.commodity_guid
