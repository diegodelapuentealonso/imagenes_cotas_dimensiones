-- Querry to get product images from BigQuery
SELECT productReferenceBu, t.type, media_item.type, media_item.url, media_item.label
FROM `opus-prod-lmes.lmes_opus_product.adeo_prod_europe_west1_APP_OPUS_CONTENT_LM_ES_P1_C3_PRODUCT_CORE_MEDIA_V1_com_adeo_CatalogsBroadcast_ProductCoreMediaValue` as t,
UNNEST(media) AS media_item
WHERE TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) = TIMESTAMP("2025-09-19") and media_item.type = "photo"


--Querry to get product characteristics from OPUS
SELECT 
    productReferenceBu, att.code, modelCode, att.name as name_char, d.value.double as value_char
FROM `opus-prod-lmes.lmes_opus_product.adeo_prod_europe_west1_APP_OPUS_CONTENT_LM_ES_P1_C3_PRODUCT_CORE_ATTRIBUTE_V1_com_adeo_CatalogsBroadcast_ProductCoreAttributeValue`,
    UNNEST(attributes) AS att,
    UNNEST(att.data) AS d
WHERE TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) 
  BETWEEN TIMESTAMP("2025-09-19") AND TIMESTAMP("2025-12-20") and
 att.code in("00053","00054", "00055")  and productReferenceBu = cast({reference} as STRING)


--Querry to get product characteristics from STEP
-- Format, open_or_closed and name of the char
    with metadata_char as (
        Select
            productCharacteristicIdentifier cod_char, productCharacteristicName as name_char, 
            productCharacteristicFormat as format_char, productCharacteristicMask as numeric_format_char,
            productCharacteristicListOfValue as open_or_closed
        From 
            `dfdp-pdp-prod-master-dat-prod.collected_product_metadata.productCharacteristic` 
        Where
            languageAlpha2Code = "ES" and productCharacteristicIdentifier IN
            -- Pensar bien que características queremos para las cotas
            ("ATT_00053", "ATT_00054", "ATT_00055", "ATT_00145", "ATT_00170","ATT_00217", "ATT_00256", "ATT_00296" )
    ),
    -- Article characteristics
    art_chars as (
        SELECT 
            productBUReference as num_art, productCharacteristicIdentifier as cod_char, 
            valueOfCharacteristicInProductDefinition as value_char
        FROM 
            `dfdp-pdp-prod-master-dat-prod.product_descriptive_data_repository.ValueOfCharacteristicInProduct`
        where 
            businessUnitIdentifier=2  
            # Si no se pone NC no recoge todas las características como las dimensiones
            and valueOfCharacteristicInProductLanguage in ("ES", "NC") 
            and productBUReference = {reference}
    )

    SELECT 
        art_chars.num_art, art_chars.cod_char,art_chars.value_char,
        m_char.name_char
        
    FROM
        art_chars
    Join 
        metadata_char as m_char
    ON m_char.cod_char = art_chars.cod_char
    order by m_char.cod_char