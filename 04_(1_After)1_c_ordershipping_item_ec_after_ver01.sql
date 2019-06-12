--セッション中のキャッシュ設定をoffに設定
set
enable_result_cache_for_session = off
;
/* 直近120日分のデータを削除 */
DELETE
FROM
    gu_free.c_ordershipping_item_ec_new_061201
WHERE
    /* 過去120日分の受注 */
    DATEADD('day', - 120, CAST(CONVERT_TIMEZONE('JST', GETDATE()) AS date)) <= TRUNC(juchu_order_datetime)
OR  DATEADD('day', - 120, CAST(CONVERT_TIMEZONE('JST', GETDATE()) AS date)) <= TRUNC(purchase_order_datetime)
;
/* 約xx分かかっている */
INSERT INTO gu_free.c_ordershipping_item_ec_new_061201
SELECT
    CASE
        WHEN a.pk_juchu_order_id <> '' THEN a.pk_juchu_order_id
        ELSE b.purchase_order_id
    END AS pk_order_id
    ,CASE
        WHEN a.pk_juchu_tran_type <> '' THEN a.pk_juchu_tran_type
        ELSE b.pk_purchase_tran_type
    END AS pk_tran_type
    ,CASE
        WHEN a.pk_juchu_item_cd <> '' THEN a.pk_juchu_item_cd
        ELSE b.purchase_item_cd
    END AS pk_item_cd
    ,pk_purchase_transaction_id AS pk_transaction_id
    ,CASE
        WHEN b.purchase_member_id <> '' THEN b.purchase_member_id
        ELSE a.juchu_member_id
    END AS member_id
    ,a.pk_juchu_order_id AS juchu_order_id
    ,a.pk_juchu_tran_type AS juchu_tran_type
    ,a.pk_juchu_item_cd AS juchu_item_cd
    /* 元々の juchu_order_datetime は時刻情報が
       '00:00:00' なので juchu_ordered_time_char で補完する */
    /* ,a.juchu_order_datetime AS juchu_order_datetime */
    ,CAST(TO_CHAR(juchu_order_datetime, 'YYYY-MM-DD') || ' ' || SUBSTRING(juchu_ordered_time_char, 1, 2) || ':' || SUBSTRING(juchu_ordered_time_char, 3, 2) || ':' || SUBSTRING(juchu_ordered_time_char, 5, 2) AS TIMESTAMP) AS juchu_order_datetime
    ,a.juchu_member_id AS juchu_member_id
    ,a.juchu_color_cd AS juchu_color_cd
    ,a.juchu_size_cd AS juchu_size_cd
    ,a.juchu_color_cd2 AS juchu_color_cd2
    ,a.juchu_size_cd2 AS juchu_size_cd2
    /* 20190603 START 修正ロジック */
    /* ,a.juchu_quantity AS juchu_quantity */
    ,CASE
        WHEN a.juchu_quantity = b.purchase_quantity THEN a.juchu_quantity
        /* -->受注情報なし */
        WHEN a.juchu_quantity IS NULL THEN a.juchu_quantity
        /* -->出荷情報なし */
        WHEN b.purchase_quantity IS NULL THEN a.juchu_quantity
        /* -->受注と発注で数量が異なる場合は、purchaseの値を入れる(同一SKU複数PLU) */
        ELSE b.purchase_quantity
    END AS juchu_quantity
    /* ,a.juchu_detail_total_amount AS juchu_detail_total_amount */
    ,CASE
        WHEN a.juchu_detail_total_amount = b.purchase_price THEN a.juchu_detail_total_amount
        /* -->受注情報なし */
        WHEN a.juchu_detail_total_amount IS NULL THEN a.juchu_detail_total_amount
        /* -->出荷情報なし */
        WHEN b.purchase_price IS NULL THEN a.juchu_detail_total_amount
        /*-->受注と発注で数量が異なる場合は、purchaseの値を入れる(同一SKU複数PLU) */
        ELSE b.purchase_price
    END AS juchu_detail_total_amount
    /* 20190603 END */
    ,a.skumaster_f_base_cost_amt_lcl AS juchu_unit_cost_sku
    ,a.skumaster_f_base_cost_amt_lcl * a.juchu_quantity AS juchu_sum_cost_sku
    ,a.juchu_ordered_date_int AS juchu_ordered_date_int
    ,a.juchu_ordered_time_char AS juchu_ordered_time_char
    ,a.juchu_payment_type AS juchu_payment_type
    ,a.juchu_card_corporation AS juchu_card_corporation
    ,a.juchu_home_address AS juchu_home_address
    ,a.juchu_shipping_address AS juchu_shipping_address
    ,a.juchu_shipping_preferd_date_int AS juchu_shipping_preferd_date_int
    ,a.juchu_shipping_preferd_time_code AS juchu_shipping_preferd_time_code
    ,a.juchu_detail_status AS juchu_detail_status
    ,a.juchu_gift_type AS juchu_gift_type
    ,a.juchu_channel_code AS juchu_channel_code
    ,a.juchu_channel_name AS juchu_channel_name
    ,a.juchu_set_selling_code AS juchu_set_selling_code
    ,a.juchu_set_selling_flag AS juchu_set_selling_flag
    ,a.juchu_home_zipcode AS juchu_home_zipcode
    ,a.juchu_shipping_zipcode AS juchu_shipping_zipcode
    ,a.juchu_promotion_cd AS juchu_promotion_cd
    ,CASE
        WHEN b.purchase_order_id <> '' THEN 1
        ELSE 0
    END AS shipping_flag
    ,b.business_datetime AS business_datetime
    ,b.purchase_order_id AS purchase_order_id
    ,b.purchase_member_id AS purchase_member_id
    ,b.pk_purchase_transaction_id AS purchase_transaction_id
    ,b.purchase_g1_ims_store_id_6 AS purchase_g1_ims_store_id_6
    ,b.purchase_order_datetime AS purchase_order_datetime
    ,b.purchase_shipped_datetime AS purchase_shipped_datetime
    ,b.purchase_tran_no AS purchase_tran_no
    ,b.pk_purchase_tran_type AS purchase_tran_type
    ,b.purchase_status AS purchase_status
    ,b.purchase_value AS purchase_value
    ,b.purchase_update_datetime AS purchase_update_datetime
    ,b.purchase_type AS purchase_type
    ,b.purchase_item_cd AS purchase_item_cd
    ,b.purchase_name AS purchase_name
    ,b.purchase_item_status AS purchase_item_status
    ,b.purchase_unit_retail AS purchase_unit_retail
    ,b.purchase_quantity AS purchase_quantity
    ,b.purchase_price AS purchase_price
    ,b.purchase_disc_price AS purchase_disc_price
    ,b.purchase_price_before_discount AS purchase_price_before_discount
    /* 20190603 START 修正ロジック 単体コスト参照元の変更 */
    /* ,b.unit_cost_plu AS purchase_unit_cost_plu */
    ,b.skumaster_f_base_cost_amt_lcl AS purchase_unit_cost_plu
    /* 20190603 END */
    ,b.sum_cost_plu AS purchase_sum_cost_plu
    ,b.purchase_ec_flag AS purchase_ec_flag
    ,b.receiptplace_delivery_type
    ,b.receiptplace_receiving_store_place
    ,b.receiptplace_receiving_store_code
    ,b.receiptplace_corporate_name
    ,b.receiptplace_pickup_store_name
    ,b.receiptplace_receipt_date
    ,b.receiptplace_returned_flag
    ,b.inventory_store_inventory_flag
    ,b.inventory_inventory_destination
    ,b.inventory_booking_store_code
    ,b.inventory_shipping_store_code
    ,b.inventory_receiptplace_type_code
    ,b.inventory_corporate_name
    ,b.inventory_booking_store_name
    ,b.inventory_shipping_store_name
    ,CASE
        WHEN d.account_account_id IS NOT NULL THEN d.account_account_id
        ELSE c.account_account_id
    END AS account_account_id
    ,CASE
        WHEN d.account_account_id IS NOT NULL THEN d.account_uid
        ELSE c.account_uid
    END AS account_uid
    ,CASE
        WHEN d.account_account_id IS NOT NULL THEN d.account_next_uid
        ELSE c.account_next_uid
    END AS account_next_uid
    ,CASE
        WHEN d.account_account_id IS NOT NULL THEN d.account_channel_key
        ELSE c.account_channel_key
    END AS account_channel_key
    ,CASE
        WHEN d.account_account_id IS NOT NULL THEN d.account_channel_uid
        ELSE c.account_channel_uid
    END AS account_channel_uid
    ,CASE
        WHEN d.account_account_id IS NOT NULL THEN d.account_client_id
        ELSE c.account_client_id
    END AS account_client_id
    ,CASE
        WHEN d.account_account_id IS NOT NULL THEN d.account_status
        ELSE c.account_status
    END AS account_status
    ,CASE
        WHEN d.account_account_id IS NOT NULL THEN d.account_data
        ELSE c.account_data
    END AS account_data
    ,CASE
        WHEN d.account_account_id IS NOT NULL THEN d.account_addr_nos
        ELSE c.account_addr_nos
    END AS account_addr_nos
    ,CASE
        WHEN d.account_account_id IS NOT NULL THEN d.account_unregist_timestamp
        ELSE c.account_unregist_timestamp
    END AS account_unregist_timestamp
    ,CASE
        WHEN d.account_account_id IS NOT NULL THEN d.address_gender_cd
        ELSE c.address_gender_cd
    END AS address_gender_cd
    ,CASE
        WHEN d.account_account_id IS NOT NULL THEN d.address_gender
        ELSE c.address_gender
    END AS address_gender
    ,CASE
        WHEN d.account_account_id IS NOT NULL THEN d.address_dateofbirth
        ELSE c.address_dateofbirth
    END AS address_dateofbirth
    ,CASE
        WHEN d.account_account_id IS NOT NULL THEN d.address_zipcode
        ELSE c.address_zipcode
    END AS address_zipcode
    ,CASE
        WHEN d.account_account_id IS NOT NULL THEN d.address_addr_pref
        ELSE c.address_addr_pref
    END AS address_addr_pref
    /* f_item 追加*/
    /*
	,b.pk_date AS item_pk_date
    ,b.pk_catalog_l3_product_id AS item_pk_catalog_l3_product_id
    */
    ,b.item_pk_date AS item_pk_date
    ,b.item_pk_catalog_l3_product_id AS item_pk_catalog_l3_product_id
    /* ,b.pk_catalog_l2_product_id */
    ,b.catalog_dept_id
    ,b.catalog_class_cd
    ,b.catalog_sub_class_cd
    ,b.catalog_g_dept_id
    ,b.catalog_product_name
    ,b.catalog_org_sales_price
    ,b.catalog_current_sales_price
    ,b.catalog_ec_product_mgmt_cd
    ,b.catalog_view_product_cd
    ,b.catalog_view_year_cd
    ,b.catalog_season
    ,b.catalog_cost_price_seq_num
    ,b.catalog_pack_ind
    ,b.catalog_receive_unit_qty
    ,b.catalog_color_cd
    ,b.catalog_size_cd
    ,b.catalog_ptn_length_cd
    ,b.catalog_color_name
    ,b.catalog_size_name
    ,b.catalog_design_name
    ,b.catalog_special_size_flag
    ,b.catalog_online_limited_flag
    ,b.catalog_price_down_flag
    ,b.catalog_limited_time_offer_flag
    ,b.catalog_limited_time_offer_end_date
    ,b.catalog_new_product_flag
    ,b.catalog_sales_start_date
    ,b.catalog_bulk_buying_id
    ,b.catalog_bulk_buying_start_date
    ,b.catalog_bulk_buying_end_date
    ,b.catalog_view_l2_color_cd
    ,b.catalog_display_level
    ,b.catalog_link_url
    ,b.catalog_sales_status
    ,b.catalog_last_update_date
    ,b.catalog_first_price_view_flag
    ,b.catalog_display_cd
    ,b.catalog_display_category
    ,b.catalog_meta_description
    ,b.catalog_meta_keywords
    ,b.catalog_promotion_discount_type
    ,b.catalog_sales_end_date
    ,b.skumaster_fr_yr_cd
    ,b.skumaster_fr_seasn_cd
    ,b.skumaster_dept_id
    ,b.skumaster_class_cd
    ,b.skumaster_sub_class_cd
    ,b.skumaster_g_dept_id
    ,b.skumaster_item_uda_d_sbc1_idnt
    ,b.skumaster_item_uda_d_sbc1_desc
    ,b.skumaster_item_uda_d_sbc3_idnt
    ,b.skumaster_item_uda_d_sbc3_desc
    ,b.skumaster_fr_view_itm_cd
    ,b.skumaster_level3_desc
    ,b.skumaster_fr_cost_price_seq_num
    ,b.skumaster_color_cd
    ,b.skumaster_color_name
    ,b.skumaster_size_cd
    ,b.skumaster_size_name
    ,b.skumaster_ptn_length_cd
    ,b.skumaster_design_name
    ,b.skumaster_fr_orig_sls_price_lcl
    ,b.skumaster_f_unit_rtl_amt_lcl
    /* ,b.skumaster_f_base_cost_amt_lcl */
    ,b.skumaster_sum_cd
    ,b.skumaster_sum_desc
    ,b.repskulink_div
    ,b.repskulink_sum_cd
    ,b.repskulink_sum_type
    ,b.repskulink_sum_name
    ,b.repskulink_item_cd
    ,b.repskulink_item_name
    ,b.repskulink_col
    ,b.repskulink_sze
    ,b.repskulink_ptn
    ,b.repskulink_yr
    ,b.repskulink_ssn
    ,b.repskulink_cost_seq
    /* 20190603 START 追加ロジック
       item_ec(ec), item_store(store), item_return(return)を判別するためのフラグ*/
    ,'ec' AS trading_type
/* 20190603 END */
FROM
    gu_free.f_order_ec_new_061201 AS a
    FULL OUTER JOIN
        gu_free.f_shipping_ec_new_061201 AS b
    ON  (
            a.pk_juchu_order_id = b.purchase_order_id
        AND a.pk_juchu_tran_type = b.pk_purchase_tran_type
        AND a.pk_juchu_item_cd = b.purchase_item_cd
        )
    LEFT JOIN
        gu_free.f_account c
    ON  a.juchu_member_id = c.pk_account_member_id
    LEFT JOIN
        gu_free.f_account d
    ON  b.purchase_member_id = d.pk_account_member_id
;
COMMIT
;