--テーブルが存在していた場合テ  ーブルdrop
--DROP TABLE IF EXISTS  gu_free.c_ordershipping_item_all_new_060602;
--c_ordershipping_order_ecのロジックを元に、bi側に対応するテーブルを作成
--CREATE TABLE  gu_free.c_ordershipping_item_all_new_060602 AS
--セッション中のキャッシュ設定をoffに設定
set
enable_result_cache_for_session = off
;
DELETE
FROM
    gu_free.c_ordershipping_item_all_new_061202
WHERE
    /* ecのみを対象 */
    trading_type = 'store'
;
INSERT INTO gu_free.c_ordershipping_item_all_new_061202
/* c_ordershipping_item_ec
   アイテム情報以外を保有 nmitemは排除 */
WITH item_base_info as(
    SELECT
        pk_order_id
        ,pk_tran_type
        ,pk_item_cd
        ,pk_transaction_id
        ,member_id
        ,juchu_order_id
        ,juchu_tran_type
        ,juchu_item_cd
        ,juchu_order_datetime
        ,juchu_member_id
        ,juchu_color_cd
        ,juchu_size_cd
        ,juchu_color_cd2
        ,juchu_size_cd2
        ,SUM(juchu_quantity) AS juchu_quantity
        ,SUM(juchu_detail_total_amount) AS juchu_detail_total_amount
        ,SUM(juchu_unit_cost_sku * juchu_quantity) / SUM(juchu_quantity) AS juchu_unit_cost_sku
        ,SUM(juchu_sum_cost_sku) AS juchu_sum_cost_sku
        ,juchu_ordered_date_int
        ,juchu_ordered_time_char
        ,juchu_payment_type
        ,juchu_card_corporation
        ,juchu_home_address
        ,juchu_shipping_address
        ,juchu_shipping_preferd_date_int
        ,juchu_shipping_preferd_time_code
        ,juchu_detail_status
        ,juchu_gift_type
        ,juchu_channel_code
        ,juchu_channel_name
        ,juchu_set_selling_code
        ,juchu_set_selling_flag
        ,juchu_home_zipcode
        ,juchu_shipping_zipcode
        ,juchu_promotion_cd
        ,shipping_flag
        ,business_datetime
        ,purchase_order_id
        ,purchase_member_id
        ,purchase_transaction_id
        ,purchase_g1_ims_store_id_6
        ,purchase_order_datetime
        ,purchase_shipped_datetime
        ,purchase_tran_no
        ,purchase_tran_type
        ,purchase_status
        ,purchase_value
        ,purchase_update_datetime
        ,purchase_type
        ,purchase_item_cd
        ,purchase_name
        ,purchase_item_status
        ,SUM(purchase_unit_retail * purchase_quantity) / SUM(purchase_quantity) AS purchase_unit_retail
        ,SUM(purchase_quantity) AS purchase_quantity
        ,SUM(purchase_price) AS purchase_price
        ,SUM(purchase_disc_price) AS purchase_disc_price
        ,SUM(purchase_price_before_discount) AS purchase_price_before_discount
        ,SUM(purchase_unit_cost_plu * purchase_quantity) / SUM(purchase_quantity) AS purchase_unit_cost_plu
        ,SUM(purchase_sum_cost_plu) AS purchase_sum_cost_plu
        ,purchase_ec_flag
        ,receiptplace_delivery_type
        ,receiptplace_receiving_store_place
        ,receiptplace_receiving_store_code
        ,receiptplace_corporate_name
        ,receiptplace_pickup_store_name
        ,receiptplace_receipt_date
        ,receiptplace_returned_flag
        ,inventory_store_inventory_flag
        ,inventory_inventory_destination
        ,inventory_booking_store_code
        ,inventory_shipping_store_code
        ,inventory_receiptplace_type_code
        ,inventory_corporate_name
        ,inventory_booking_store_name
        ,inventory_shipping_store_name
        ,account_account_id
        ,account_uid
        ,account_next_uid
        ,account_channel_key
        ,account_channel_uid
        ,account_client_id
        ,account_status
        ,account_data
        ,account_addr_nos
        ,account_unregist_timestamp
        ,address_gender_cd
        ,address_gender
        ,address_dateofbirth
        ,address_zipcode
        ,address_addr_pref
        ,trading_type
    FROM
        gu_free.c_ordershipping_item_store_new_061202
    WHERE
        /* 直近10日分 */
        DATEADD('day', - 10, CAST(CONVERT_TIMEZONE('JST', GETDATE()) AS date)) <= TRUNC(business_datetime)
    AND (
            purchase_type = 'ITEM'
        OR  purchase_type IS NULL
        )
    GROUP BY
        pk_order_id
        ,pk_tran_type
        ,pk_item_cd
        ,pk_transaction_id
        ,member_id
        ,juchu_order_id
        ,juchu_tran_type
        ,juchu_item_cd
        ,juchu_order_datetime
        ,juchu_member_id
        ,juchu_color_cd
        ,juchu_size_cd
        ,juchu_color_cd2
        ,juchu_size_cd2
        ,juchu_ordered_date_int
        ,juchu_ordered_time_char
        ,juchu_payment_type
        ,juchu_card_corporation
        ,juchu_home_address
        ,juchu_shipping_address
        ,juchu_shipping_preferd_date_int
        ,juchu_shipping_preferd_time_code
        ,juchu_detail_status
        ,juchu_gift_type
        ,juchu_channel_code
        ,juchu_channel_name
        ,juchu_set_selling_code
        ,juchu_set_selling_flag
        ,juchu_home_zipcode
        ,juchu_shipping_zipcode
        ,juchu_promotion_cd
        ,shipping_flag
        ,business_datetime
        ,purchase_order_id
        ,purchase_member_id
        ,purchase_transaction_id
        ,purchase_g1_ims_store_id_6
        ,purchase_order_datetime
        ,purchase_shipped_datetime
        ,purchase_tran_no
        ,purchase_tran_type
        ,purchase_status
        ,purchase_value
        ,purchase_update_datetime
        ,purchase_type
        ,purchase_item_cd
        ,purchase_name
        ,purchase_item_status
        ,purchase_ec_flag
        ,receiptplace_delivery_type
        ,receiptplace_receiving_store_place
        ,receiptplace_receiving_store_code
        ,receiptplace_corporate_name
        ,receiptplace_pickup_store_name
        ,receiptplace_receipt_date
        ,receiptplace_returned_flag
        ,inventory_store_inventory_flag
        ,inventory_inventory_destination
        ,inventory_booking_store_code
        ,inventory_shipping_store_code
        ,inventory_receiptplace_type_code
        ,inventory_corporate_name
        ,inventory_booking_store_name
        ,inventory_shipping_store_name
        ,account_account_id
        ,account_uid
        ,account_next_uid
        ,account_channel_key
        ,account_channel_uid
        ,account_client_id
        ,account_status
        ,account_data
        ,account_addr_nos
        ,account_unregist_timestamp
        ,address_gender_cd
        ,address_gender
        ,address_dateofbirth
        ,address_zipcode
        ,address_addr_pref
        ,trading_type
)
/* 代表PLUをMAXで取得 */
,item_info_max as(
    SELECT
        pk_transaction_id
        ,pk_item_cd
        ,pk_tran_type
        ,item_pk_date
        ,max(item_pk_catalog_l3_product_id) as item_max_plu
    FROM
        gu_free.c_ordershipping_item_store_new_061202
    WHERE
        /* 直近10日分 */
        DATEADD('day', - 10, CAST(CONVERT_TIMEZONE('JST', GETDATE()) AS date)) <= TRUNC(business_datetime)
    AND (
            purchase_type = 'ITEM'
        OR  purchase_type IS NULL
        )
    GROUP BY
        pk_transaction_id
        ,pk_item_cd
        ,pk_tran_type
        ,item_pk_date
)
/* 代表PLUのアイテム情報をf_itemから取得 */
SELECT
    a.pk_order_id
    ,a.pk_tran_type
    ,a.pk_item_cd
    ,a.pk_transaction_id
    ,a.member_id
    ,a.juchu_order_id
    ,a.juchu_tran_type
    ,a.juchu_item_cd
    ,a.juchu_order_datetime
    ,a.juchu_member_id
    ,a.juchu_color_cd
    ,a.juchu_size_cd
    ,a.juchu_color_cd2
    ,a.juchu_size_cd2
    ,a.juchu_quantity
    ,a.juchu_detail_total_amount
    ,a.juchu_unit_cost_sku
    ,a.juchu_sum_cost_sku
    ,a.juchu_ordered_date_int
    ,a.juchu_ordered_time_char
    ,a.juchu_payment_type
    ,a.juchu_card_corporation
    ,a.juchu_home_address
    ,a.juchu_shipping_address
    ,a.juchu_shipping_preferd_date_int
    ,a.juchu_shipping_preferd_time_code
    ,a.juchu_detail_status
    ,a.juchu_gift_type
    ,a.juchu_channel_code
    ,a.juchu_channel_name
    ,a.juchu_set_selling_code
    ,a.juchu_set_selling_flag
    ,a.juchu_home_zipcode
    ,a.juchu_shipping_zipcode
    ,a.juchu_promotion_cd
    ,a.shipping_flag
    ,a.business_datetime
    ,a.purchase_order_id
    ,a.purchase_member_id
    ,a.purchase_transaction_id
    ,a.purchase_g1_ims_store_id_6
    ,a.purchase_order_datetime
    ,a.purchase_shipped_datetime
    ,a.purchase_tran_no
    ,a.purchase_tran_type
    ,a.purchase_status
    ,a.purchase_value
    ,a.purchase_update_datetime
    ,a.purchase_type
    ,a.purchase_item_cd
    ,a.purchase_name
    ,a.purchase_item_status
    ,a.purchase_unit_retail
    ,a.purchase_quantity
    ,a.purchase_price
    ,a.purchase_disc_price
    ,a.purchase_price_before_discount
    ,a.purchase_unit_cost_plu
    ,a.purchase_sum_cost_plu
    ,a.purchase_ec_flag
    ,a.receiptplace_delivery_type
    ,a.receiptplace_receiving_store_place
    ,a.receiptplace_receiving_store_code
    ,a.receiptplace_corporate_name
    ,a.receiptplace_pickup_store_name
    ,a.receiptplace_receipt_date
    ,a.receiptplace_returned_flag
    ,a.inventory_store_inventory_flag
    ,a.inventory_inventory_destination
    ,a.inventory_booking_store_code
    ,a.inventory_shipping_store_code
    ,a.inventory_receiptplace_type_code
    ,a.inventory_corporate_name
    ,a.inventory_booking_store_name
    ,a.inventory_shipping_store_name
    ,a.account_account_id
    ,a.account_uid
    ,a.account_next_uid
    ,a.account_channel_key
    ,a.account_channel_uid
    ,a.account_client_id
    ,a.account_status
    ,a.account_data
    ,a.account_addr_nos
    ,a.account_unregist_timestamp
    ,a.address_gender_cd
    ,a.address_gender
    ,a.address_dateofbirth
    ,a.address_zipcode
    ,a.address_addr_pref
    ,b.item_pk_date
    ,b.item_max_plu
    ,item.catalog_dept_id
    ,item.catalog_class_cd
    ,item.catalog_sub_class_cd
    ,item.catalog_g_dept_id
    ,item.catalog_product_name
    ,item.catalog_org_sales_price
    ,item.catalog_current_sales_price
    ,item.catalog_ec_product_mgmt_cd
    ,item.catalog_view_product_cd
    ,item.catalog_view_year_cd
    ,item.catalog_season
    ,item.catalog_cost_price_seq_num
    ,item.catalog_pack_ind
    ,item.catalog_receive_unit_qty
    ,item.catalog_color_cd
    ,item.catalog_size_cd
    ,item.catalog_ptn_length_cd
    ,item.catalog_color_name
    ,item.catalog_size_name
    ,item.catalog_design_name
    ,item.catalog_special_size_flag
    ,item.catalog_online_limited_flag
    ,item.catalog_price_down_flag
    ,item.catalog_limited_time_offer_flag
    ,item.catalog_limited_time_offer_end_date
    ,item.catalog_new_product_flag
    ,item.catalog_sales_start_date
    ,item.catalog_bulk_buying_id
    ,item.catalog_bulk_buying_start_date
    ,item.catalog_bulk_buying_end_date
    ,item.catalog_view_l2_color_cd
    ,item.catalog_display_level
    ,item.catalog_link_url
    ,item.catalog_sales_status
    ,item.catalog_last_update_date
    ,item.catalog_first_price_view_flag
    ,item.catalog_display_cd
    ,item.catalog_display_category
    ,item.catalog_meta_description
    ,item.catalog_meta_keywords
    ,item.catalog_promotion_discount_type
    ,item.catalog_sales_end_date
    ,item.skumaster_fr_yr_cd
    ,item.skumaster_fr_seasn_cd
    ,item.skumaster_dept_id
    ,item.skumaster_class_cd
    ,item.skumaster_sub_class_cd
    ,item.skumaster_g_dept_id
    ,item.skumaster_item_uda_d_sbc1_idnt
    ,item.skumaster_item_uda_d_sbc1_desc
    ,item.skumaster_item_uda_d_sbc3_idnt
    ,item.skumaster_item_uda_d_sbc3_desc
    ,item.skumaster_fr_view_itm_cd
    ,item.skumaster_level3_desc
    ,item.skumaster_fr_cost_price_seq_num
    ,item.skumaster_color_cd
    ,item.skumaster_color_name
    ,item.skumaster_size_cd
    ,item.skumaster_size_name
    ,item.skumaster_ptn_length_cd
    ,item.skumaster_design_name
    ,item.skumaster_fr_orig_sls_price_lcl
    ,item.skumaster_f_unit_rtl_amt_lcl
    ,item.skumaster_sum_cd
    ,item.skumaster_sum_desc
    ,item.repskulink_div
    ,item.repskulink_sum_cd
    ,item.repskulink_sum_type
    ,item.repskulink_sum_name
    ,item.repskulink_item_cd
    ,item.repskulink_item_name
    ,item.repskulink_col
    ,item.repskulink_sze
    ,item.repskulink_ptn
    ,item.repskulink_yr
    ,item.repskulink_ssn
    ,item.repskulink_cost_seq
    ,a.trading_type
FROM
    item_base_info a
    LEFT JOIN
        item_info_max b
    ON  a.pk_transaction_id = b.pk_transaction_id
    AND a.pk_tran_type = b.pk_tran_type
    AND a.pk_item_cd = b.pk_item_cd
    LEFT JOIN
        gu_free.f_item item
    ON  b.item_max_plu = item.pk_catalog_l3_product_id
    AND b.item_pk_date = item.pk_date
;
COMMIT
;