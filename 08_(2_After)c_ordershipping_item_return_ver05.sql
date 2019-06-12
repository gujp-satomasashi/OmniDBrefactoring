--セッション中のキャッシュ設定をoffに設定
set
enable_result_cache_for_session = off
;
/* 全期間の返品データを削除 */
DELETE
FROM
    gu_free.c_ordershipping_item_return_new_061202
;
/* 約分かかっている */
/*
   f_order には返品のレコードは入らないため、f_shipping だけで
   FULL OUTER JOINしたときと同じ形式にして、後段の v_c_ordershiiping_item で返品以外レコードと UNION ALL する
*/
INSERT INTO gu_free.c_ordershipping_item_return_new_061202 WITH
/* PLUで引当した原価で原価計算 */
shipping_plu AS(
    SELECT
        business_datetime
        ,purchase_order_id
        ,purchase_member_id
        ,purchase_account_uid
        ,pk_purchase_transaction_id
        ,purchase_g1_ims_store_id_6
        ,purchase_order_datetime
        ,purchase_shipped_datetime
        ,purchase_tran_no
        ,pk_purchase_tran_type
        ,purchase_status
        ,purchase_value
        ,purchase_update_datetime
        ,purchase_type
        ,item.pk_catalog_l2_product_id AS purchase_item_cd
        /* SKU単位 */
        ,purchase_name
        ,SUM(purchase_quantity) AS purchase_quantity
        ,SUM(purchase_price) AS purchase_price
        ,SUM(purchase_disc_price) AS purchase_disc_price
        ,SUM(purchase_price_before_discount) AS purchase_price_before_discount
        ,purchase_item_status
        /* 20190603 START 修正・追加ロジック */
        /* ,MAX(purchase_unit_retail) AS purchase_unit_retail */
        ,purchase_unit_retail
        ,item.skumaster_f_base_cost_amt_lcl AS skumaster_f_base_cost_amt_lcl
        /* 20190603 END*/
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
        ,SUM(item.skumaster_f_base_cost_amt_lcl * purchase_quantity) AS sum_cost_plu
        /* PLUコードで引当した原価の合計 */
        /*20190603 START f_itemカラム追加*/
        ,item.pk_date
        ,item.pk_catalog_l3_product_id
        /*,item.pk_catalog_l2_product_id */
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
        /* ,item_skumaster_f_base_cost_amt_lcl */
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
    /*20190603 END */
    FROM
        gu_free.f_shipping AS fs
        /* PLUコードで受注時点の原価を取得 */
        LEFT JOIN
            gu_free.f_item item
        ON  fs.pk_purchase_plu = item.pk_catalog_l3_product_id
        AND TRUNC(fs.purchase_order_datetime) = item.pk_date
    WHERE
        /* 返品（RETURN） */
        pk_purchase_tran_type IN('RETURN')
    GROUP BY
        business_datetime
        ,purchase_order_id
        ,purchase_member_id
        ,purchase_account_uid
        ,pk_purchase_transaction_id
        ,purchase_g1_ims_store_id_6
        ,purchase_order_datetime
        ,purchase_shipped_datetime
        ,purchase_tran_no
        ,pk_purchase_tran_type
        ,purchase_status
        ,purchase_value
        ,purchase_update_datetime
        ,purchase_type
        ,item.pk_catalog_l2_product_id
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
        /* 20190603 START 追加ロジック */
        ,purchase_unit_retail
        /* 20190603 END */
        ,item.skumaster_f_base_cost_amt_lcl
        /* 20190603 START 追加ロジック START */
        ,item.pk_date
        ,item.pk_catalog_l3_product_id
        /* ,item_pk_catalog_l2_product_id */
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
        /* ,item_skumaster_f_base_cost_amt_lcl */
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
        /* 20190603 END */
)
/* 20190527 START 以下のwith句のロジック削除
,shipping AS(
     SELECT
         business_datetime
        ,purchase_order_id
        ,purchase_member_id
        ,purchase_account_uid
        ,pk_purchase_transaction_id
        ,purchase_g1_ims_store_id_6
        ,purchase_order_datetime
        ,purchase_shipped_datetime
        ,purchase_tran_no
        ,pk_purchase_tran_type
        ,purchase_status
        ,purchase_value
        ,purchase_update_datetime
        ,purchase_type
        ,purchase_item_cd
        ,purchase_name
        ,SUM(purchase_quantity) AS purchase_quantity
        ,SUM(purchase_price) AS purchase_price
        ,SUM(purchase_disc_price) AS purchase_disc_price
        ,SUM(purchase_price_before_discount) AS purchase_price_before_discount
        ,purchase_item_status
        ,MAX(purchase_unit_retail) AS purchase_unit_retail
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
        ,SUM(sum_cost_plu) AS sum_cost_plu 
    FROM
        shipping_plu
    GROUP BY
         business_datetime
        ,purchase_order_id
        ,purchase_member_id
        ,purchase_account_uid
        ,pk_purchase_transaction_id
        ,purchase_g1_ims_store_id_6
        ,purchase_order_datetime
        ,purchase_shipped_datetime
        ,purchase_tran_no
        ,pk_purchase_tran_type
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
)
   20190603 END */
SELECT
    b.purchase_order_id AS pk_order_id
    ,b.pk_purchase_tran_type AS pk_tran_type
    ,b.purchase_item_cd AS pk_item_cd
    ,pk_purchase_transaction_id AS pk_transaction_id
    ,b.purchase_member_id AS member_id
    ,b.purchase_order_id AS juchu_order_id
    ,b.pk_purchase_tran_type AS juchu_tran_type
    ,b.purchase_item_cd AS juchu_item_cd
    ,b.purchase_order_datetime AS juchu_order_datetime
    ,b.purchase_member_id AS juchu_member_id
    ,NULL AS juchu_color_cd
    /* 対応カラムなし */
    ,NULL AS juchu_size_cd
    /* 対応カラムなし */
    ,NULL AS juchu_color_cd2
    /* 対応カラムなし */
    ,NULL AS juchu_size_cd2
    /* 対応カラムなし */
    ,b.purchase_quantity AS juchu_quantity
    /* 20190603 START ロジック修正
    ,b.purchase_price * -(purchase_quantity) AS juchu_detail_total_amount */
    ,b.purchase_price AS juchu_detail_total_amount
    /* 20190603 END */
    ,NULL AS juchu_f_base_cost_amt_lcl
    /* 対応カラムなし */
    /* 20190603 START カラム追加 */
    ,NULL AS juchu_sum_cost_sku
    /* 対応カラムなし */
    /* 20190603 END */
    ,NULL AS juchu_ordered_date_int
    /* 使わなそうなのでいったん除外 */
    ,NULL AS juchu_ordered_time_char
    /* 使わなそうなのでいったん除外 */
    ,NULL AS juchu_payment_type
    /* 対応カラムなし */
    ,NULL AS juchu_card_corporation
    /* 対応カラムなし */
    ,d.address_addr_pref AS juchu_home_address
    ,d.address_addr_pref AS juchu_shipping_address
    ,NULL AS juchu_shipping_preferd_date_int
    /* 対応カラムなし */
    ,NULL AS juchu_shipping_preferd_time_code
    /* 対応カラムなし */
    ,NULL AS juchu_detail_status
    /* 対応カラムなし */
    ,NULL AS juchu_gift_type
    /* 対応カラムなし */
    ,NULL AS juchu_channel_code
    /* 対応カラムなし */
    ,NULL AS juchu_channel_name
    /* 対応カラムなし */
    ,NULL AS juchu_set_selling_code
    /* 対応カラムなし */
    ,NULL AS juchu_set_selling_flag
    /* 対応カラムなし */
    ,d.address_zipcode AS juchu_home_zipcode
    ,d.address_zipcode AS juchu_shipping_zipcode
    ,NULL AS juchu_promotion_cd
    /* 対応カラムなし */
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
    /* 20190603 START ロジックの修正 */
    /* ,b.sum_cost_plu / b.purchase_quantity AS purchase_unit_cost_plu*/
    /* 加重平均コスト */
    ,b.skumaster_f_base_cost_amt_lcl AS purchase_unit_cost_plu
    /* 20190603 END */
    ,b.sum_cost_plu AS purchase_sum_cost_plu
    /* 20190603 START ロジックの削除 コストはpurchase_unit_cost_pluで保有　後続で使用していない*/
    /*,NULL AS purchase_f_base_cost_amt_lcl*/
    /* TODO: 不要なので削除 */
    /* 20190603 END */
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
    ,d.account_account_id
    ,d.account_uid
    ,d.account_next_uid
    ,d.account_channel_key
    ,d.account_channel_uid
    ,d.account_client_id
    ,d.account_status
    ,d.account_data AS account_data
    ,d.account_addr_nos
    ,d.account_unregist_timestamp
    ,d.address_gender_cd
    ,d.address_gender
    ,d.address_dateofbirth
    ,d.address_zipcode
    ,d.address_addr_pref
    /*20190603 f_item 追加 */
    ,b.pk_date AS item_pk_date
    ,b.pk_catalog_l3_product_id AS item_pk_catalog_l3_product_id
    /* ,item.pk_catalog_l2_product_id */
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
    /* ,item_skumaster_f_base_cost_amt_lcl */
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
    /* 20190603 END */
    /* 20190603 START 追加ロジック
       item_ec(ec), item_store(store), item_return(return)を判別するためのフラグ*/
    ,'return' AS trading_type
/* 20190603 END */
FROM
    shipping_plu AS b
    LEFT JOIN
        gu_free.f_account AS d
    ON  b.purchase_member_id = d.pk_account_member_id
;
COMMIT
;