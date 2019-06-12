--セッション中のキャッシュ設定をoffに設定
set
enable_result_cache_for_session = off
;
TRUNCATE gu_free.f_shipping_return_new_061201
;
/* 約xx分かかっている */
INSERT INTO gu_free.f_shipping_return_new_061201
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
;
COMMIT
;