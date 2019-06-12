--セッション中のキャッシュ設定をoffに設定
set
enable_result_cache_for_session = off
;
TRUNCATE gu_free.f_order_ec_new_061201
;
/* 約xx分かかっている */
/* xxx万件 */
INSERT INTO gu_free.f_order_ec_new_061201
SELECT
    pk_juchu_order_id
    ,pk_juchu_tran_type
    ,pk_juchu_item_cd
    ,juchu_order_datetime
    ,juchu_member_id
    ,juchu_account_uid
    ,juchu_color_cd
    ,juchu_size_cd
    ,juchu_color_cd2
    ,juchu_size_cd2
    ,juchu_quantity
    ,juchu_detail_total_amount
    ,skumaster_f_base_cost_amt_lcl
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
FROM
    gu_free.f_order
WHERE
    /* 過去120日分の受注レコード */
    DATEADD('day', - 120, CAST(CONVERT_TIMEZONE('JST', GETDATE()) AS date)) <= TRUNC(juchu_order_datetime)
;
COMMIT
;