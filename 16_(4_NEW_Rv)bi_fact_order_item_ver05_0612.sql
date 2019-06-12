--セッション中のキャッシュ設定をoffに設定
set
enable_result_cache_for_session = off
;
/*bi_fact_order_itemを120日分のデータを更新*/
DELETE
FROM
    --gu_free.bi_fact_order_item
    gu_free.bi_fact_order_item_new_061201
    --WHERE
    --    /* 過去120日分の受注 */
    --        DATEADD('day', - 120, CAST(CONVERT_TIMEZONE('JST', GETDATE()) AS date)) <= TRUNC(juchu_order_datetime)
    --    OR  DATEADD('day', - 120, CAST(CONVERT_TIMEZONE('JST', GETDATE()) AS date)) <= TRUNC(purchase_order_datetime)
;
/* 約xx分かかっている */
INSERT INTO gu_free.bi_fact_order_item_new_061201 WITH c_item_all AS(
    SELECT
        a.pk_order_id
        ,a.pk_item_cd
        ,a.pk_tran_type
        ,a.pk_transaction_id
        ,a.member_id
        /* trading_type = 'return'の場合はNULL */
        ,CASE
            WHEN a.trading_type <> 'return' THEN a.juchu_order_datetime
            ELSE NULL
        END AS juchu_order_datetime
        ,a.juchu_order_id
        ,a.juchu_tran_type
        /* BIGINT型にINSERT、''の場合はNULLで返す */
        ,CASE
            WHEN a.juchu_member_id <> '' THEN a.juchu_member_id
            ELSE NULL
        END AS juchu_member_id
        ,a.juchu_color_cd
        ,a.juchu_size_cd
        /* trading_type = 'return'の場合はNULL */
        ,CASE
            WHEN a.trading_type <> 'return' THEN a.juchu_item_cd
            ELSE NULL
        END AS juchu_item_cd
        ,a.juchu_color_cd2
        ,a.juchu_size_cd2
        /* trading_type = 'return'の場合はNULL */
        ,CASE
            WHEN a.trading_type <> 'return' THEN a.juchu_quantity
            ELSE NULL
        END AS juchu_quantity
        /* trading_type = 'return'の場合はNULL */
        ,CASE
            WHEN a.trading_type <> 'return' THEN a.juchu_detail_total_amount
            ELSE NULL
        END AS juchu_detail_total_amount
        ,a.juchu_ordered_date_int
        ,a.juchu_ordered_time_char
        ,a.juchu_card_corporation
        ,a.juchu_shipping_preferd_date_int
        ,a.juchu_shipping_preferd_time_code
        ,a.juchu_detail_status
        ,a.juchu_set_selling_code
        ,a.juchu_set_selling_flag
        ,a.purchase_order_id
        ,a.shipping_flag
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
        ,c.uid
        ,c.gender_cd
        ,c.gender
        ,c.dateofbirth
        ,c.addr_pref
        ,c.zipcode
    FROM
        (
            SELECT
                *
            FROM
                --gu_free.c_ordershipping_item_all
                gu_free.c_ordershipping_item_all_new_061201
            WHERE
                /* 過去120日分の受注 */
                (
                    DATEADD('day', - 120, CAST(CONVERT_TIMEZONE('JST', GETDATE()) AS date)) <= TRUNC(juchu_order_datetime)
                OR  DATEADD('day', - 120, CAST(CONVERT_TIMEZONE('JST', GETDATE()) AS date)) <= TRUNC(purchase_order_datetime)
                )
            /* ec・returnを対象 */
        AND (
                trading_type = 'ec'
            OR  trading_type = 'return'
            )
        ) a
        LEFT JOIN
            /* 出荷レコードが前受金(店舗在庫引当)の場合に、juchu側のメンバーIDが */
            gu_free.bi_account_info AS c
        ON  a.member_id = c.member_id
    WHERE
        /* ec購入以外(店舗購入)を除外 */
        (
            purchase_ec_flag IS TRUE
        OR
            /* 前受金(在庫店舗引当)のレコードは保持しておくための条件
            order単位でまとめる際に、NMITEMの情報とJOIN ただし、受注後キャンセルしたレコードも含む */
            purchase_order_id IS NULL
        )
)
,f_order_info AS(
    SELECT
        pk_juchu_order_id
        ,juchu_payment_type
        ,juchu_channel_code
        ,juchu_channel_name
        ,juchu_gift_type
        ,juchu_home_address
        ,juchu_home_zipcode
        ,juchu_shipping_address
        ,juchu_shipping_zipcode
    FROM
        gu_free.f_order_ec_new_061201
        --gu_free.f_order_ec
        --    gu_free.f_order
        --WHERE
        --    /* 過去120日分の受注レコード */
    --    DATEADD('day', - 120, CAST(CONVERT_TIMEZONE('JST', GETDATE()) AS date)) <= TRUNC(juchu_order_datetime)
    GROUP BY
        pk_juchu_order_id
        ,juchu_payment_type
        ,juchu_channel_code
        ,juchu_channel_name
        ,juchu_gift_type
        ,juchu_home_address
        ,juchu_home_zipcode
        ,juchu_shipping_address
        ,juchu_shipping_zipcode
)
SELECT
    a.pk_order_id AS order_id
    ,a.pk_item_cd AS item_cd
    ,a.pk_tran_type AS tran_type
    /*,a.pk_transaction_id AS transaction_id */
    ,a.member_id
    ,cast(to_char(a.juchu_order_datetime, 'yyyymmdd') AS INTEGER) AS order_received_date_and_time
    ,a.juchu_order_datetime AS order_date
    ,a.juchu_order_id
    ,a.juchu_tran_type
    ,cast(a.juchu_member_id AS BIGINT) AS juchu_member_id
    ,0 AS shipping_destination_warehouse
    ,0 AS promotion_cd
    ,0 AS request_number
    ,a.juchu_color_cd AS color_cd
    ,a.juchu_size_cd AS size_cd
    ,a.juchu_item_cd
    ,a.juchu_color_cd2 AS color_cd2
    ,a.juchu_size_cd2 AS size_cd2
    ,a.juchu_quantity
    ,a.juchu_detail_total_amount AS juchu_total_amount
    ,a.juchu_ordered_date_int AS order_received_date
    ,a.juchu_ordered_time_char AS order_received_time
    ,a.juchu_card_corporation AS credit_card_company
    ,a.juchu_shipping_preferd_date_int AS requested_delivery_date
    ,a.juchu_shipping_preferd_time_code AS delivery_time
    ,a.juchu_detail_status AS order_detail_status
    ,a.juchu_set_selling_code AS set_sales_code
    ,cast(a.juchu_set_selling_flag AS BIGINT) AS set_sales_flag
    ,NULL AS integrated_date
    ,a.purchase_order_id AS shipped_order_id
    ,cast(a.shipping_flag AS INTEGER) AS shipped_flag
    ,a.purchase_member_id AS shipped_member_id
    ,a.purchase_transaction_id AS transaction_id
    ,a.purchase_g1_ims_store_id_6 AS g1_ims_store_id_6
    ,a.purchase_order_datetime AS order_date_jst
    ,a.purchase_shipped_datetime AS shipped_date_jst
    ,a.purchase_tran_no AS tran_no
    ,a.purchase_tran_type AS shipped_tran_type
    ,a.purchase_status AS status
    ,a.purchase_value AS VALUE
    ,a.purchase_update_datetime AS update_date_jst
    /*,a.purchase_type AS TYPE */
    ,a.purchase_type AS type
    ,a.purchase_item_cd AS shipped_item_cd
    ,a.purchase_name AS name
    ,a.purchase_item_status AS item_status
    ,a.purchase_unit_retail AS unit_retail_price
    ,a.purchase_quantity AS shipped_quantity
    ,a.purchase_price AS shipped_sales_amount_after_discount
    ,nvl(a.purchase_disc_price, 0) AS discount_amount
    ,a.purchase_price_before_discount AS shipped_sales_amount_before_discount
    ,a.purchase_unit_cost_plu AS unit_cost_plu
    ,a.purchase_sum_cost_plu AS sum_cost_plu
    ,b.juchu_payment_type AS payment_type
    ,b.juchu_channel_code AS channel_code
    ,b.juchu_channel_name AS channel_name
    ,b.juchu_gift_type AS gift_division
    ,b.juchu_home_address AS orderers_address
    ,cast(b.juchu_home_zipcode AS BIGINT) AS postal_code_of_orderers_address
    ,b.juchu_shipping_address AS shipping_address_prefectures
    ,cast(b.juchu_shipping_zipcode AS BIGINT) AS shipping_postal_code
    ,a.uid
    ,a.gender_cd
    ,a.gender
    ,a.dateofbirth
    ,a.addr_pref
    ,a.zipcode
    ,d.dept_idnt
    ,d.item_uda_d_sbc1_idnt
    ,d.max_cost AS unit_cost_item_cd
    ,d.max_cost * shipped_quantity AS sum_cost_item_cd
    ,CASE
        WHEN unit_cost_plu = 999988 THEN unit_cost_item_cd
        WHEN unit_cost_plu = 999999 THEN unit_cost_item_cd
        WHEN unit_cost_plu IS NULL THEN unit_cost_item_cd
        ELSE unit_cost_plu
    END AS unit_cost
    ,CASE
        WHEN unit_cost_plu = 999988 THEN sum_cost_item_cd
        WHEN unit_cost_plu = 999999 THEN sum_cost_item_cd
        WHEN unit_cost_plu IS NULL THEN sum_cost_item_cd
        ELSE sum_cost_plu
    END AS sum_cost
    ,CASE
        WHEN e.delivery_type IS NULL THEN 'Home'
        ELSE e.delivery_type
    END AS 受取方法
    ,e.receiving_store_place
    ,e.receiving_store_code
    ,e.corporate_name
    ,e.pickup_store_name
    ,e.receipt_date
    ,e.returned_flag
    ,CASE
        WHEN f.booking_store_code IS NULL THEN 0
        ELSE 1
    END AS store_inventory_flag
    ,CASE
        WHEN f.booking_store_code IS NULL THEN 'EC'
        ELSE '店舗'
    END AS 在庫引当先
    ,f.booking_store_code
    ,f.shipping_store_code
    ,f.receiptplace_type_code
    ,f.corporate_name AS corporate_name_inventory
    ,f.booking_store_name
    ,f.shipping_store_name
    ,to_char(e.receipt_date, 'yyyymmdd') || '-' || a.uid AS order_key
    ,CASE
        WHEN(
            payment_type = '店舗'
        AND 在庫引当先 = 'EC'
        AND 受取方法 = 'GU'
        ) THEN 5
        WHEN(
            payment_type = '店舗'
        AND 在庫引当先 = 'EC'
        AND 受取方法 <> 'GU'
        ) THEN 6
        WHEN(
            payment_type <> '店舗'
        AND 在庫引当先 = '店舗'
        AND 受取方法 = 'GU'
        ) THEN 7
        WHEN(
            payment_type <> '店舗'
        AND 在庫引当先 = '店舗'
        AND 受取方法 <> 'GU'
        ) THEN 8
        WHEN(
            payment_type <> '店舗'
        AND 在庫引当先 = 'EC'
        AND 受取方法 = 'GU'
        ) THEN 9
        WHEN(
            payment_type <> '店舗'
        AND 在庫引当先 = 'EC'
        AND 受取方法 <> 'GU'
        ) THEN 10
        ELSE 0
    END AS accounting_channel
    ,CASE
        WHEN accounting_channel = 5
    AND TYPE = 'ITEM' THEN shipped_sales_amount_before_discount * 0.2
        WHEN accounting_channel = 6
    AND TYPE = 'ITEM' THEN shipped_sales_amount_before_discount * 0.15
        WHEN accounting_channel = 9
    AND TYPE = 'ITEM' THEN shipped_sales_amount_before_discount * 0.05
        ELSE 0
    END AS fee_to_store
    ,CASE
        WHEN accounting_channel = 7
    AND TYPE = 'ITEM' THEN shipped_sales_amount_before_discount * 0.025
        WHEN accounting_channel = 8
    AND TYPE = 'ITEM' THEN shipped_sales_amount_before_discount * 0.025
        ELSE 0
    END AS fee_to_EC
    ,pl.ec_prev_order AS previous_order_date
    ,pl.ec_diff_day AS daycount_from_previous_order_date
    ,pl.ec_diff_month AS monthcount_from_previous_order_date
FROM
    c_item_all AS a
    LEFT JOIN
        f_order_info AS b
    ON  a.pk_order_id = b.pk_juchu_order_id
    LEFT JOIN
        gu_free.bi_item_cd_cost_info AS d
    ON  a.pk_item_cd = d.item_cd
        /* 前受金(店舗在庫引当の場合に漏れる為、再度JOIN) */
    LEFT JOIN
        gu_free.bi_receiptplace_info AS e
    ON  a.pk_order_id = e.order_no
        /* 前受金(店舗在庫引当の場合に漏れる為、再度JOIN) */
    LEFT JOIN
        gu_free.bi_store_inventory_allocation_gu AS f
    ON  a.pk_order_id = f.order_no
    LEFT JOIN
        gu_free.c_purchase_lag AS pl
    ON  a.uid = pl.uid
    --店舗購入を除外
AND a.pk_order_id <> ''
AND a.pk_order_id = pl.order_id
;
ANALYZE gu_free.bi_fact_order_item
;
COMMIT
;