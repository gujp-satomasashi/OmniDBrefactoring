--セッション中のキャッシュ設定をoffに設定
set
enable_result_cache_for_session = off
;
--TRUNCATE gu_free.bi_cube_order;
TRUNCATE gu_free.bi_cube_order_new_061201
;
INSERT INTO gu_free.bi_cube_order_new_061201 WITH order_wo_nmitem AS(
    SELECT
        a.order_id
        ,a.tran_type
        ,a.order_date
        ,nvl(count(a.juchu_item_cd), 0) as juchu_item_cnt
        ,nvl(sum(a.juchu_quantity), 0) as sum_juchu_qty
        ,nvl(sum(a.juchu_total_amount), 0) as sum_juchu_amount
        ,a.shipped_member_id
        ,a.g1_ims_store_id_6
        ,a.shipped_date_jst
        ,a.type
        ,nvl(count(a.shipped_item_cd), 0) as shipped_item_cnt
        ,nvl(sum(a.shipped_quantity), 0) as sum_shipped_qty
        ,nvl(sum(a.shipped_sales_amount_before_discount), 0) as sum_shipped_sales_amount_before_discount
        ,nvl(sum(a.discount_amount), 0) as sum_discount_amount
        ,nvl(sum(a.shipped_sales_amount_after_discount), 0) as sum_shipped_sales_amount_after_discount
        ,a.payment_type
        ,a.channel_code
        ,a.channel_name
        ,a.gift_division
        ,a.orderers_address
        ,a.postal_code_of_orderers_address
        ,a.shipping_address_prefectures
        ,a.shipping_postal_code
        ,a.uid
        ,a.gender_cd
        ,a.gender
        ,a.dateofbirth
        ,a.addr_pref
        ,a.zipcode
        ,nvl(sum(a.sum_cost), 0) as sum_cost
        ,a.受取方法
        ,a.corporate_name
        ,a.pickup_store_name
        ,a.store_inventory_flag
        ,a.在庫引当先
        ,a.booking_store_code
        ,a.receiptplace_type_code
        ,a.corporate_name_inventory
        ,a.booking_store_name
        ,a.order_key
        ,a.accounting_channel
        ,nvl(sum(a.fee_to_store), 0) as sum_fee_to_store
        ,nvl(sum(a.fee_to_ec), 0) as sum_fee_to_ec
        ,a.previous_order_date
        ,a.daycount_from_previous_order_date
        ,a.monthcount_from_previous_order_date
    FROM
        --gu_free.bi_fact_order_item AS a
        gu_free.bi_fact_order_item_new_061201 AS a
    WHERE
        (
            a.type = 'ITEM'
        OR  a.type is null
        )
    GROUP BY
        a.order_id
        ,a.tran_type
        ,a.order_date
        ,a.shipped_member_id
        ,a.g1_ims_store_id_6
        ,a.shipped_date_jst
        ,a.type
        ,a.payment_type
        ,a.channel_code
        ,a.channel_name
        ,a.gift_division
        ,a.orderers_address
        ,a.postal_code_of_orderers_address
        ,a.shipping_address_prefectures
        ,a.shipping_postal_code
        ,a.uid
        ,a.gender_cd
        ,a.gender
        ,a.dateofbirth
        ,a.addr_pref
        ,a.zipcode
        ,a.受取方法
        ,a.corporate_name
        ,a.pickup_store_name
        ,a.store_inventory_flag
        ,a.在庫引当先
        ,a.booking_store_code
        ,a.receiptplace_type_code
        ,a.corporate_name_inventory
        ,a.booking_store_name
        ,a.order_key
        ,a.accounting_channel
        ,a.previous_order_date
        ,a.daycount_from_previous_order_date
        ,a.monthcount_from_previous_order_date
)
,nmitem AS(
    SELECT
        purchase_order_id AS order_id
        --,pk_purchase_transaction_id AS transaction_id
        ,pk_purchase_tran_type AS tran_type
        ,purchase_type AS type
        /* 商品外売上 */
        ,SUM(nmitem_price) AS 商品外売上
        /* 97ダイレクト送料 */
        ,SUM(nmitem_shipping_cost) AS 送料
        /* 93前受金 */
        ,SUM(nmitem_advance_payment) AS 前受金
        /* 82前受金店舗在庫引当 */
        ,SUM(nmitem_advance_payment_store_inventry_reserve) AS 前受金店舗在庫引当
        /* その他商品外売上 */
        ,SUM(nmitem_other_price) AS その他商品外売上
    FROM
        --gu_free.v_c_ordershipping_nmitem
        (
            SELECT
                fs.purchase_order_id
                ,fs.pk_purchase_transaction_id
                ,fs.pk_purchase_tran_type
                ,fs.purchase_type
                ,fs.purchase_ec_flag
                ,sum(fs.purchase_price) AS nmitem_price
                ,sum(
                    CASE
                        WHEN fs.purchase_name::text = 'ダイレクト送料'::text THEN fs.purchase_price
                        ELSE NULL::numeric
                    END
                ) AS nmitem_shipping_cost
                ,sum(
                    CASE
                        WHEN fs.purchase_name::text = '前受金'::text THEN fs.purchase_price
                        ELSE NULL::numeric
                    END
                ) AS nmitem_advance_payment
                ,sum(
                    CASE
                        WHEN fs.purchase_name::text = '前受金（店舗在庫引当)'::text THEN fs.purchase_price
                        ELSE NULL::numeric
                    END
                ) AS nmitem_advance_payment_store_inventry_reserve
                ,sum(
                    CASE
                        WHEN fs.purchase_name::text <> 'ダイレクト送料'::text
                    AND fs.purchase_name::text <> '前受金'::text
                    AND fs.purchase_name::text <> '前受金（店舗在庫引当)'::text THEN fs.purchase_price
                        ELSE NULL::numeric
                    END
                ) AS nmitem_other_price
            FROM
                gu_free.f_shipping fs
            WHERE
                fs.purchase_status::text = 'P'::text
            AND fs.purchase_type::text = 'NMITEM'::text
            GROUP BY
                fs.purchase_order_id
                ,fs.pk_purchase_transaction_id
                ,fs.pk_purchase_tran_type
                ,fs.purchase_type
                ,fs.purchase_ec_flag
        ) as a
    WHERE
        /* ECのみ */
        purchase_ec_flag IS TRUE
    GROUP BY
        order_id
        ,tran_type
        ,type
)
SELECT
    a.order_id
    ,a.tran_type
    ,a.order_date
    ,a.juchu_item_cnt
    ,a.sum_juchu_qty
    ,a.sum_juchu_amount
    ,a.shipped_member_id
    ,a.g1_ims_store_id_6
    ,a.shipped_date_jst
    ,a.type
    ,a.shipped_item_cnt
    ,a.sum_shipped_qty
    ,a.sum_shipped_sales_amount_before_discount
    ,a.sum_discount_amount
    ,a.sum_shipped_sales_amount_after_discount
    ,a.payment_type
    ,a.channel_code
    ,a.channel_name
    ,a.gift_division
    ,a.orderers_address
    ,a.postal_code_of_orderers_address
    ,a.shipping_address_prefectures
    ,a.shipping_postal_code
    ,a.uid
    ,a.gender_cd
    ,a.gender
    ,a.dateofbirth
    ,a.addr_pref
    ,a.zipcode
    ,a.sum_cost
    ,a.受取方法
    ,a.corporate_name
    ,a.pickup_store_name
    ,a.store_inventory_flag
    ,a.在庫引当先
    ,a.booking_store_code
    ,a.receiptplace_type_code
    ,a.corporate_name_inventory
    ,a.booking_store_name
    ,a.order_key
    ,a.accounting_channel
    ,a.sum_fee_to_store
    ,a.sum_fee_to_ec
    ,previous_order_date
    ,daycount_from_previous_order_date
    ,monthcount_from_previous_order_date
    ,CASE
        WHEN a.tran_type = 'SALE' THEN a.sum_shipped_qty * 30
        ELSE 0
    END as 入荷コスト
    ,CASE
        WHEN a.tran_type = 'SALE' THEN a.sum_shipped_qty * 10
        ELSE abs(a.sum_shipped_qty) * 80
    END as 加工コスト
    /* 返品コストはすべてここに計上 */
    ,CASE
        WHEN a.tran_type = 'SALE' THEN a.sum_shipped_qty * 26
        ELSE 0
    END as ピッキングコスト
    ,CASE
        WHEN a.tran_type = 'SALE' THEN 52
        ELSE 0
    END as 出荷コスト
    ,CASE
        WHEN a.tran_type = 'SALE' THEN 152
        ELSE 0
    END as 管理固定コスト
    ,CASE
        WHEN a.tran_type = 'SALE' THEN 99
        ELSE 0
    END as 倉庫振替コスト
    ,入荷コスト + 加工コスト + ピッキングコスト + 出荷コスト + 管理固定コスト + 倉庫振替コスト as 倉庫関連費用
    ,CASE
        WHEN a.tran_type = 'SALE' THEN 460
        ELSE 0
    END as 配送コスト
    ,倉庫関連費用 + 配送コスト as 物流関連コスト
    ,b.tran_type as nmitem_tran_type
    ,b.type as nmitem_type
    ,nvl(b."商品外売上", 0) as 商品外売上
    ,nvl(b."送料", 0) as 送料
    /* nvl(b."前受金",0) as 前受金, */
    ,0 as 前受金
    ,nvl(b."前受金店舗在庫引当", 0) as 前受金店舗在庫引当
    ,nvl(b."その他商品外売上", 0) as その他商品外売上
    ,c.uid as store_uid
    ,CASE
        WHEN store_uid is null THEN 0
        ELSE 1
    END as easy_purchase_flag
FROM
    order_wo_nmitem AS a
    LEFT JOIN
        nmitem AS b
    ON  a.order_id = b.order_id
    AND a.tran_type = b.tran_type
    LEFT JOIN
        gu_free.bi_store_emp_account AS c
    ON  a.uid = c.uid
;
COMMIT
;