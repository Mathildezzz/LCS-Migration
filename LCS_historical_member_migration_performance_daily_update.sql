truncate tutorial.mz_lcs_historical_member_migration_performance; -- for the subsequent update
insert into tutorial.mz_lcs_historical_member_migration_performance

WITH all_purchase_rk AS (
  SELECT partner, 
        crm_member_id,
        original_order_id,
        order_paid_time,
        ROW_NUMBER () OVER (PARTITION BY partner,crm_member_id ORDER BY order_paid_time DESC) AS rk
FROM (
SELECT DISTINCT 
        CASE WHEN trans.distributor_name IN ('LCS UNIFUN (CS)', 'LCS UNIFUN (SY)') THEN 'LCS UNIFUN'
             WHEN trans.distributor_name IN ('LCS XJM (CD)', 'LCS XJM (ZZ)') THEN 'LCS XJM'
             WHEN trans.distributor_name IN ('LCS MCFJ (FJ)', 'LCS MCSZ (SZ)') THEN 'LCS MCFJ (FJ) & LCS MCSZ (SZ)'
             ELSE trans.distributor_name
             END                   AS partner,
        trans.crm_member_id,
        trans.original_order_id,
        orders.order_paid_time
FROM edw.f_member_order_detail trans
LEFT JOIN (SELECT original_store_code, 
                  original_order_id,
                  MIN(order_paid_time) AS order_paid_time
            FROM edw.f_lcs_order_detail
             GROUP BY  1,2
        ) orders
      ON trans.original_order_id = CONCAT(orders.original_store_code, orders.original_order_id)
    WHERE is_rrp_sales_type = 1 
      AND if_eff_order_tag IS TRUE
      )
),


last_purchase AS (
SELECT 
   DISTINCT 
     CASE WHEN trans.distributor_name IN ('LCS UNIFUN (CS)', 'LCS UNIFUN (SY)') THEN 'LCS UNIFUN'
             WHEN trans.distributor_name IN ('LCS XJM (CD)', 'LCS XJM (ZZ)') THEN 'LCS XJM'
             WHEN trans.distributor_name IN ('LCS MCFJ (FJ)', 'LCS MCSZ (SZ)') THEN 'LCS MCFJ (FJ) & LCS MCSZ (SZ)'
             ELSE trans.distributor_name
             END                            AS partner,
   trans.crm_member_id,
   trans.original_order_id,
   trans.original_store_code,
   ps.store_name,
   MIN(trans.date_id)                   AS date_id,
   listagg(DISTINCT lego_sku_id,', ')            AS lego_sku_id,
   listagg(DISTINCT lego_sku_name_cn,', ')       AS lego_sku_name_cn,
   sum(case when is_member = '是' AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member = '是' AND sales_qty < 0 then abs(order_rrp_amt) else 0 end) AS mbr_sales
 FROM edw.f_member_order_detail trans
LEFT JOIN edw.d_dl_phy_store as ps 
   ON trans.original_store_code = ps.lego_store_code 
INNER JOIN (SELECT * FROM all_purchase_rk WHERE rk = 1) all_purchase_rk
        ON all_purchase_rk.original_order_id = trans.original_order_id
       AND all_purchase_rk.partner = CASE WHEN trans.distributor_name IN ('LCS UNIFUN (CS)', 'LCS UNIFUN (SY)') THEN 'LCS UNIFUN'
             WHEN trans.distributor_name IN ('LCS XJM (CD)', 'LCS XJM (ZZ)') THEN 'LCS XJM'
             WHEN trans.distributor_name IN ('LCS MCFJ (FJ)', 'LCS MCSZ (SZ)') THEN 'LCS MCFJ (FJ) & LCS MCSZ (SZ)'
             ELSE trans.distributor_name
             END      
    WHERE is_rrp_sales_type = 1 
      AND if_eff_order_tag IS TRUE
GROUP BY 1,2,3,4,5
),

member_base_partner AS (
              select DISTINCT  
                      CASE WHEN UPPER(eff_reg_channel) IN ('LCS UNIFUN (CS)', 'LCS UNIFUN (SY)', 'LCS UNIFUN') THEN 'LCS UNIFUN'
                             WHEN UPPER(eff_reg_channel) IN ('LCS XJM (CD)', 'LCS XJM (ZZ)', 'LCS XJM') THEN 'LCS XJM'
                             WHEN UPPER(eff_reg_channel) IN ('LCS MCFJ (FJ)', 'LCS MCSZ (SZ)') THEN 'LCS MCFJ (FJ) & LCS MCSZ (SZ)'
                             ELSE UPPER(eff_reg_channel)
                             END                      AS partner,
                      member_detail_id                AS member_detail_id
                from edw.d_member_detail
                where 1 = 1
                and join_time < current_date -- SET CUTOFF TIME
                union 
                SELECT DISTINCT 
                    CASE WHEN trans.distributor_name IN ('LCS UNIFUN (CS)', 'LCS UNIFUN (SY)') THEN 'LCS UNIFUN'
                         WHEN trans.distributor_name IN ('LCS XJM (CD)', 'LCS XJM (ZZ)') THEN 'LCS XJM'
                         WHEN trans.distributor_name IN ('LCS MCFJ (FJ)', 'LCS MCSZ (SZ)') THEN 'LCS MCFJ (FJ) & LCS MCSZ (SZ)'
                         ELSE trans.distributor_name
                         END      AS partner,
                    crm_member_id AS member_detail_id
                FROM edw.f_member_order_detail trans
                  WHERE 1 =1
                  AND is_member = '是'
                  AND is_rrp_sales_type = 1
                  AND if_eff_order_tag IS TRUE
)


SELECT 

--------------------------- 历史会员 历史情况
   base.phone                                                       AS phone,
   base.partner,
   base.distributor,
   base.historical_join_dt,
   base.historical_join_channel,
   base.historical_vip_tier,
   base.historical_city,
   base.if_historical_high_vip_tier,
   base.if_neither_belong_to_lcs_nor_add_wecom_20231224_by_partner,
   
  ------------------------ 是否加入乐高 以及是否归属
   
   CASE WHEN mbr.member_detail_id IS NOT NULL THEN 1 ELSE 0 END                      AS if_joined_lego,
   CASE WHEN mbr.member_detail_id IS NOT NULL THEN right ('000000000'+ cast (mbr.member_detail_id as varchar),9) ELSE NULL    END member_detail_id,
   CASE WHEN member_base.member_detail_id IS NOT NULL THEN 1 ELSE 0 END              AS if_lcs_member_base,
   CASE WHEN member_base_partner.member_detail_id IS NOT NULL THEN 1 ELSE 0 END      AS if_partner_member_base,
   
------------------------- 如果归属，展示profile信息
   CASE WHEN member_base.member_detail_id IS NOT NULL THEN DATE(mbr.join_time) ELSE NULL END                              AS join_dt,
      CASE WHEN member_base_partner.member_detail_id IS NOT NULL THEN mbr.eff_reg_channel ELSE NULL END                   AS eff_reg_channel,
   CASE WHEN member_base_partner.member_detail_id IS NOT NULL THEN mbr.eff_reg_store ELSE NULL END                        AS eff_reg_store,
    CASE WHEN member_base_partner.member_detail_id IS NOT NULL THEN mbr.store_name ELSE NULL END                          AS eff_reg_store_name,  
   CASE WHEN member_base_partner.member_detail_id IS NOT NULL THEN mbr.tier_code ELSE NULL END                            AS tier_code, 
   CASE WHEN member_base_partner.member_detail_id IS NOT NULL THEN mbr.point ELSE NULL  END                               AS point,
   
   
   ---------------------- 如果加入会员，是否在LCS转化
   
   CASE WHEN trans_orders_in_lcs.crm_member_id IS NOT NULL THEN 1 ELSE 0 END AS ever_purchased_in_lcs,
   trans_orders_in_lcs.order_count                                    AS ttl_orders_in_lcs,
   trans_rrp_in_lcs.mbr_sales                                         AS ttl_rrp_sales_in_lcs,
  
   
   ---------------------- 如果加入会员，是否在本客户转化
   
   
   CASE WHEN trans_orders_by_partner.crm_member_id IS NOT NULL THEN 1 ELSE 0 END AS ever_purchased_in_partner,
   trans_orders_by_partner.order_count                                    AS ttl_orders_in_partner,
   trans_rrp_by_partner.mbr_sales                                         AS ttl_rrp_sales_in_partner,
   
------------------------- 在本客户最后一次购买的情况，用于后续customize沟通
   last_purchase.date_id                                       AS last_purchase_in_partner_dt,
   last_purchase.original_store_code                           AS last_purchase_in_partner_store_code,
   last_purchase.store_name                                    AS last_purchase_in_partner_store_name,
   last_purchase.lego_sku_id                                   AS last_purchase_in_partner_sku_id,
   last_purchase.lego_sku_name_cn                              AS last_purchase_in_partner_sku_name_cn,
   last_purchase.mbr_sales                                     AS last_purchase_in_partner_rrp_sales
  FROM tutorial.mz_lcs_historical_member_base base
--   LEFT JOIN (  SELECT DISTINCT member_detail_id,
--                     phone
--                 from report.member_phone phone_list
--                 where phone_source_type = 0
--             ) phone_list
--          ON phone_list.phone = REPLACE(base.phone,',','')
LEFT JOIN ods.crm_member_phone  phone_list
         ON base.encrypt_phone = phone_list.phone
  LEFT JOIN edw.d_member_detail mbr
         ON phone_list.member_detail_id::integer = mbr.member_detail_id::integer
  LEFT JOIN (SELECT DISTINCT member_detail_id FROM member_base_partner) member_base
         ON phone_list.member_detail_id::integer = member_base.member_detail_id::integer
  LEFT JOIN member_base_partner
         ON phone_list.member_detail_id::integer = member_base_partner.member_detail_id::integer
        AND base.partner = member_base_partner.partner
 LEFT JOIN (SELECT crm_member_id,
                    COUNT(DISTINCT original_order_id) AS order_count
               FROM edw.f_member_order_detail trans
                WHERE is_rrp_sales_type = 1 
                  AND if_eff_order_tag IS TRUE
                  GROUP BY 1
            ) trans_orders_in_lcs
         ON member_base_partner.member_detail_id::integer = trans_orders_in_lcs.crm_member_id::integer
  LEFT JOIN (SELECT crm_member_id,
                    sum(case when is_member = '是' AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member = '是' AND sales_qty < 0 then abs(order_rrp_amt) else 0 end) AS mbr_sales
                 FROM edw.f_member_order_detail trans
                WHERE is_rrp_sales_type = 1 
                  GROUP BY 1
            ) trans_rrp_in_lcs
         ON member_base_partner.member_detail_id::integer = trans_rrp_in_lcs.crm_member_id::integer
  LEFT JOIN (SELECT  CASE WHEN trans.distributor_name IN ('LCS UNIFUN (CS)', 'LCS UNIFUN (SY)') THEN 'LCS UNIFUN'
                         WHEN trans.distributor_name IN ('LCS XJM (CD)', 'LCS XJM (ZZ)') THEN 'LCS XJM'
                         WHEN trans.distributor_name IN ('LCS MCFJ (FJ)', 'LCS MCSZ (SZ)') THEN 'LCS MCFJ (FJ) & LCS MCSZ (SZ)'
                         ELSE trans.distributor_name
                         END      AS partner,
                         crm_member_id,
                    COUNT(DISTINCT original_order_id) AS order_count
               FROM edw.f_member_order_detail trans
                WHERE is_rrp_sales_type = 1 
                  AND if_eff_order_tag IS TRUE
                  GROUP BY 1,2
            ) trans_orders_by_partner
         ON member_base_partner.member_detail_id::integer = trans_orders_by_partner.crm_member_id::integer
        AND member_base_partner.partner  = trans_orders_by_partner.partner
  LEFT JOIN (SELECT  CASE WHEN trans.distributor_name IN ('LCS UNIFUN (CS)', 'LCS UNIFUN (SY)') THEN 'LCS UNIFUN'
                         WHEN trans.distributor_name IN ('LCS XJM (CD)', 'LCS XJM (ZZ)') THEN 'LCS XJM'
                         WHEN trans.distributor_name IN ('LCS MCFJ (FJ)', 'LCS MCSZ (SZ)') THEN 'LCS MCFJ (FJ) & LCS MCSZ (SZ)'
                         ELSE trans.distributor_name
                         END      AS partner,
                         crm_member_id,
                    sum(case when is_member = '是' AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member = '是' AND sales_qty < 0 then abs(order_rrp_amt) else 0 end) AS mbr_sales
                 FROM edw.f_member_order_detail trans
                WHERE is_rrp_sales_type = 1 
                  GROUP BY 1,2
            ) trans_rrp_by_partner
         ON member_base_partner.member_detail_id::integer = trans_rrp_by_partner.crm_member_id::integer
        AND member_base_partner.partner  = trans_rrp_by_partner.partner
  LEFT JOIN last_purchase
         ON member_base_partner.member_detail_id::integer = last_purchase.crm_member_id::integer
        AND member_base_partner.partner = last_purchase.partner;
        
        
        
    