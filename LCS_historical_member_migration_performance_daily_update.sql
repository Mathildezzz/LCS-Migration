delete from tutorial.mz_lcs_historical_member_migration_performance;  -- for the subsequent update
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
        trans.order_paid_time
FROM edw.f_member_order_detail trans
    WHERE is_rrp_sales_type = 1 
      AND if_eff_order_tag IS TRUE
      AND crm_member_id IS NOT NULL
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
SELECT partner,
       member_detail_id,
       MIN(DATE(min_dt))        AS belong_by_partner_dt
   FROM (
              select   
                      CASE WHEN UPPER(eff_reg_channel) IN ('LCS UNIFUN (CS)', 'LCS UNIFUN (SY)', 'LCS UNIFUN') THEN 'LCS UNIFUN'
                             WHEN UPPER(eff_reg_channel) IN ('LCS XJM (CD)', 'LCS XJM (ZZ)', 'LCS XJM') THEN 'LCS XJM'
                             WHEN UPPER(eff_reg_channel) IN ('LCS MCFJ (FJ)', 'LCS MCSZ (SZ)') THEN 'LCS MCFJ (FJ) & LCS MCSZ (SZ)'
                             ELSE UPPER(eff_reg_channel)
                             END                      AS partner,
                      member_detail_id                AS member_detail_id,
                      MIN(join_time)                  AS min_dt
                from edw.d_member_detail
                where 1 = 1
                and eff_reg_channel LIKE '%LCS%'
                and join_time < current_date -- SET CUTOFF TIME
                GROUP BY 1,2
                union 
                SELECT  
                    CASE WHEN trans.distributor_name IN ('LCS UNIFUN (CS)', 'LCS UNIFUN (SY)') THEN 'LCS UNIFUN'
                         WHEN trans.distributor_name IN ('LCS XJM (CD)', 'LCS XJM (ZZ)') THEN 'LCS XJM'
                         WHEN trans.distributor_name IN ('LCS MCFJ (FJ)', 'LCS MCSZ (SZ)') THEN 'LCS MCFJ (FJ) & LCS MCSZ (SZ)'
                         ELSE trans.distributor_name
                         END      AS partner,
                    crm_member_id AS member_detail_id,
                    MIN(date_id)  AS min_dt
                FROM edw.f_member_order_detail trans
                  WHERE 1 =1
                  AND is_member = '是'
                  AND is_rrp_sales_type = 1
                  AND if_eff_order_tag IS TRUE
                  AND trans.distributor_name LIKE '%LCS%'
                  GROUP BY 1,2
          )
          GROUP BY 1,2
),

wecom_by_distributor AS (
select
    -- shopper attributes
    pr.member_detail_id
    ,rel.external_user_unionid
    
    -- store staff attributes
    ,rel.staff_ext_id
    ,staff.staff_name
    ,staff.lego_store_code
    ,staff.store_name
    ,staff.distributor AS distributor_name
    ,staff.partner
    ,CASE WHEN distributor IN ('LCS UNIFUN (CS)', 'LCS UNIFUN (SY)') THEN 'LCS UNIFUN'
          WHEN distributor IN ('LCS XJM (CD)', 'LCS XJM (ZZ)') THEN 'LCS XJM'
          WHEN distributor IN ('LCS MCFJ (FJ)', 'LCS MCSZ (SZ)') THEN 'LCS MCFJ (FJ) & LCS MCSZ (SZ)'
          ELSE distributor
    END                AS partner_customized
    ,staff.region
    ,staff.channel

    -- friend relationship creation attributes
    ,rel.relation_created_at
    ,rel.relation_source as created_type  --0：在职添加，1：继承/内部成员共享

    
    -- friend relationship deletion attributes
    ,rel.relation_deleted
    
from edw.f_sa_staff_external_user_relation_detail rel
left join edw.d_sa_staff_info staff
    on rel.staff_ext_id = staff.staff_ext_id
-- 目前CA金表获取会员ID使用的是edw.f_crm_thirdparty_bind_detail，建议先保持一致，便于与FR看到的统计表对齐。未来会迁移到以下CRM银表：
-- left join edw.f_platform_relationship pr
--     on pr.platform = 'WMP' 
--     and pr.wmp_union_id = rel.external_user_unionid
left join edw.f_crm_thirdparty_bind_detail pr
    on pr.id_type = 'unionId'
    and pr.thirdparty_app_id = 4
    and pr.id_value = rel.external_user_unionid
where 1=1
and staff.lego_store_code is not null
AND relation_deleted = 0
AND channel = 'LCS'
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
   base.if_neither_belong_to_lcs_nor_add_wecom_20231224_lcs_ttl,
  CASE WHEN DATE(mbr.join_time) <= '2023-12-24' THEN 1 ELSE 0 END                  AS if_joined_lego_20231224,            --- added
   
  ------------------------ 是否加入乐高 以及是否归属
   
  CASE WHEN mbr.member_detail_id IS NOT NULL THEN 1 ELSE 0 END                      AS if_joined_lego,
  CASE WHEN mbr.member_detail_id IS NOT NULL THEN right ('000000000'+ cast (mbr.member_detail_id as varchar),9) ELSE NULL    END member_detail_id,
  CASE WHEN member_base.member_detail_id IS NOT NULL THEN 1 ELSE 0 END              AS if_lcs_member_base,
  CASE WHEN member_base_partner.member_detail_id IS NOT NULL THEN 1 ELSE 0 END      AS if_partner_member_base,
   
------------------------- 如果归属，展示profile信息
  CASE WHEN member_base.member_detail_id IS NOT NULL THEN DATE(mbr.join_time) ELSE NULL END                              AS join_dt,
  CASE WHEN member_base.member_detail_id IS NOT NULL THEN member_base.belong_to_lcs_dt ELSE NULL END                     AS belong_to_lcs_dt,     --- added
   
  CASE WHEN member_base_partner.member_detail_id IS NOT NULL THEN member_base_partner.belong_by_partner_dt ELSE NULL END AS belong_to_partner_dt, --- added
  CASE WHEN member_base_partner.member_detail_id IS NOT NULL THEN mbr.eff_reg_channel ELSE NULL END                      AS eff_reg_channel,
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
  last_purchase.mbr_sales                                     AS last_purchase_in_partner_rrp_sales,

-------------------------- 是否加本客户企微
  CASE WHEN wecom.member_detail_id IS NOT NULL THEN 1 ELSE 0 END AS add_partner_wecom,
-------------------------------------------------------------------
  to_char(getdate(), 'yyyymmdd')                              AS dl_batch_date,
  getdate()                                                   AS dl_load_time
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
  LEFT JOIN (SELECT member_detail_id, MIN(belong_by_partner_dt) AS belong_to_lcs_dt FROM member_base_partner GROUP BY 1) member_base
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
  LEFT JOIN (SELECT CASE WHEN trans.distributor_name IN ('LCS UNIFUN (CS)', 'LCS UNIFUN (SY)') THEN 'LCS UNIFUN'
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
        AND member_base_partner.partner = last_purchase.partner
  LEFT JOIN (SELECT DISTINCT member_detail_id, partner_customized FROM wecom_by_distributor) wecom
         ON member_base_partner.member_detail_id::integer = wecom.member_detail_id::integer
        AND member_base_partner.partner = wecom.partner_customized;
        
        
    