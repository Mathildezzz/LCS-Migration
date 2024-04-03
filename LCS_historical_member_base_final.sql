DROP TABLE IF EXISTS tutorial.mz_lcs_historical_member_base;
CREATE TABLE tutorial.mz_lcs_historical_member_base AS
WITH base AS (
SELECT DISTINCT 
        REPLACE(phone, ',','')       AS phone,
       'LCS BLH (HZ)'                AS partner,
       NULL                            AS distributor,
       NULL                            AS historical_join_dt,
       NULL                            AS historical_join_channel,
       blh_tier                        AS historical_vip_tier,
       NULL                          AS historical_city,
       CASE WHEN blh_tier IN ('VIP_1','VIP_2','VIP_3','VIP_4') THEN '1' ELSE '0' END AS if_historical_high_vip_tier
 FROM tutorial.blh_original_member_base_roy_v1 
UNION ALL
SELECT phone,
       partner,
       distributor,
       historical_join_dt,
       historical_join_channel,
       historical_vip_tier,
       historical_city,
       if_historical_high_vip_tier
     FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY phone ORDER BY historical_vip_tier_number, historical_join_dt DESC) AS rk
            FROM (
            SELECT DISTINCT 
                   REPLACE(phone_number, ',','') AS phone,
                   'LCS BRICK (XA)'              AS partner,
                   NULL                                          AS distributor,
                   CAST(DATE(historical_join_dt) AS TEXT)        AS historical_join_dt,
                   NULL                           AS historical_join_channel,
                   historical_vip_tier            AS historical_vip_tier,
                   CASE WHEN historical_vip_tier = '青铜' THEN 1
                        WHEN historical_vip_tier = '白银' THEN 2
                        WHEN historical_vip_tier = '黄金' THEN 3
                        WHEN historical_vip_tier = '铂金' THEN 4
                        WHEN historical_vip_tier = '钻石' THEN 5 
                    END                          AS historical_vip_tier_number,
                   city                          AS historical_city,
                   CASE WHEN historical_vip_tier IN ('白银','黄金','铂金','钻石') THEN '1' ELSE '0' END AS if_historical_high_vip_tier
             FROM tutorial.mz_brick_historical_member_base_final 
             ) base
        ) ranked
    WHERE ranked.rk = 1
 UNION ALL
 SELECT phone,
       partner,
       distributor,
       historical_join_dt,
       historical_join_channel,
       historical_vip_tier,
       historical_city,
       if_historical_high_vip_tier
  FROM (
             SELECT *,
                     ROW_NUMBER() OVER (PARTITION BY phone ORDER BY historical_join_dt DESC) AS rk
                FROM (
                    SELECT DISTINCT 
                           REPLACE(phone, ',','')          AS phone,
                           'LCS EBT (SH)'                AS partner,
                           NULL                           AS distributor,
                           CAST(DATE(historical_join_dt) AS TEXT)       AS historical_join_dt,
                           NULL                             AS historical_join_channel,
                           NULL                             AS historical_vip_tier,
                           NULL                             AS historical_city,
                           NULL                             AS if_historical_high_vip_tier
                     FROM tutorial.mz_ebt_historical_member_base_final
                 ) base
        ) ranked
WHERE ranked.rk = 1
 UNION ALL
 SELECT phone,
           partner,
           distributor,
           historical_join_dt,
           historical_join_channel,
           historical_vip_tier,
           historical_city,
           if_historical_high_vip_tier
   FROM (
                 SELECT *,
                        ROW_NUMBER() OVER (PARTITION BY phone ORDER BY historical_vip_tier_number, historical_join_dt DESC) AS rk
                   FROM (
                             SELECT DISTINCT 
                                   REPLACE(htd_member_base.phone, ',','') AS phone,
                                   'LCS HTD (BJ)'                AS partner,
                                   NULL                            AS distributor,
                                   NULL                                                                                                                    AS historical_join_dt,
                                   NULL                                                                                                                    AS historical_join_channel,
                                   CASE WHEN htd_high_tier.historical_vip_tier IS NOT NULL THEN htd_high_tier.historical_vip_tier ELSE '初级LEGO粉' END  AS historical_vip_tier,
                                   CASE WHEN historical_vip_tier = '初级LEGO粉' THEN 1
                                                    WHEN historical_vip_tier = '中级LEGO粉' THEN 2
                                                    WHEN historical_vip_tier = '高级LEGO粉' THEN 3
                                                    WHEN historical_vip_tier = '超级LEGO粉' THEN 4
                                                END                                                                                                        AS historical_vip_tier_number, 
                                   NULL                                                                                                                    AS historical_city,
                                   CASE WHEN historical_vip_tier IN ('中级LEGO粉','高级LEGO粉','超级LEGO粉') THEN '1' ELSE '0' END                           AS if_historical_high_vip_tier  
                             FROM tutorial.mz_HTD_historical_member_base_final_v2 htd_member_base
                             LEFT JOIN tutorial.mz_HTD_historical_high_tier_member_base_final_v2 htd_high_tier
                                    ON REPLACE(htd_member_base.phone, ',','') = REPLACE(htd_high_tier.phone, ',','')
                        ) base
        ) ranked
      WHERE ranked.rk = 1
 UNION ALL
 
 SELECT DISTINCT 
       REPLACE(phone, ',','')                              AS phone,
       'LCS JOY (HB)'                                      AS partner,
       NULL                                                  AS distributor,
       NULL                                                  AS historical_join_dt,
       historical_join_channel                             AS historical_join_channel,
       historical_vip_tier                                 AS historical_vip_tier,
       NULL                                                  AS historical_city,
       CASE WHEN historical_vip_tier IN ('黄金','铂金','钻石') THEN '1' ELSE '0' END AS if_historical_high_vip_tier  
 FROM tutorial.mz_JOY_historical_member_base_final
 UNION ALL
  SELECT DISTINCT 
      REPLACE(mobile, ',','') AS phone,
      'LCS LEWIN (NJ)'                AS partner,
      NULL                              AS distributor,
      NULL                              AS historical_join_dt,
      NULL                              AS historical_join_channel,
      grade                                 AS historical_vip_tier,
      NULL                                                  AS historical_city,
      CASE WHEN grade IN ('黄金会员','白金会员','钻石会员','黑金会员','黑金plus') THEN '1' ELSE '0' END AS if_historical_high_vip_tier  
 FROM report.lewin_member
 UNION ALL
 SELECT   DISTINCT 
    REPLACE(member_base.phone, ',','')                 AS phone,
       'LCS MCFJ (FJ) & LCS MCSZ (SZ)'                 AS partner,
       high_tier.distributor_name                      AS distributor,
       CAST(DATE(historical_join_dt) AS TEXT)                               AS historical_join_dt,
       historical_join_channel                         AS historical_join_channel,
       historical_vip_tier                             AS historical_vip_tier,
       historical_city                                 AS historical_city,
    --   CASE WHEN historical_vip_tier IN ('VIP会员') THEN '1' ELSE '0' END AS if_historical_high_vip_tier,
       CASE WHEN high_tier.phone IS NOT NULL THEN '1' ELSE '0' END AS if_historical_high_vip_tier
   FROM tutorial.mz_mcfj_mcsz_historical_member_base_final member_base
   LEFT JOIN (SELECT phone, distributor_name FROM tutorial.mz_historical_member_high_tier WHERE distributor_name IN ('LCS MCFJ (FJ)', 'LCS MCSZ (SZ)')) high_tier
          ON REPLACE(member_base.phone, ',','') = REPLACE(high_tier.phone, ',','')
   UNION ALL
   SELECT
    REPLACE(phone, ',','')                             AS phone,
       'LCS MCWH (WH)'                                 AS partner,
       NULL                                              AS distributor,
       CAST(DATE(historical_join_dt) AS TEXT)                              AS historical_join_dt,
       historical_join_channel                         AS historical_join_channel,
       NULL                                              AS historical_vip_tier,
       NULL                                              AS historical_city,
       NULL                                              AS if_historical_high_vip_tier 
    FROM tutorial.mz_mcwh_historical_member_base_final
    UNION ALL
    SELECT phone,
           partner,
           distributor,
           historical_join_dt,
           historical_join_channel,
           historical_vip_tier,
           historical_city,
           if_historical_high_vip_tier
      FROM (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY phone ORDER BY historical_vip_tier_number, historical_join_dt DESC) AS rk
                  FROM (
                          SELECT DISTINCT
                                REPLACE(phone, ',','')                         AS phone,
                               'LCS UNIFUN'                                    AS partner,
                               NULL                                              AS distributor,
                               CAST(DATE(historical_join_dt) AS TEXT)             AS historical_join_dt,
                               historical_join_channel                           AS historical_join_channel,
                               historical_vip_tier                               AS historical_vip_tier,
                               CASE WHEN historical_vip_tier = '普通会员' THEN 1
                                    WHEN historical_vip_tier = '白金会员' THEN 2
                                    WHEN historical_vip_tier = '黄金会员' THEN 3
                                    WHEN historical_vip_tier = '至尊VIP会员' THEN 4
                                END                                              AS historical_vip_tier_number, 
                               NULL                                              AS historical_city,
                               CASE WHEN historical_vip_tier IN ('黄金会员','白金会员','至尊VIP会员') THEN '1' ELSE '0' END AS if_historical_high_vip_tier 
                           FROM tutorial.mz_unifun_historical_member_base_final
                          ) base
            ) ranked
       WHERE ranked.rk = 1
   UNION ALL
   SELECT DISTINCT
       REPLACE(phone, ',','')                         AS phone,
       'LCS XJM'                                      AS partner,
       NULL                                              AS distributor,
       CAST(DATE(historical_join_dt) AS TEXT)                             AS historical_join_dt,
       historical_join_channel                         AS historical_join_channel,
       historical_vip_tier                             AS historical_vip_tier,
       NULL                                            AS historical_city,
       CASE WHEN historical_vip_tier IN ('高级会员','VIP会员') THEN '1' ELSE '0' END AS if_historical_high_vip_tier 
   FROM tutorial.mz_xjm_historical_member_base_final
   UNION ALL
   SELECT DISTINCT REPLACE(phone, ',','')                         AS phone,
            'LCS KQL (BJ)'                                        AS partner,
            NULL                                                  AS distributor,
            NULL                                                  AS historical_join_dt,
            NULL                                                  AS historical_join_channel,
            NULL                                                  AS historical_vip_tier,
            NULL                                                  AS historical_city,
            NULL                                                  AS if_historical_high_vip_tier
     FROM tutorial.mz_KQL_historical_member_base_final
   )
   
   SELECT DISTINCT 
       base.phone,
       encrypt_base.encrypt_phone,
       base.partner,
       distributor,
       historical_join_dt,
       historical_join_channel,
       historical_vip_tier,
       historical_city,
       if_historical_high_vip_tier,
       CASE WHEN cny_tracking_group_2.member_detail_id IS NOT NULL THEN 1 ELSE 0 END AS if_neither_belong_to_lcs_nor_add_wecom_20231224_by_partner,
       CASE WHEN cny_tracking_group_2_ttl.member_detail_id IS NOT NULL THEN 1 ELSE 0 END AS if_neither_belong_to_lcs_nor_add_wecom_20231224_lcs_ttl
   FROM base
   LEFT JOIN tutorial.encrypt_phone_20240131 encrypt_base
          ON base.phone = encrypt_base.phone
  LEFT JOIN ods.crm_member_phone  encrypt_all
          ON encrypt_base.encrypt_phone = encrypt_all.phone
   LEFT JOIN (
            SELECT DISTINCT 
                   CASE WHEN distributor_name IN ('LCS UNIFUN (CS) and LCS UNIFUN (SY)') THEN 'LCS UNIFUN'
                                     WHEN distributor_name IN ('LCS XJM (CD) and LCS XJM (ZZ)') THEN 'LCS XJM'
                                     WHEN distributor_name IN ('LCS MCFJ (FJ) and LCS MCSZ (SZ)') THEN 'LCS MCFJ (FJ) & LCS MCSZ (SZ)'
                                     ELSE distributor_name
                                     END      AS partner,
                    member_detail_id
              FROM tutorial.mz_historical_not_belong_to_lcs_and_not_add_wecom_20231224_by_distributor 
              ) cny_tracking_group_2
         ON encrypt_all.member_detail_id::integer = cny_tracking_group_2.member_detail_id::integer
        AND base.partner = cny_tracking_group_2.partner
    LEFT JOIN (SELECT DISTINCT member_detail_id FROM tutorial.mz_historical_not_belong_to_lcs_and_not_add_wecom_20231224_ttl) cny_tracking_group_2_ttl
           ON encrypt_all.member_detail_id::integer = cny_tracking_group_2_ttl.member_detail_id::integer;