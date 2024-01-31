
-- 第二类人：截止12.24 加入乐高，但是未归属且未加企微的人
DROP TABLE IF EXISTS tutorial.mz_historical_not_belong_to_lcs_and_not_add_wecom_20231224_by_distributor;
CREATE TABLE tutorial.mz_historical_not_belong_to_lcs_and_not_add_wecom_20231224_by_distributor AS
SELECT 
      base.distributor_name,
      mbr.member_detail_id    AS member_detail_id
 FROM tutorial.lcs_historical_member_info_roy_v2 base
 LEFT JOIN( select
                cast(member_detail_id as varchar) as member_detail_id,
                phone
              from report.member_phone
              where  phone_source_type = 0
            ) phone_list
       ON cast(base.phone as varchar) = cast(phone_list.phone as varchar)
LEFT JOIN (SELECT DISTINCT member_detail_id 
             FROM edw.d_member_detail
             WHERE DATE(join_time) < '2023-12-25'
           ) mbr
       ON phone_list.member_detail_id = mbr.member_detail_id
LEFT JOIN (  select DISTINCT member_detail_id, 
                     CASE WHEN UPPER(eff_reg_channel) IN ('LCS UNIFUN','LCS UNIFUN (CS)','LCS UNIFUN (SY)') THEN 'LCS UNIFUN (CS) and LCS UNIFUN (SY)'
                          WHEN UPPER(eff_reg_channel) IN ('LCS JIMUU', 'LCS XJM (CD)', 'LCS XJM (ZZ)') THEN 'LCS XJM (CD) and LCS XJM (ZZ)'
                          WHEN UPPER(eff_reg_channel) IN ('LCS MCFJ (FJ)', 'LCS MCSZ (SZ)') THEN 'LCS MCFJ (FJ) and LCS MCSZ (SZ)'
                     ELSE eff_reg_channel END AS distributor_name
                from edw.d_member_detail
                where 1 = 1
                and UPPER(eff_reg_channel) LIKE '%LCS%'
                and DATE(join_time) < '2023-12-25' -- SET CUTOFF TIME
                union 
                SELECT DISTINCT crm_member_id AS member_detail_id, 
                 CASE WHEN UPPER(distributor_name) IN ('LCS UNIFUN (CS)','LCS UNIFUN (SY)') THEN 'LCS UNIFUN (CS) and LCS UNIFUN (SY)'
                          WHEN UPPER(distributor_name) IN ('LCS JIMUU', 'LCS XJM (CD)', 'LCS XJM (ZZ)') THEN 'LCS XJM (CD) and LCS XJM (ZZ)'
                          WHEN UPPER(distributor_name) IN ('LCS MCFJ (FJ)', 'LCS MCSZ (SZ)') THEN 'LCS MCFJ (FJ) and LCS MCSZ (SZ)'
                     ELSE distributor_name END AS distributor_name
                FROM edw.f_member_order_detail
                  WHERE 1 =1
                  AND is_member = '是'
                  AND is_rrp_sales_type = 1
                  AND if_eff_order_tag IS TRUE
                --   AND distributor_name IN {}
                  AND DATE(date_id) < '2023-12-25'
           ) belong
       ON mbr.member_detail_id::integer = belong.member_detail_id::integer  
      AND base.distributor_name = belong.distributor_name  
LEFT JOIN (SELECT * FROM tutorial.historical_memberid_join_wecom_part1_20231225 WHERE is_wecom_member = 1
             UNION ALL
             SELECT * FROM tutorial.historical_memberid_join_wecom_part2_mcfj_mcsz_20231225  WHERE is_wecom_member = 1
             UNION ALL
             SELECT distributor_name,member_detail_id AS member_id, is_wecom_member FROM tutorial.mcwh_wecom_memberid_20231225 WHERE is_wecom_member = 1
           ) wecom
        ON mbr.member_detail_id::integer = wecom.member_id::integer
       AND base.distributor_name = wecom.distributor_name
WHERE mbr.member_detail_id IS NOT NULL 
  AND belong.member_detail_id IS NULL
  AND wecom.member_id IS NULL ;