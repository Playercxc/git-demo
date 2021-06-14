#!/bin/bash  

sqoop=/usr/local/service/sqoop/bin/sqoop-import
#sqoop=/usr/hdp/2.6.5.0-292/sqoop/bin/sqoop-import
hive=/usr/local/service/hive/bin/hive



import_data(){
$sqoop \
"-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
--connect jdbc:mysql://172.16.0.3:3306/$1?zeroDateTimeBehavior=convertToNull \
--username drch_middlegroun \
--password \$ukfCikSP3Pp \
--target-dir /origin_data/drc_pollute/$2/$do_date \
--delete-target-dir \
--query "$5 and  \$CONDITIONS" \
--fields-terminated-by '\001' \
--null-string '\\N' \
--null-non-string '\\N' \
--split-by $3 \
--hive-drop-import-delims \
--num-mappers $4 \

}


# 1 安徽
import_anhui_company(){
  import_data drc_pollute anhui_company id 1 "select 
                                                  id,
                                                  company_name,
                                                  com_qyid,
                                                  org_number,
                                                  usc_code,
                                                  zhuce_type,
                                                  address,
                                                  hangye,
                                                  wry_type,
                                                  legal_person,
                                                  lianxi_ren,
                                                  telephone,
                                                  email,
                                                  website,
                                                  lat_lng,
                                                  company_count,
                                                  ah_com_type,
                                                  url,
                                                  btime,
                                                  create_time
                                              from anhui_company where 1=1 "
}

import_anhui_paiwu(){
  import_data drc_pollute anhui_paiwu id 1 "select 
                                                  id,
                                                  filter_id,
                                                  company_name,
                                                  com_qyid,
                                                  usc_code,
                                                  jiance_type,
                                                  jiance_dian,
                                                  jiance_project,
                                                  jiance_time,
                                                  jiance_zhi,
                                                  shice_zhi,
                                                  zhesuan_zhi,
                                                  biaozhun_zhi,
                                                  xianzhi_danwei,
                                                  is_chaobiao,
                                                  chaobiao_info,
                                                  create_time,
                                                  btime
                                              from anhui_paiwu where DATE_FORMAT(btime,'%Y-%m-%d')>='$lst_date2' and DATE_FORMAT(btime,'%Y-%m-%d')<'$do_date2' "
}

# 2 北京

import_comp_beijing_ent(){
  import_data drc_pollute comp_beijing_ent id 1 "select 
                                                  id,
                                                  ent_name,
                                                  area_name,
                                                  org_type,
                                                  is_open,
                                                  url,
                                                  ent_abs,
                                                  ent_code,
                                                  legal_person,
                                                  industry,
                                                  link_man,
                                                  link_phone,
                                                  start_date,
                                                  self_test_method,
                                                  manual_test_method,
                                                  pollution_source_name,
                                                  main_product,
                                                  measures,
                                                  lead_time,
                                                  btime,
                                                  address,
                                                  province_name,
                                                  check_org,
                                                  ent_scale,
                                                  register_type,
                                                  fax,
                                                  ent_id
                                              from comp_beijing_ent where 1=1 "
}

import_comp_beijing_pollution(){
  import_data drc_pollute comp_beijing_pollution id 1 "select 
                                                  id,
                                                  ent_name,
                                                  province_name,
                                                  area_name,
                                                  org_type,
                                                  check_type,
                                                  check_point,
                                                  check_date,
                                                  source,
                                                  value,
                                                  standard_value,
                                                  unit,
                                                  up_to_standard,
                                                  times,
                                                  standard,
                                                  discharge_to,
                                                  dischage_method,
                                                  bz,
                                                  btime
                                              from comp_beijing_pollution where DATE_FORMAT(btime,'%Y-%m-%d')>='$lst_date2' and DATE_FORMAT(btime,'%Y-%m-%d')<'$do_date2' "
}

# 3 甘肃

import_comp_gansu_ent(){
  import_data drc_pollute comp_gansu_ent id 1 "select 
                                                  id,
                                                  ent_name,
                                                  ent_id,
                                                  ent_type,
                                                  lng_lat,
                                                  legal_person,
                                                  uccode,
                                                  pwxkbh,
                                                  pwxkz_date,
                                                  link_man,
                                                  link_phone,
                                                  email,
                                                  start_date,
                                                  address,
                                                  province_name,
                                                  city_name,
                                                  area,
                                                  url,
                                                  btime
                                              from comp_gansu_ent where 1=1 "
}

import_comp_gansu_pollution(){
  import_data drc_pollute comp_gansu_pollution id 8 "select 
                                                  id,
                                                  ent_name,
                                                  ent_id,
                                                  province_name,
                                                  city_name,
                                                  area,
                                                  check_type,
                                                  hanyangliang,
                                                  bzmc,
                                                  cbbs,
                                                  jcdmc,
                                                  jcrq,
                                                  liuliang,
                                                  liusu,
                                                  scfuhe,
                                                  gzfuhe,
                                                  scnd,
                                                  sfcb,
                                                  shidu,
                                                  sjfbrq,
                                                  tbsj,
                                                  wendu,
                                                  wjcyy,
                                                  xzdw,
                                                  xzxz,
                                                  x_id,
                                                  zbmc,
                                                  zsnd,
                                                  jcdjcxmbh,
                                                  url,
                                                  btime
                                              from comp_gansu_pollution where DATE_FORMAT(btime,'%Y-%m-%d')>='$lst_date2' and DATE_FORMAT(btime,'%Y-%m-%d')<'$do_date2' "
}

# 4 河南

import_henan_company(){
  import_data drc_pollute henan_company id 1 "select 
                                                  id,
                                                  filter_id,
                                                  company_name,
                                                  enp_code,
                                                  info_year,
                                                  corporation_code,
                                                  corporation_name,
                                                  region_code,
                                                  region_name,
                                                  communicate_addr,
                                                  enp_address,
                                                  direct_id,
                                                  enter_type_name,
                                                  longitude,
                                                  latitude,
                                                  btime,
                                                  create_time
                                              from henan_company where 1=1 "
}

import_henan_paiwu(){
  import_data drc_pollute henan_paiwu id 1 "select 
                                                  id           ,
                                                  filter_id    ,
                                                  company_name ,
                                                  enp_code     ,
                                                  xuhao        ,
                                                  jcrq         ,
                                                  jc_project   ,
                                                  jcdw         ,
                                                  jcjg         ,
                                                  danwei       ,
                                                  zxbz         ,
                                                  bzxz         ,
                                                  is_ok        ,
                                                  cbbs         ,
                                                  fbsj         ,
                                                  wrwpffs      ,
                                                  pfqx         ,
                                                  sjycyy       ,
                                                  jiance_type  ,
                                                  create_time  ,
                                                  btime           
                                              from henan_paiwu where DATE_FORMAT(btime,'%Y-%m-%d')>='$lst_date2' and DATE_FORMAT(btime,'%Y-%m-%d')<'$do_date2' "
}


# 5 辽宁

import_liaoning_company(){
  import_data drc_pollute liaoning_company id 1 "select 
                                                  id              ,
                                                  company_name    ,
                                                  company_id      ,
                                                  province        ,
                                                  telephone       ,
                                                  email           ,
                                                  address         ,
                                                  legal_person    ,
                                                  mobile          ,
                                                  name            ,
                                                  other_name      ,
                                                  xzqh            ,
                                                  zip_code        ,
                                                  dsfmc           ,
                                                  ep_person       ,
                                                  fax             ,
                                                  industry        ,
                                                  production_phase,
                                                  sfdsfjc         ,
                                                  sfkzzxjc        ,
                                                  btime           ,
                                                  create_time     
                                              from liaoning_company where 1=1 "
}

import_liaoning_paiwu(){
  import_data drc_pollute liaoning_paiwu id 1 "select 
                                                  id            ,
                                                  filter_id     ,
                                                  company_name  ,
                                                  company_id    ,
                                                  mon_name      ,
                                                  pullutant_name,
                                                  pullutant_time,
                                                  avg_sc        ,
                                                  avg_zs        ,
                                                  std_value     ,
                                                  is_ok         ,
                                                  over_num      ,
                                                  create_time   ,
                                                  btime
                                              from liaoning_paiwu where DATE_FORMAT(btime,'%Y-%m-%d')>='$lst_date2' and DATE_FORMAT(btime,'%Y-%m-%d')<'$do_date2' "
}



# 6 内蒙古

import_neimenggu_company(){
  import_data drc_pollute neimenggu_company id 1 "select 
                                                  id,
                                                  company_name,
                                                  company_id,
                                                  usc_code,
                                                  company_type,
                                                  company_lat_lng,
                                                  legal_person,
                                                  paiwu_code,
                                                  paiwu_code_date,
                                                  lianxi_ren,
                                                  telephone,
                                                  email,
                                                  com_date,
                                                  address,
                                                  shi,
                                                  area,
                                                  wd_miao,
                                                  jiandu_type,
                                                  url,
                                                  btime,
                                                  create_time,
                                                  status+0
                                              from neimenggu_company where 1=1 "
}

import_neimenggu_paiwu(){
  import_data drc_pollute neimenggu_paiwu id 1 "select 
                                                  id             ,
                                                  filter_id      ,
                                                  company_name   ,
                                                  company_id     ,
                                                  usc_code       ,
                                                  province       ,
                                                  temprownumber,
                                                  jcdmc          ,
                                                  jcrq           ,
                                                  zbmc           ,
                                                  zsnd           ,
                                                  xzxz           ,
                                                  xzdw           ,
                                                  sfcb           ,
                                                  cbbs           ,
                                                  wendu          ,
                                                  shidu          ,
                                                  sjfbrq         ,
                                                  tbsj           ,
                                                  liusu          ,
                                                  hanyangliang   ,
                                                  liuliang       ,
                                                  wjcyy          ,
                                                  scnd           ,
                                                  scfuhe         ,
                                                  bzmc           ,
                                                  jc_type        ,
                                                  gkfuhe         ,
                                                  kzjbfs         ,
                                                  fspfl          ,
                                                  tmmc           ,
                                                  fqpfl          ,
                                                  xzqh           ,
                                                  xh             ,
                                                  hymc           ,
                                                  publishstatus ,
                                                  xzqhdmshi      ,
                                                  sendbackreason,
                                                  jknd           ,
                                                  note           ,
                                                  pfbz           ,
                                                  create_time    ,
                                                  btime          
                                              from neimenggu_paiwu where DATE_FORMAT(btime,'%Y-%m-%d')>='$lst_date2' and DATE_FORMAT(btime,'%Y-%m-%d')<'$do_date2' "
}



# 7 海南

import_comp_hainan_ent(){
  import_data drc_pollute comp_hainan_ent id 1 "select 
                                                  id,
                                                  ent_name,
                                                  ent_id,
                                                  legal_person,
                                                  uccode,
                                                  address,
                                                  lng,
                                                  lat,
                                                  link_man,
                                                  link_phone,
                                                  email,
                                                  industry,
                                                  wrymc,
                                                  xkzwry,
                                                  xkzbh,
                                                  xkzfzrq,
                                                  btime
                                              from comp_hainan_ent where 1=1 "
}

import_comp_hainan_pollution(){
  import_data drc_pollute comp_hainan_pollution id 1 "select 
                                                  id          ,
                                                  ent_name    ,
                                                  ent_id      ,
                                                  xmlx        ,
                                                  jcdmc       ,
                                                  pkmc        ,
                                                  zbmc        ,
                                                  zsnd        ,
                                                  bzxz        ,
                                                  sfcb        ,
                                                  check_type  ,
                                                  jcsj        ,
                                                  bzmc        ,
                                                  cbbs        ,
                                                  cbyy        ,
                                                  dw          ,
                                                  data_id     ,
                                                  jcrq        ,
                                                  liuliang    ,
                                                  pkbh        ,
                                                  pkcodesheng ,
                                                  sjlx        ,
                                                  url         ,
                                                  btime       
                                              from comp_hainan_pollution where DATE_FORMAT(btime,'%Y-%m-%d')>='$lst_date2' and DATE_FORMAT(btime,'%Y-%m-%d')<'$do_date2' "
}


# 8 河北

import_paiwu_hebei_coms(){
  import_data drc_pollute paiwu_hebei_coms id 1 "select 
                                                  id                   ,
                                                  psName               ,
                                                  psCode               ,
                                                  longitude            ,
                                                  remarks              ,
                                                  address              ,
                                                  latitude             ,
                                                  regionName           ,
                                                  parkName             ,
                                                  regulationIndustry   ,
                                                  attentiondegreeYear  ,
                                                  psTypeName           ,
                                                  postalCode           ,
                                                  runDate              ,
                                                  psState              ,
                                                  industryName         ,
                                                  environmentPrincipal ,
                                                  btime                ,
                                                  create_time          
                                              from paiwu_hebei_coms where 1=1 "
}

import_paiwu_hebei(){
  import_data drc_pollute paiwu_hebei id 1 "select 
                                                  id            ,
                                                  psName        ,
                                                  psCode        ,
                                                  portTypeName  ,
                                                  portTypeId    ,
                                                  portName      ,
                                                  pollutantName ,
                                                  pollutantType ,
                                                  unitCou       ,
                                                  unitAvg       ,
                                                  avg           ,
                                                  zsavg         ,
                                                  min           ,
                                                  zsmin         ,
                                                  max           ,
                                                  zsmax         ,
                                                  flag          ,
                                                  stopFlag      ,
                                                  alarmUplimit  ,
                                                  time          ,
                                                  create_time   ,
                                                  btime         
                                              from paiwu_hebei where DATE_FORMAT(btime,'%Y-%m-%d')>='$lst_date2' and DATE_FORMAT(btime,'%Y-%m-%d')<'$do_date2' "
}



# 9 天津

import_paiwu_tianjin_coms(){
  import_data drc_pollute paiwu_tianjin_coms id 1 "select 
                                                  id            ,
                                                  qymc          ,
                                                  shxydm        ,
                                                  frdb          ,
                                                  ssjt          ,
                                                  qylb          ,
                                                  js            ,
                                                  wd            ,
                                                  pwxkzfzrq     ,
                                                  jd            ,
                                                  dz            ,
                                                  pwxkzbh       ,
                                                  snst          ,
                                                  lxr           ,
                                                  dzyx          ,
                                                  lxrdh         ,
                                                  psqx          ,
                                                  jctcny        ,
                                                  btime         ,
                                                  create_time 
                                              from paiwu_tianjin_coms where 1=1 "
}

import_paiwu_tianjin(){
  import_data drc_pollute paiwu_tianjin id 12 "select 
                                                  ID            ,
                                                  QYMC          ,
                                                  JCDMC         ,
                                                  JCRQ          ,
                                                  SCND          ,
                                                  ZBMC          ,
                                                  X_ID          ,
                                                  TBSJ          ,
                                                  CYSJ          ,
                                                  WJCYY         ,
                                                  SJFBRQ        ,
                                                  XZDW          ,
                                                  XZQHDMSHI     ,
                                                  SHUIWEN       ,
                                                  BZMC          ,
                                                  tempRowNumber ,
                                                  XZQH          ,
                                                  SFCB          ,
                                                  GZFUHE        ,
                                                  SCFUHE        ,
                                                  LIULIANG      ,
                                                  SFCH          ,
                                                  XZXZ          ,
                                                  CBBS          ,
                                                  tempColumn    ,
                                                  shxydm        ,
                                                  lx            ,
                                                  create_time   ,
                                                  SHIDU         ,
                                                  WENDU         ,
                                                  HANYANGLIANG  ,
                                                  LIUSU         ,
                                                  ZSND          ,
                                                  FX            ,
                                                  FS            ,
                                                  YL            ,
                                                  HJKQ_FS       ,
                                                  HJKQ_SD       ,
                                                  HJKQ_QW       ,
                                                  HJKQ_FX       ,
                                                  HJKQ_QY       ,
                                                  SY            ,
                                                  SQSJ          ,
                                                  SQYY          ,
                                                  SQBTGYY       ,
                                                  FBZT          ,
                                                  SFSQ          ,
                                                  DXS_JS        ,
                                                  DXS_SW        ,
                                                  jclx          ,
                                                  btime     
                                              from paiwu_tianjin where DATE_FORMAT(btime,'%Y-%m-%d')>='$lst_date2' and DATE_FORMAT(btime,'%Y-%m-%d')<'$do_date2' "
}

# 10 山西

import_paiwu_shanxi_coms(){
  import_data drc_pollute paiwu_shanxi_coms id 1 "select 
                                                  cid            ,
                                                  frdbxm         ,
                                                  qyyzbm         ,
                                                  qylb           ,
                                                  qywz           ,
                                                  qyxxdz         ,
                                                  hblxrsjh       ,
                                                  hblxrdzyx      ,
                                                  shtyxydm       ,
                                                  xzqh           ,
                                                  qyzcdz         ,
                                                  hylb           ,
                                                  qymc           ,
                                                  dlzb           ,
                                                  hblxrdh        ,
                                                  pwxkzbh        ,
                                                  qyzcdzyb       ,
                                                  hblxrxm        ,
                                                  pwxkzfzrq      ,
                                                  cym            ,
                                                  zzjgdm         ,
                                                  hblxrcz        ,
                                                  btime          ,
                                                  create_time    
                                              from paiwu_shanxi_coms where 1=1 "
}

import_paiwu_shanxi(){
  import_data drc_pollute paiwu_shanxi id 1 "select 
                                                id               ,
                                                jcdw             ,
                                                jcxmmc           ,
                                                jcsj             ,
                                                scnd             ,
                                                zsnd             ,
                                                jcxmdw           ,
                                                bzsx             ,
                                                bzxx             ,
                                                sfcb             ,
                                                cbbs             ,
                                                kzfs             ,
                                                type             ,
                                                cid              ,
                                                create_time      ,
                                                btime                      
                                              from paiwu_shanxi where DATE_FORMAT(btime,'%Y-%m-%d')>='$lst_date2' and DATE_FORMAT(btime,'%Y-%m-%d')<'$do_date2' "
}


# 11 新疆

import_comp_xinjiang_pollution(){
  import_data drc_pollute comp_xinjiang_pollution id 1 "select 
                                                id             ,
                                                ent_name       ,
                                                province_name  ,
                                                area_name      ,
                                                org_type       ,
                                                check_type     ,
                                                check_point    ,
                                                check_date     ,
                                                source         ,
                                                value          ,
                                                standard_value ,
                                                unit           ,
                                                up_to_standard ,
                                                times          ,
                                                standard       ,
                                                discharge_to   ,
                                                dischage_method,
                                                bz             ,
                                                btime                               
                                              from comp_xinjiang_pollution where DATE_FORMAT(btime,'%Y-%m-%d')>='$lst_date2' and DATE_FORMAT(btime,'%Y-%m-%d')<'$do_date2' "
}


# 12 江苏

import_jiangsu_company(){
  import_data drc_pollute jiangsu_company id 1 "select  
                                                id                           ,      
                                                enterprise_name              ,
                                                enterprise_code              ,
                                                canton_code                  ,
                                                canton_name                  ,
                                                attLevel_type_name           ,
                                                posx                         ,
                                                posy                         ,
                                                vocation_code                ,
                                                vocation_name                ,
                                                legal_person_code            ,
                                                legal_person_name            ,
                                                enterprise_address           ,
                                                enterprise_pollute_type      ,
                                                enterprise_website           ,
                                                enterprise_agency            ,
                                                bring_into_production_date   ,
                                                open_account_bank            ,
                                                envir_protection_agency_name ,
                                                envir_people_count           ,
                                                report_open_date             ,
                                                remark                       ,
                                                btime                        ,
                                                create_time                                               
                                              from jiangsu_company where  1=1 "
}


import_jiangsu_paiwu(){
  import_data drc_pollute jiangsu_paiwu id 1 "select 
                                                  id                        ,                
                                                  filter_id                 ,
                                                  enterprise_name           ,
                                                  enterprise_code           ,
                                                  monitor_site_code         ,
                                                  s_name                    ,
                                                  sname                     ,
                                                  pollution_factor_code     ,
                                                  pollution_factor_name     ,
                                                  sctype                    ,
                                                  sctype_name               ,
                                                  monitor_value             ,
                                                  monitor_date              ,
                                                  monitor_zs_value          ,
                                                  monitor_unit              ,
                                                  verify_reason             ,
                                                  put_rule                  ,
                                                  put_where                 ,
                                                  cmemo                     ,
                                                  dover_float               ,
                                                  pfbzzsx                   ,
                                                  pjpfl                     ,
                                                  pfbzzxx                   ,
                                                  fbitover                  ,
                                                  reg_comment               ,
                                                  audit_state               ,
                                                  fdatafrom_name            ,
                                                  pK_alarm_id               ,
                                                  gasor_water_monitor_value ,
                                                  data_status_string        ,
                                                  monitor_site_type_name    ,
                                                  monitoring_type_name      ,
                                                  create_time               ,
                                                  btime                           
                                              from jiangsu_paiwu where DATE_FORMAT(btime,'%Y-%m-%d')>='$lst_date2' and DATE_FORMAT(btime,'%Y-%m-%d')<'$do_date2' "
}


# 13 山东

import_shandong_paiwu(){
  import_data drc_pollute shandong_paiwu id 1 "select 
                                                id           ,
                                                filter_id    ,
                                                qymc         ,
                                                jcdmc        ,
                                                province     ,
                                                c0003_stname ,
                                                shi          ,
                                                areaname     ,
                                                xian         ,
                                                jcdlxmc      ,
                                                jcdlx        ,
                                                xzdwmc       ,
                                                jcxm         ,
                                                jcxmmc       ,
                                                yjlx         ,
                                                jcrq         ,
                                                sxzb         ,
                                                jcjg         ,
                                                jcjgy        ,
                                                tmnr         ,
                                                bzmc         ,
                                                yjwj         ,
                                                sfcbmc       ,
                                                sfcb         ,
                                                rn           ,
                                                xxzb         ,
                                                bz           ,
                                                cbbs         ,
                                                sbxh         ,
                                                sbmc         ,
                                                sccs         ,
                                                jffid        ,
                                                create_time  ,
                                                jclx         ,
                                                jcffid       ,
                                                btime                               
                                              from shandong_paiwu where DATE_FORMAT(btime,'%Y-%m-%d')>='$lst_date2' and DATE_FORMAT(btime,'%Y-%m-%d')<'$do_date2' "
}

# 14 陕西

import_comp_shanxi3_pollution(){
  import_data drc_pollute comp_shanxi3_pollution id 8 "select 
                                                id              ,
                                                ent_name        ,
                                                ent_id          ,
                                                province_name   ,
                                                area_name       ,
                                                check_point     ,
                                                source          ,
                                                jknd            ,
                                                pknd            ,
                                                value           ,
                                                standard_value  ,
                                                check_date      ,
                                                unit            ,
                                                times           ,
                                                check_type      ,
                                                gzfuhe          ,
                                                liuliang        ,
                                                jcdjcxmbh       ,
                                                zsnd            ,
                                                xzxx            ,
                                                url             ,
                                                wendu           ,
                                                shidu           ,
                                                liusu           ,
                                                hanyangliang    ,
                                                xxzb            ,
                                                btime                               
                                              from comp_shanxi3_pollution where DATE_FORMAT(btime,'%Y-%m-%d')>='$lst_date2' and DATE_FORMAT(btime,'%Y-%m-%d')<'$do_date2' "
}


# 15 限期整改

import_paiwu_xianqizhenggai(){
  import_data drc_pollute paiwu_xianqizhenggai btime 1 "select 
                                                code       ,
                                                com_name   ,
                                                province   ,
                                                city       ,
                                                industry   ,
                                                time       ,
                                                btime      ,
                                                info_url   ,
                                                time_start ,
                                                time_end   ,
                                                create_time                  
                                              from paiwu_xianqizhenggai where 1=1 "
}



# 16 宁夏

import_comp_ningxia_ent(){
  import_data drc_pollute comp_ningxia_ent id 1 "select  
                                                  id                         , 
                                                  ent_name                   ,
                                                  legal_person               ,
                                                  address                    ,
                                                  lng_lat                    ,
                                                  link_man                   ,
                                                  link_phone                 ,
                                                  industry                   ,
                                                  fax                        ,
                                                  txdz                       ,
                                                  email                      ,
                                                  telephone                  ,
                                                  ent_status                 ,
                                                  bz                         ,
                                                  btime                      ,
                                                  ent_id                     ,
                                                  ent_type                   ,
                                                  ent_type_code              ,
                                                  area                       ,
                                                  province_name                               
                                              from comp_ningxia_ent where  1=1 "
}


import_comp_ningxia_pollution(){
  import_data drc_pollute comp_ningxia_pollution id 1 "select 
                                                  id                         ,
                                                  ent_name                   ,
                                                  ent_id                     ,
                                                  ent_type                   ,
                                                  ent_type_code              ,
                                                  area                       ,
                                                  url                        ,
                                                  check_type                 ,
                                                  pfl                        ,
                                                  cbbs                       ,
                                                  dbqk                       ,
                                                  bzxz                       ,
                                                  scnd                       ,
                                                  check_date                 ,
                                                  check_point                ,
                                                  jcxm                       ,
                                                  province_name              ,
                                                  btime                                        
                                              from comp_ningxia_pollution where DATE_FORMAT(btime,'%Y-%m-%d')>='$lst_date2' and DATE_FORMAT(btime,'%Y-%m-%d')<'$do_date2' "
}



# 17 上海

import_comp_shanghai_ent(){
  import_data drc_pollute comp_shanghai_ent id 1 "select  
                                                  id                         ,
                                                  ent_name                   ,
                                                  ent_id                     ,
                                                  uccode                     ,
                                                  legal_person               ,
                                                  sfss                       ,
                                                  clrq                       ,
                                                  jyfw                       ,
                                                  wrymc                      ,
                                                  wrybh                      ,
                                                  wrylb                      ,
                                                  hydl                       ,
                                                  hyxl                       ,
                                                  address                    ,
                                                  xzqy                       ,
                                                  zgbm                       ,
                                                  link_man                   ,
                                                  link_phone                 ,
                                                  email                      ,
                                                  sffz                       ,
                                                  lng_lat                    ,
                                                  sczq                       ,
                                                  sftc                       ,
                                                  zphwry                     ,
                                                  scqk                       ,
                                                  url                        ,
                                                  btime                      
                                              from comp_shanghai_ent where  1=1 "
}


import_comp_shanghai_pollution(){
  import_data drc_pollute comp_shanghai_pollution id 1 "select 
                                                  id                         ,
                                                  ent_name                   ,
                                                  ent_id                     ,
                                                  wrybh                      ,
                                                  check_type                 ,
                                                  cysj                       ,
                                                  data_id                    ,
                                                  jcdmc                      ,
                                                  jcxmmc                     ,
                                                  jcxmdw                     ,
                                                  pcdw                       ,
                                                  pcz                        ,
                                                  pfsbmc                     ,
                                                  scheme_name                ,
                                                  scnd                       ,
                                                  sfcb                       ,
                                                  zsnd                       ,
                                                  data_comment               ,
                                                  flag                       ,
                                                  bzxz                       ,
                                                  bzxz_min                   ,
                                                  jcdcode                    ,
                                                  jclx                       ,
                                                  wycyy                      ,
                                                  bz                         ,
                                                  pollution_code             ,
                                                  unit_cou                   ,
                                                  value_type                 ,
                                                  btime                                                
                                              from comp_shanghai_pollution where DATE_FORMAT(btime,'%Y-%m-%d')>='$lst_date2' and DATE_FORMAT(btime,'%Y-%m-%d')<'$do_date2' "
}



# 18 湖北

import_hubei_company(){
  import_data drc_pollute hubei_company id 1 "select  
                                                  id                        ,
                                                  company_name              ,
                                                  company_id                ,
                                                  usc_code                  ,
                                                  qyzcdz                    ,
                                                  qyxxdz                    ,
                                                  longitude                 ,
                                                  latitude                  ,
                                                  sheng                     ,
                                                  sheng_code                ,
                                                  shi                       ,
                                                  shi_code                  ,
                                                  xian_code                 ,
                                                  xkxtid                    ,
                                                  sfzdqy                    ,
                                                  xkzgljb                   ,
                                                  sync_time                 ,
                                                  qyyzbm                    ,
                                                  ly                        ,
                                                  sync_iud                  ,
                                                  gljb                      ,
                                                  sfbj                      ,
                                                  legal_person              ,
                                                  item_type                 ,
                                                  fzrq                      ,
                                                  hblxrdzyx                 ,
                                                  qylb                      ,
                                                  hylb                      ,
                                                  isvoc                     ,
                                                  pwxkzfzrq                 ,
                                                  enterid                   ,
                                                  xkznum                    ,
                                                  sfsh                      ,
                                                  ywsjid                    ,
                                                  btime                     ,
                                                  create_time                          
                                              from hubei_company where  1=1 "
}


import_hubei_paiwu(){
  import_data drc_pollute hubei_paiwu id 1 "select 
                                                  id                         ,
                                                  filter_id                  ,
                                                  company_name               ,
                                                  company_id                 ,
                                                  usc_code                   ,
                                                  jiance_type                ,
                                                  monitor_type               ,
                                                  scnd                       ,
                                                  jcdmc                      ,
                                                  jcxmmc                     ,
                                                  pfsbmc                     ,
                                                  xzsx                       ,
                                                  cbbs                       ,
                                                  cysj                       ,
                                                  wjcyy                      ,
                                                  pai_id                     ,
                                                  create_time                ,
                                                  btime                                 
                                              from hubei_paiwu where DATE_FORMAT(btime,'%Y-%m-%d')>='$lst_date2' and DATE_FORMAT(btime,'%Y-%m-%d')<'$do_date2' "
}



# 19 福建

import_paiwu_fujian_coms(){
  import_data drc_pollute paiwu_fujian_coms id 1 "select  
                                                  cid                        ,
                                                  qymc                       ,
                                                  hymc                       ,
                                                  qygm                       ,
                                                  frxm                       ,
                                                  xxdz                       ,
                                                  zt                         ,
                                                  hylx                       ,
                                                  yb                         ,
                                                  dh                         ,
                                                  city                       ,
                                                  cz                         ,
                                                  djzclx                     ,
                                                  area                       ,
                                                  btime                      ,
                                                  create_time                          
                                              from paiwu_fujian_coms where  1=1 "
}


import_paiwu_fujian(){
  import_data drc_pollute paiwu_fujian id 1 "select 
                                                  id                         ,
                                                  cid                        ,
                                                  type                       ,
                                                  type_name                  ,
                                                  jcdmc                      ,
                                                  jcsj                       ,
                                                  xmmc                       ,
                                                  jcz2                       ,
                                                  jcz1                       ,
                                                  jcz2_bz                    ,
                                                  jcz1_bz                    ,
                                                  dw                         ,
                                                  sfdb                       ,
                                                  cbbs                       ,
                                                  sftc                       ,
                                                  bz                         ,
                                                  jcrq                       ,
                                                  create_time                , 
                                                  btime                           
                                              from paiwu_fujian where DATE_FORMAT(btime,'%Y-%m-%d')>='$lst_date2' and DATE_FORMAT(btime,'%Y-%m-%d')<'$do_date2' "
}



# 20 云南

import_paiwu_yunnan_coms(){
  import_data drc_pollute paiwu_yunnan_coms id 1 "select  
                                                  cid                         ,
                                                  qymc                        ,
                                                  xzqh                        ,
                                                  tyshxydm                    ,
                                                  wrylb                       ,
                                                  province                    ,
                                                  city                        ,
                                                  area                        ,
                                                  hylb                        ,
                                                  frdbxm                      ,
                                                  lxdh                        ,
                                                  qywz                        ,
                                                  dwjj                        ,
                                                  xxdz                        ,
                                                  btime                       ,
                                                  create_time                   
                                              from paiwu_yunnan_coms where  1=1 "
}


import_paiwu_yunnan(){
  import_data drc_pollute paiwu_yunnan id 1 "select 
                                                  id                         ,
                                                  cid                        ,
                                                  jcxm                       ,
                                                  jcsj                       ,
                                                  jcz                        ,
                                                  dw                         ,
                                                  bzxz                       ,
                                                  ll                         ,
                                                  jcd                        ,
                                                  jcdlx                      ,
                                                  type                       ,
                                                  cbbs                       ,
                                                  create_time                ,
                                                  btime                           
                                              from paiwu_yunnan where DATE_FORMAT(btime,'%Y-%m-%d')>='$lst_date2' and DATE_FORMAT(btime,'%Y-%m-%d')<'$do_date2' "
}



# 21 黑龙江

import_heilongjiang_company(){
  import_data drc_pollute heilongjiang_company id 1 "select  
                                                  id                         ,
                                                  company_name               ,
                                                  company_id                 ,
                                                  wrylx                      ,
                                                  zdlb                       ,
                                                  sshy                       ,
                                                  xzqh                       ,
                                                  legal_person               ,
                                                  sczq                       ,
                                                  yzbm                       ,
                                                  address                    ,
                                                  hbfzr                      ,
                                                  telephone                  ,
                                                  cz                         ,
                                                  mobile                     ,
                                                  email                      ,
                                                  btime                      ,
                                                  create_time                                         
                                              from heilongjiang_company where  1=1 "
}


import_heilongjiang_paiwu(){
  import_data drc_pollute heilongjiang_paiwu id 1 "select 
                                                  id                         ,
                                                  filter_id                  ,
                                                  company_name               ,
                                                  jcdw                       ,
                                                  jcfs                       ,
                                                  jcsj                       ,
                                                  jcxm                       ,
                                                  zxbz                       ,
                                                  jcz                        ,
                                                  dw                         ,
                                                  bzxz                       ,
                                                  sfdb                       ,
                                                  wjcyy                      ,
                                                  sftc                       ,
                                                  bz                         ,
                                                  create_time                ,
                                                  btime            
                                              from heilongjiang_paiwu where DATE_FORMAT(btime,'%Y-%m-%d')>='$lst_date2' and DATE_FORMAT(btime,'%Y-%m-%d')<'$do_date2' "
}



# 22 重庆

import_comp_chongqing_ent(){
  import_data drc_pollute comp_chongqing_ent id 1 "select  
                                                  id                ,
                                                  ent_name          ,
                                                  ent_id            ,
                                                  uccode            ,
                                                  address           ,
                                                  legal_person      ,
                                                  industry          ,
                                                  lymc              ,
                                                  link_phone        ,
                                                  btime             ,
                                                  province_name                            
                                              from comp_chongqing_ent where  1=1 "
}


import_comp_chongqing_pollution(){
  import_data drc_pollute comp_chongqing_pollution id 1 "select 
                                                  id                ,
                                                  ent_name          ,
                                                  ent_id            ,
                                                  paikou            ,
                                                  check_date        ,
                                                  jcxm              ,
                                                  jcz               ,
                                                  zsz               ,
                                                  bzxz              ,
                                                  xmdw              ,
                                                  sfcb              ,
                                                  cbbs              ,
                                                  bz                ,
                                                  check_type        ,
                                                  province_name     ,
                                                  btime         
                                              from comp_chongqing_pollution where DATE_FORMAT(btime,'%Y-%m-%d')>='$lst_date2' and DATE_FORMAT(btime,'%Y-%m-%d')<'$do_date2' "
}




#更新方式 0 表示增量更新 1 指定日期更新 2 时间段更新
update_type=0        
if [[ -n "$1" ]]; then
    update_type=$1
fi

echo "update_type: $update_type"
period=1
#如果是增量更新延迟时间（天），-2  当前时间前推2天
if [ "$update_type" = '0' ]; then
  begin_date=$(date +%Y%m%d) 
  period=$(($2+0));
    begin_date=date -d "$period days ago ${begin_date}" +%Y%m%d
  end_date=$begin_date
  echo "period: $period"
elif [ "$update_type" = '1' ]; then
   begin_date=$2 
     end_date=$begin_date
elif [ "$update_type" = '2' ]; then
  begin_date=$2 
  if [[ -n "$3" ]]; then
      end_date=$3
  else
    end_date=$begin_date
  fi 
fi
echo "begin_date: $begin_date"
echo "end_date: $end_date"

do_date=$begin_date
lst_date=`date -d "4 days ago ${do_date}" +%Y%m%d`

while [[ "$do_date" -le "$end_date" ]]
do 


  do_date2=${do_date:0:4}'-'${do_date:4:2}'-'${do_date:6:2}
  echo "do_date2:$do_date2"

  lst_date2=${lst_date:0:4}'-'${lst_date:4:2}'-'${lst_date:6:2}
  echo "lst_date2:$lst_date2"

  






  # 1 安徽
  echo "%%%%%%%%ANHUI%%%%%%%%%"

  import_anhui_company
  import_anhui_paiwu

    sql1=" 
      load data inpath '/origin_data/drc_pollute/anhui_company/$do_date' OVERWRITE into table drc_ods.ods_pollute_anhui_company;
      load data inpath '/origin_data/drc_pollute/anhui_paiwu/$do_date' OVERWRITE into table drc_tmp.tmp_pollute_anhui_paiwu;
     " 



   sql2="   
      set hive.execution.engine=spark;
      set hive.exec.dynamic.partition.mode=nonstrict;
      set hive.merge.sparkfiles=true; 
      set spark.executor.memory=6g;
      set spark.executor.cores=3;
      set spark.driver.memoryOverhead=1g;
      set spark.executor.instances=2;

      insert overwrite table drc_ods.ods_pollute_anhui_paiwu partition(btime)
      select 
                  id             ,
                  filter_id      ,
                  company_name   ,
                  com_qyid       ,
                  usc_code       ,
                  jiance_type    ,
                  jiance_dian    ,
                  jiance_project ,
                  jiance_time    ,
                  jiance_zhi     ,
                  shice_zhi      ,
                  zhesuan_zhi    ,
                  biaozhun_zhi   ,
                  xianzhi_danwei ,
                  is_chaobiao    ,
                  chaobiao_info  ,
                  create_time    ,
                  to_date(btime)          
      from drc_tmp.tmp_pollute_anhui_paiwu
      ;
   "


    $hive -e "$sql1"
    $hive -e "$sql2"



  # 2 北京
  echo "%%%%%%%%BEIJING%%%%%%%%%"

  import_comp_beijing_ent
  import_comp_beijing_pollution


    sql3=" 
      load data inpath '/origin_data/drc_pollute/comp_beijing_ent/$do_date' OVERWRITE into table drc_ods.ods_pollute_comp_beijing_company;
      load data inpath '/origin_data/drc_pollute/comp_beijing_pollution/$do_date' OVERWRITE into table drc_tmp.tmp_pollute_beijing_paiwu;
     " 

   sql4="   
      set hive.execution.engine=spark;
      set hive.exec.dynamic.partition.mode=nonstrict;
      set hive.merge.sparkfiles=true; 
      set spark.executor.memory=6g;
      set spark.executor.cores=3;
      set spark.driver.memoryOverhead=1g;
      set spark.executor.instances=2;

      insert overwrite table drc_ods.ods_pollute_beijing_paiwu partition(btime)
      select 
                  id,
                  ent_name,
                  province_name,
                  area_name,
                  org_type,
                  check_type,
                  check_point,
                  check_date,
                  source,
                  value,
                  standard_value,
                  unit,
                  up_to_standard,
                  times,
                  standard,
                  discharge_to,
                  dischage_method,
                  bz,
                  to_date(btime)          
      from drc_tmp.tmp_pollute_beijing_paiwu
      ;
   "


    $hive -e "$sql3"
    $hive -e "$sql4"

 # 3 甘肃
  echo "%%%%%%%%GANSU%%%%%%%%%"

  import_comp_gansu_ent
  import_comp_gansu_pollution


    sql5=" 
      load data inpath '/origin_data/drc_pollute/comp_gansu_ent/$do_date' OVERWRITE into table drc_ods.ods_pollute_gansu_company;
      load data inpath '/origin_data/drc_pollute/comp_gansu_pollution/$do_date' OVERWRITE into table drc_tmp.tmp_pollute_gansu_paiwu;
     " 

   sql6="   
      set hive.execution.engine=spark;
      set hive.exec.dynamic.partition.mode=nonstrict;
      set hive.merge.sparkfiles=true; 
      set spark.executor.memory=6g;
      set spark.executor.cores=3;
      set spark.driver.memoryOverhead=1g;
      set spark.executor .instances=2;

      insert overwrite table drc_ods.ods_pollute_gansu_paiwu partition(btime)
      select 
                  id           ,
                  ent_name     ,
                  ent_id       ,
                  province_name,
                  city_name    ,
                  area         ,
                  check_type   ,
                  hanyangliang ,
                  bzmc         ,
                  cbbs         ,
                  jcdmc        ,
                  jcrq         ,
                  liuliang     ,
                  liusu        ,
                  scfuhe       ,
                  gzfuhe       ,
                  scnd         ,
                  sfcb         ,
                  shidu        ,
                  sjfbrq       ,
                  tbsj         ,
                  wendu        ,
                  wjcyy        ,
                  xzdw         ,
                  xzxz         ,
                  x_id         ,
                  zbmc         ,
                  zsnd         ,
                  jcdjcxmbh    ,
                  url          ,
                  to_date(btime)          
      from drc_tmp.tmp_pollute_gansu_paiwu
      ;
   "


     $hive -e "$sql5"
     $hive -e "$sql6"



 # 4 河南
  echo "%%%%%%%%HENAN%%%%%%%%%"

  import_henan_company
  import_henan_paiwu


    sql7=" 
      load data inpath '/origin_data/drc_pollute/henan_company/$do_date' OVERWRITE into table drc_ods.ods_pollute_henan_company;
      load data inpath '/origin_data/drc_pollute/henan_paiwu/$do_date' OVERWRITE into table drc_tmp.tmp_pollute_henan_paiwu;
     " 

   sql8="   
      set hive.execution.engine=spark;
      set hive.exec.dynamic.partition.mode=nonstrict;
      set hive.merge.sparkfiles=true; 
      set spark.executor.memory=6g;
      set spark.executor.cores=3;
      set spark.driver.memoryOverhead=1g;
      set spark.executor .instances=2;

      insert overwrite table drc_ods.ods_pollute_henan_paiwu partition(btime)
      select 
                  id            ,
                  filter_id     ,
                  company_name  ,
                  enp_code      ,
                  xuhao         ,
                  jcrq          ,
                  jc_project    ,
                  jcdw          ,
                  jcjg          ,
                  danwei        ,
                  zxbz          ,
                  bzxz          ,
                  is_ok         ,
                  cbbs          ,
                  fbsj          ,
                  wrwpffs       ,
                  pfqx          ,
                  sjycyy        ,
                  jiance_type   ,
                  create_time   ,
                  to_date(btime)          
      from drc_tmp.tmp_pollute_henan_paiwu
      ;
   "


    $hive -e "$sql7"
    $hive -e "$sql8"


 # 5 辽宁
  echo "%%%%%%%%LIAONING%%%%%%%%%"

  import_liaoning_company
  import_liaoning_paiwu


    sql9=" 
      load data inpath '/origin_data/drc_pollute/liaoning_company/$do_date' OVERWRITE into table drc_ods.ods_pollute_liaoning_company;
      load data inpath '/origin_data/drc_pollute/liaoning_paiwu/$do_date' OVERWRITE into table drc_tmp.tmp_pollute_liaoning_paiwu;
     " 

   sql10="   
      set hive.execution.engine=spark;
      set hive.exec.dynamic.partition.mode=nonstrict;
      set hive.merge.sparkfiles=true; 
      set spark.executor.memory=6g;
      set spark.executor.cores=3;
      set spark.driver.memoryOverhead=1g;
      set spark.executor .instances=2;

      insert overwrite table drc_ods.ods_pollute_liaoning_paiwu partition(btime)
      select 
                  id            ,
                  filter_id     ,
                  company_name  ,
                  company_id    ,
                  mon_name      ,
                  pullutant_name,
                  pullutant_time,
                  avg_sc        ,
                  avg_zs        ,
                  std_value     ,
                  is_ok         ,
                  over_num      ,
                  create_time   ,
                  to_date(btime)   
      from drc_tmp.tmp_pollute_liaoning_paiwu
      ;
   "


    $hive -e "$sql9"
    $hive -e "$sql10"



 # 6  内蒙古
  echo "%%%%%%%%NEIMENGGU%%%%%%%%%"

  import_neimenggu_company
  import_neimenggu_paiwu


    sql11=" 
      load data inpath '/origin_data/drc_pollute/neimenggu_company/$do_date' OVERWRITE into table drc_ods.ods_pollute_neimenggu_company;
      load data inpath '/origin_data/drc_pollute/neimenggu_paiwu/$do_date' OVERWRITE into table drc_tmp.tmp_pollute_neimenggu_paiwu;
     " 

   sql12="   
      set hive.execution.engine=spark;
      set hive.exec.dynamic.partition.mode=nonstrict;
      set hive.merge.sparkfiles=true; 
      set spark.executor.memory=6g;
      set spark.executor.cores=3;
      set spark.driver.memoryOverhead=1g;
      set spark.executor .instances=2;

      insert overwrite table drc_ods.ods_pollute_neimenggu_paiwu partition(btime)
      select      
                  id             ,
                  filter_id      ,
                  company_name   ,
                  company_id     ,
                  usc_code       ,
                  province       ,
                  temp_row_number,
                  jcdmc          ,
                  jcrq           ,
                  zbmc           ,
                  zsnd           ,
                  xzxz           ,
                  xzdw           ,
                  sfcb           ,
                  cbbs           ,
                  wendu          ,
                  shidu          ,
                  sjfbrq         ,
                  tbsj           ,
                  liusu          ,
                  hanyangliang   ,
                  liuliang       ,
                  wjcyy          ,
                  scnd           ,
                  scfuhe         ,
                  bzmc           ,
                  jc_type        ,
                  gkfuhe         ,
                  kzjbfs         ,
                  fspfl          ,
                  tmmc           ,
                  fqpfl          ,
                  xzqh           ,
                  xh             ,
                  hymc           ,
                  publish_status ,
                  xzqhdmshi      ,
                  sendback_reason,
                  jknd           ,
                  note           ,
                  pfbz           ,
                  create_time    ,
                  to_date(btime)   
      from drc_tmp.tmp_pollute_neimenggu_paiwu
      ;
   "


     $hive -e "$sql11"
     $hive -e "$sql12"



 # 7  海南
  echo "%%%%%%%%HAINAN%%%%%%%%%"

  import_comp_hainan_ent
  import_comp_hainan_pollution


    sql13=" 
      load data inpath '/origin_data/drc_pollute/comp_hainan_ent/$do_date' OVERWRITE into table drc_ods.ods_pollute_hainan_company;
      load data inpath '/origin_data/drc_pollute/comp_hainan_pollution/$do_date' OVERWRITE into table drc_tmp.tmp_pollute_hainan_paiwu;
     " 

   sql14="   
      set hive.execution.engine=spark;
      set hive.exec.dynamic.partition.mode=nonstrict;
      set hive.merge.sparkfiles=true; 
      set spark.executor.memory=6g;
      set spark.executor.cores=3;
      set spark.driver.memoryOverhead=1g;
      set spark.executor .instances=2;

      insert overwrite table drc_ods.ods_pollute_hainan_paiwu partition(btime)
      select      
                  id         ,
                  ent_name   ,
                  ent_id     ,
                  xmlx       ,
                  jcdmc      ,
                  pkmc       ,
                  zbmc       ,
                  zsnd       ,
                  bzxz       ,
                  sfcb       ,
                  check_type ,
                  jcsj       ,
                  bzmc       ,
                  cbbs       ,
                  cbyy       ,
                  dw         ,
                  data_id    ,
                  jcrq       ,
                  liuliang   ,
                  pkbh       ,
                  pkcodesheng,
                  sjlx       ,
                  url        ,   
                  to_date(btime)   
      from drc_tmp.tmp_pollute_hainan_paiwu
      ;
   "


     $hive -e "$sql13"
     $hive -e "$sql14"



 # 8  河北
  echo "%%%%%%%%HEBEI%%%%%%%%%"

  import_paiwu_hebei_coms
  import_paiwu_hebei


    sql15=" 
      load data inpath '/origin_data/drc_pollute/paiwu_hebei_coms/$do_date' OVERWRITE into table drc_ods.ods_pollute_hebei_company;
      load data inpath '/origin_data/drc_pollute/paiwu_hebei/$do_date' OVERWRITE into table drc_tmp.tmp_pollute_hebei_paiwu;
     " 

   sql16="   
      set hive.execution.engine=spark;
      set hive.exec.dynamic.partition.mode=nonstrict;
      set hive.merge.sparkfiles=true; 
      set spark.executor.memory=6g;
      set spark.executor.cores=3;
      set spark.driver.memoryOverhead=1g;
      set spark.executor .instances=2;

      insert overwrite table drc_ods.ods_pollute_hebei_paiwu partition(btime)
      select      
                  id            ,
                  psName        ,
                  psCode        ,
                  portTypeName  ,
                  portTypeId    ,
                  portName      ,
                  pollutantName ,
                  pollutantType ,
                  unitCou       ,
                  unitAvg       ,
                  avg           ,
                  zsavg         ,
                  min           ,
                  zsmin         ,
                  max           ,
                  zsmax         ,
                  flag          ,
                  stopFlag      ,
                  alarmUplimit  ,
                  times         ,
                  create_time   ,
                  to_date(btime)   
      from drc_tmp.tmp_pollute_hebei_paiwu
      ;
   "


     $hive -e "$sql15"
     $hive -e "$sql16"


 # 9  天津
  echo "%%%%%%%%TIANJIN%%%%%%%%%"

  import_paiwu_tianjin_coms
  import_paiwu_tianjin


    sql17=" 
      load data inpath '/origin_data/drc_pollute/paiwu_tianjin_coms/$do_date' OVERWRITE into table drc_ods.ods_pollute_tianjin_company;
      load data inpath '/origin_data/drc_pollute/paiwu_tianjin/$do_date' OVERWRITE into table drc_tmp.tmp_pollute_tianjin_paiwu;
     " 

   sql18="   
      set hive.execution.engine=spark;
      set hive.exec.dynamic.partition.mode=nonstrict;
      set hive.merge.sparkfiles=true; 
      set spark.executor.memory=6g;
      set spark.executor.cores=3;
      set spark.driver.memoryOverhead=1g;
      set spark.executor .instances=2;

      insert overwrite table drc_ods.ods_pollute_tianjin_paiwu partition(btime)
      select      
                  ID            ,
                  QYMC          ,
                  JCDMC         ,
                  JCRQ          ,
                  SCND          ,
                  ZBMC          ,
                  X_ID          ,
                  TBSJ          ,
                  CYSJ          ,
                  WJCYY         ,
                  SJFBRQ        ,
                  XZDW          ,
                  XZQHDMSHI     ,
                  SHUIWEN       ,
                  BZMC          ,
                  tempRowNumber ,
                  XZQH          ,
                  SFCB          ,
                  GZFUHE        ,
                  SCFUHE        ,
                  LIULIANG      ,
                  SFCH          ,
                  XZXZ          ,
                  CBBS          ,
                  tempColumn    ,
                  shxydm        ,
                  lx            ,
                  create_time   ,
                  SHIDU         ,
                  WENDU         ,
                  HANYANGLIANG  ,
                  LIUSU         ,
                  ZSND          ,
                  FX            ,
                  FS            ,
                  YL            ,
                  HJKQ_FS       ,
                  HJKQ_SD       ,
                  HJKQ_QW       ,
                  HJKQ_FX       ,
                  HJKQ_QY       ,
                  SY            ,
                  SQSJ          ,
                  SQYY          ,
                  SQBTGYY       ,
                  FBZT          ,
                  SFSQ          ,
                  DXS_JS        ,
                  DXS_SW        ,
                  jclx          ,
                  to_date(btime)   
      from drc_tmp.tmp_pollute_tianjin_paiwu
      ;
   "


    $hive -e "$sql17"
    $hive -e "$sql18"


 # 10 山西
  echo "%%%%%%%%SHANXI%%%%%%%%%"

  import_paiwu_shanxi_coms
  import_paiwu_shanxi


    sql19=" 
      load data inpath '/origin_data/drc_pollute/paiwu_shanxi_coms/$do_date' OVERWRITE into table drc_ods.ods_pollute_shanxi_company;
      load data inpath '/origin_data/drc_pollute/paiwu_shanxi/$do_date' OVERWRITE into table drc_tmp.tmp_pollute_shanxi_paiwu;
     " 

   sql20="   
      set hive.execution.engine=spark;
      set hive.exec.dynamic.partition.mode=nonstrict;
      set hive.merge.sparkfiles=true; 
      set spark.executor.memory=6g;
      set spark.executor.cores=3;
      set spark.driver.memoryOverhead=1g;
      set spark.executor .instances=2;

      insert overwrite table drc_ods.ods_pollute_shanxi_paiwu partition(btime)
      select      
                  id            ,
                  jcdw          ,
                  jcxmmc        ,
                  jcsj          ,
                  scnd          ,
                  zsnd          ,
                  jcxmdw        ,
                  bzsx          ,
                  bzxx          ,
                  sfcb          ,
                  cbbs          ,
                  kzfs          ,
                  type          ,
                  cid           ,
                  create_time   ,
                  to_date(btime)   
      from drc_tmp.tmp_pollute_shanxi_paiwu
      ;
   "


     $hive -e "$sql19"
     $hive -e "$sql20"


 # 11 新疆
  echo "%%%%%%%%XINJIANG%%%%%%%%%"

  import_comp_xinjiang_pollution


    sql21=" 
      load data inpath '/origin_data/drc_pollute/comp_xinjiang_pollution/$do_date' OVERWRITE into table drc_tmp.tmp_pollute_xinjiang_paiwu;
     " 

   sql22="   
      set hive.execution.engine=spark;
      set hive.exec.dynamic.partition.mode=nonstrict;
      set hive.merge.sparkfiles=true; 
      set spark.executor.memory=6g;
      set spark.executor.cores=3;
      set spark.driver.memoryOverhead=1g;
      set spark.executor .instances=2;

      insert overwrite table drc_ods.ods_pollute_xinjiang_paiwu partition(btime)
      select      
                  id             ,
                  ent_name       ,
                  province_name  ,
                  area_name      ,
                  org_type       ,
                  check_type     ,
                  check_point    ,
                  check_date     ,
                  source         ,
                  value          ,
                  standard_value ,
                  unit           ,
                  up_to_standard ,
                  times          ,
                  standard       ,
                  discharge_to   ,
                  dischage_method,
                  bz             ,
                  to_date(btime)   
      from drc_tmp.tmp_pollute_xinjiang_paiwu
      ;
   "


    $hive -e "$sql21"
    $hive -e "$sql22"


 # 12 江苏
  echo "%%%%%%%%JIANGSU%%%%%%%"

  import_jiangsu_company
  import_jiangsu_paiwu

    sql23=" 
      load data inpath '/origin_data/drc_pollute/jiangsu_company/$do_date' OVERWRITE into table drc_ods.ods_pollute_jiangsu_company;
      load data inpath '/origin_data/drc_pollute/jiangsu_paiwu/$do_date' OVERWRITE into table drc_tmp.tmp_pollute_jiangsu_paiwu;
     " 

   sql24="   
      set hive.execution.engine=spark;
      set hive.exec.dynamic.partition.mode=nonstrict;
      set hive.merge.sparkfiles=true; 
      set spark.executor.memory=6g;
      set spark.executor.cores=3;
      set spark.driver.memoryOverhead=1g;
      set spark.executor .instances=2;

      insert overwrite table drc_ods.ods_pollute_jiangsu_paiwu partition(btime)
      select      
                    id                       ,
                    filter_id                ,
                    enterprise_name          ,
                    enterprise_code          ,
                    monitor_site_code        ,
                    s_name                   ,
                    sname                    ,
                    pollution_factor_code    ,
                    pollution_factor_name    ,
                    sctype                   ,
                    sctype_name              ,
                    monitor_value            ,
                    monitor_date             ,
                    monitor_zs_value         ,
                    monitor_unit             ,
                    verify_reason            ,
                    put_rule                 ,
                    put_where                ,
                    cmemo                    ,
                    dover_float              ,
                    pfbzzsx                  ,
                    pjpfl                    ,
                    pfbzzxx                  ,
                    fbitover                 ,
                    reg_comment              ,
                    audit_state              ,
                    fdatafrom_name           ,
                    pK_alarm_id              ,
                    gasor_water_monitor_value,
                    data_status_string       ,
                    monitor_site_type_name   ,
                    monitoring_type_name     ,
                    create_time              ,                   
                    to_date(btime)   
      from drc_tmp.tmp_pollute_jiangsu_paiwu
      ;
   "


      $hive -e "$sql23"
      $hive -e "$sql24"


 # 13 山东
  echo "%%%%%%%%SHANDONG%%%%%%%%%"

  import_shandong_paiwu


    sql25=" 
      load data inpath '/origin_data/drc_pollute/shandong_paiwu/$do_date' OVERWRITE into table drc_tmp.tmp_pollute_shandong_paiwu;
     " 

   sql26="   
      set hive.execution.engine=spark;
      set hive.exec.dynamic.partition.mode=nonstrict;
      set hive.merge.sparkfiles=true; 
      set spark.executor.memory=6g;
      set spark.executor.cores=3;
      set spark.driver.memoryOverhead=1g;
      set spark.executor .instances=2;

      insert overwrite table drc_ods.ods_pollute_shandong_paiwu partition(btime)
      select      
                    id           ,
                    filter_id    ,
                    qymc         ,
                    jcdmc        ,
                    province     ,
                    c0003_stname ,
                    shi          ,
                    areaname     ,
                    xian         ,
                    jcdlxmc      ,
                    jcdlx        ,
                    xzdwmc       ,
                    jcxm         ,
                    jcxmmc       ,
                    yjlx         ,
                    jcrq         ,
                    sxzb         ,
                    jcjg         ,
                    jcjgy        ,
                    tmnr         ,
                    bzmc         ,
                    yjwj         ,
                    sfcbmc       ,
                    sfcb         ,
                    rn           ,
                    xxzb         ,
                    bz           ,
                    cbbs         ,
                    sbxh         ,
                    sbmc         ,
                    sccs         ,
                    jffid        ,
                    create_time  ,
                    jclx         ,
                    jcffid       ,
                  to_date(btime)   
      from drc_tmp.tmp_pollute_shandong_paiwu
      ;
   "



     $hive -e "$sql25"
     $hive -e "$sql26"


 # 14 陕西
  echo "%%%%%%%%SHANXI3%%%%%%%%%"

  import_comp_shanxi3_pollution


    sql27=" 
      load data inpath '/origin_data/drc_pollute/comp_shanxi3_pollution/$do_date' OVERWRITE into table drc_tmp.tmp_pollute_shanxi3_paiwu;
     " 

   sql28="   
      set hive.execution.engine=spark;
      set hive.exec.dynamic.partition.mode=nonstrict;
      set hive.merge.sparkfiles=true; 
      set spark.executor.memory=6g;
      set spark.executor.cores=3;
      set spark.driver.memoryOverhead=1g;
      set spark.executor .instances=2;

      insert overwrite table drc_ods.ods_pollute_shanxi3_paiwu partition(btime)
      select      
                  id               ,  
                  ent_name         ,
                  ent_id           ,
                  province_name    ,
                  area_name        ,
                  check_point      ,
                  source           ,
                  jknd             ,
                  pknd             ,
                  value            ,
                  standard_value   ,
                  check_date       ,
                  unit             ,
                  times            ,
                  check_type       ,
                  gzfuhe           ,
                  liuliang         ,
                  jcdjcxmbh        ,
                  zsnd             ,
                  xzxx             ,
                  url              ,
                  wendu            ,
                  shidu            ,
                  liusu            ,
                  hanyangliang     ,
                  xxzb             ,   
                  to_date(btime)   
      from drc_tmp.tmp_pollute_shanxi3_paiwu
      ;
   "
     $hive -e "$sql27"
     $hive -e "$sql28"
   

 # 15 限期整改
  echo "%%%%%%%%XIANQIZHENGGAI%%%%%%%"

  import_paiwu_xianqizhenggai


    sql29=" 
      load data inpath '/origin_data/drc_pollute/paiwu_xianqizhenggai/$do_date' OVERWRITE into table drc_ods.ods_pollute_xianqizhenggai_paiwu;
     " 

     $hive -e "$sql29"




# 16 宁夏
  echo "%%%%%%%%NINGXIA%%%%%%%%%"

  import_comp_ningxia_ent
  import_comp_ningxia_pollution


    sql31=" 
      load data inpath '/origin_data/drc_pollute/comp_ningxia_ent/$do_date' OVERWRITE into table drc_ods.ods_pollute_ningxia_company;
      load data inpath '/origin_data/drc_pollute/comp_ningxia_pollution/$do_date' OVERWRITE into table drc_tmp.tmp_pollute_ningxia_paiwu;
     " 

   sql32="   
      set hive.execution.engine=spark;
      set hive.exec.dynamic.partition.mode=nonstrict;
      set hive.merge.sparkfiles=true; 
      set spark.executor.memory=6g;
      set spark.executor.cores=3;
      set spark.driver.memoryOverhead=1g;
      set spark.executor .instances=2;

      insert overwrite table drc_ods.ods_pollute_ningxia_paiwu partition(btime)
      select 
                  id                         ,
                  ent_name                   ,
                  ent_id                     ,
                  ent_type                   ,
                  ent_type_code              ,
                  area                       ,
                  url                        ,
                  check_type                 ,
                  pfl                        ,
                  cbbs                       ,
                  dbqk                       ,
                  bzxz                       ,
                  scnd                       ,
                  check_date                 ,
                  check_point                ,
                  jcxm                       ,
                  province_name              ,
                  to_date(btime)   
      from drc_tmp.tmp_pollute_ningxia_paiwu
      ;
   "


    $hive -e "$sql31"
    $hive -e "$sql32"




# 17 上海
  echo "%%%%%%%%SHANGHAI%%%%%%%%%"

  import_comp_shanghai_ent
  import_comp_shanghai_pollution


    sql33=" 
      load data inpath '/origin_data/drc_pollute/comp_shanghai_ent/$do_date' OVERWRITE into table drc_ods.ods_pollute_shanghai_company;
      load data inpath '/origin_data/drc_pollute/comp_shanghai_pollution/$do_date' OVERWRITE into table drc_tmp.tmp_pollute_shanghai_paiwu;
     " 

   sql34="   
      set hive.execution.engine=spark;
      set hive.exec.dynamic.partition.mode=nonstrict;
      set hive.merge.sparkfiles=true; 
      set spark.executor.memory=6g;
      set spark.executor.cores=3;
      set spark.driver.memoryOverhead=1g;
      set spark.executor .instances=2;

      insert overwrite table drc_ods.ods_pollute_shanghai_paiwu partition(btime)
      select 
                  id                         ,
                  ent_name                   ,
                  ent_id                     ,
                  wrybh                      ,
                  check_type                 ,
                  cysj                       ,
                  data_id                    ,
                  jcdmc                      ,
                  jcxmmc                     ,
                  jcxmdw                     ,
                  pcdw                       ,
                  pcz                        ,
                  pfsbmc                     ,
                  scheme_name                ,
                  scnd                       ,
                  sfcb                       ,
                  zsnd                       ,
                  data_comment               ,
                  flag                       ,
                  bzxz                       ,
                  bzxz_min                   ,
                  jcdcode                    ,
                  jclx                       ,
                  wycyy                      ,
                  bz                         ,
                  pollution_code             ,
                  unit_cou                   ,
                  value_type                 ,
                  to_date(btime)   
      from drc_tmp.tmp_pollute_shanghai_paiwu
      ;
   "


    $hive -e "$sql33"
    $hive -e "$sql34"




# 18 湖北
  echo "%%%%%%%%HUBEI%%%%%%%%%"

  import_hubei_company
  import_hubei_paiwu


    sql35=" 
      load data inpath '/origin_data/drc_pollute/hubei_company/$do_date' OVERWRITE into table drc_ods.ods_pollute_hubei_company;
      load data inpath '/origin_data/drc_pollute/hubei_paiwu/$do_date' OVERWRITE into table drc_tmp.tmp_pollute_hubei_paiwu;
     " 

   sql36="   
      set hive.execution.engine=spark;
      set hive.exec.dynamic.partition.mode=nonstrict;
      set hive.merge.sparkfiles=true; 
      set spark.executor.memory=6g;
      set spark.executor.cores=3;
      set spark.driver.memoryOverhead=1g;
      set spark.executor .instances=2;

      insert overwrite table drc_ods.ods_pollute_hubei_paiwu partition(btime)
      select 
                  id                         ,
                  filter_id                  ,
                  company_name               ,
                  company_id                 ,
                  usc_code                   ,
                  jiance_type                ,
                  monitor_type               ,
                  scnd                       ,
                  jcdmc                      ,
                  jcxmmc                     ,
                  pfsbmc                     ,
                  xzsx                       ,
                  cbbs                       ,
                  cysj                       ,
                  wjcyy                      ,
                  pai_id                     ,
                  create_time                ,      
                  to_date(btime)   
      from drc_tmp.tmp_pollute_hubei_paiwu
      ;
   "


    $hive -e "$sql35"
    $hive -e "$sql36"





# 19 福建
  echo "%%%%%%%%FUJIAN%%%%%%%%%"

  import_paiwu_fujian_coms
  import_paiwu_fujian


    sql37=" 
      load data inpath '/origin_data/drc_pollute/paiwu_fujian_coms/$do_date' OVERWRITE into table drc_ods.ods_pollute_fujian_company;
      load data inpath '/origin_data/drc_pollute/paiwu_fujian/$do_date' OVERWRITE into table drc_tmp.tmp_pollute_fujian_paiwu;
     " 

   sql38="   
      set hive.execution.engine=spark;
      set hive.exec.dynamic.partition.mode=nonstrict;
      set hive.merge.sparkfiles=true; 
      set spark.executor.memory=6g;
      set spark.executor.cores=3;
      set spark.driver.memoryOverhead=1g;
      set spark.executor .instances=2;

      insert overwrite table drc_ods.ods_pollute_fujian_paiwu partition(btime)
      select 
                  id                         ,
                  cid                        ,
                  type                       ,
                  type_name                  ,
                  jcdmc                      ,
                  jcsj                       ,
                  xmmc                       ,
                  jcz2                       ,
                  jcz1                       ,
                  jcz2_bz                    ,
                  jcz1_bz                    ,
                  dw                         ,
                  sfdb                       ,
                  cbbs                       ,
                  sftc                       ,
                  bz                         ,
                  jcrq                       ,
                  create_time                ,       
                  to_date(btime)   
      from drc_tmp.tmp_pollute_fujian_paiwu
      ;
   "


    $hive -e "$sql37"
    $hive -e "$sql38"





# 20 云南
  echo "%%%%%%%%YUNNAN%%%%%%%%%"

  import_paiwu_yunnan_coms
  import_paiwu_yunnan


    sql39=" 
      load data inpath '/origin_data/drc_pollute/paiwu_yunnan_coms/$do_date' OVERWRITE into table drc_ods.ods_pollute_yunnan_company;
      load data inpath '/origin_data/drc_pollute/paiwu_yunnan/$do_date' OVERWRITE into table drc_tmp.tmp_pollute_yunnan_paiwu;
     " 

   sql40="   
      set hive.execution.engine=spark;
      set hive.exec.dynamic.partition.mode=nonstrict;
      set hive.merge.sparkfiles=true; 
      set spark.executor.memory=6g;
      set spark.executor.cores=3;
      set spark.driver.memoryOverhead=1g;
      set spark.executor .instances=2;

      insert overwrite table drc_ods.ods_pollute_yunnan_paiwu partition(btime)
      select 
                  id                         ,
                  cid                        ,
                  jcxm                       ,
                  jcsj                       ,
                  jcz                        ,
                  dw                         ,
                  bzxz                       ,
                  ll                         ,
                  jcd                        ,
                  jcdlx                      ,
                  type                       ,
                  cbbs                       ,
                  create_time                ,      
                  to_date(btime)   
      from drc_tmp.tmp_pollute_yunnan_paiwu
      ;
   "


    $hive -e "$sql39"
    $hive -e "$sql40"





# 21 黑龙江
  echo "%%%%%%%%HEILONGJIANG%%%%%%%%%"

  import_heilongjiang_company
  import_heilongjiang_paiwu


    sql39=" 
      load data inpath '/origin_data/drc_pollute/heilongjiang_company/$do_date' OVERWRITE into table drc_ods.ods_pollute_heilongjiang_company;
      load data inpath '/origin_data/drc_pollute/heilongjiang_paiwu/$do_date' OVERWRITE into table drc_tmp.tmp_pollute_heilongjiang_paiwu;
      
     " 

   sql40="   
      set hive.execution.engine=spark;
      set hive.exec.dynamic.partition.mode=nonstrict;
      set hive.merge.sparkfiles=true; 
      set spark.executor.memory=6g;
      set spark.executor.cores=3;
      set spark.driver.memoryOverhead=1g;
      set spark.executor .instances=2;

      insert overwrite table drc_ods.ods_pollute_heilongjiang_paiwu partition(btime)
      select 
                    id               ,
                    filter_id        ,
                    company_name     ,
                    jcdw             ,
                    jcfs             ,
                    jcsj             ,
                    jcxm             ,
                    zxbz             ,
                    jcz              ,
                    dw               ,
                    bzxz             ,
                    sfdb             ,
                    wjcyy            ,
                    sftc             ,
                    bz               ,
                    create_time      ,   
                    to_date(btime)   
      from drc_tmp.tmp_pollute_heilongjiang_paiwu
      ;
   "


    $hive -e "$sql39"
    $hive -e "$sql40"





# 22 重庆
  echo "%%%%%%%%CHONGQING%%%%%%%%%"

  import_comp_chongqing_ent
  import_comp_chongqing_pollution


    sql41=" 
      load data inpath '/origin_data/drc_pollute/comp_chongqing_ent/$do_date' OVERWRITE into table drc_ods.ods_pollute_chongqing_company;
      load data inpath '/origin_data/drc_pollute/comp_chongqing_pollution/$do_date' OVERWRITE into table drc_tmp.tmp_pollute_chongqing_paiwu;
      
     " 

   sql42="   
      set hive.execution.engine=spark;
      set hive.exec.dynamic.partition.mode=nonstrict;
      set hive.merge.sparkfiles=true; 
      set spark.executor.memory=6g;
      set spark.executor.cores=3;
      set spark.driver.memoryOverhead=1g;
      set spark.executor .instances=2;

      insert overwrite table drc_ods.ods_pollute_chongqing_paiwu partition(btime)
      select 
                    id                ,
                    ent_name          ,
                    ent_id            ,
                    paikou            ,
                    check_date        ,
                    jcxm              ,
                    jcz               ,
                    zsz               ,
                    bzxz              ,
                    xmdw              ,
                    sfcb              ,
                    cbbs              ,
                    bz                ,
                    check_type        ,
                    province_name     ,   
                    to_date(btime)   
      from drc_tmp.tmp_pollute_chongqing_paiwu
      ;
   "


    $hive -e "$sql41"
    $hive -e "$sql42"



    let do_date=`date -d "-1 days ago ${do_date}" +%Y%m%d`
    do_year=${do_date:0:4}
    do_month=${do_date:0:6}
done