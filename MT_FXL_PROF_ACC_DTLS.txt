create or replace PROCEDURE            MT_FXL_PROF_ACC_DTLS (ip_limit_n NUMBER DEFAULT 10000)
AS
   /******************************************************************************
          NAME          : MIG_PROF_ACC_DTLS
       PURPOSE       : This procedure will populate customer's profile,account and address details .
       PREREQUISITES :
       OBJECTS       : The tables populated :-
                       1.  CB_SUBSCRIBER_MASTER        -> Customer Profile/geometric info
                       2.  CB_ACCOUNT_MASTER           -> Invoice Account info
                       3.  CB_SUBS_HEIRARCHY           -> Group-Subgroup Heirarchy
                       4.  CB_PERSONAL_DETAILS         -> Customer Personal Info
                       5.  CB_REPRESENTATIVE_DETAILS   -> Representative person's info in case of corporate profile
                       6.  MAP_STG_CUST_ABILLITY_CODES -> Mapping table for source to staging attributes
                      Procedures called :-
                       1.  CHECK_DIGIT_DST             -> To add Check digit to the account number
                       2.  MTNGC_ADDRESS_PRC             -> To populate address information for profile and account
                       3.  Mig_Log_Error           -> To log the errors if any errors in the migration process
       RESULT        :
       CREATED DATE  : 09Sept2006
       CREATED BY    :
       MODULE        : Data Migration
       REVISIONS     :
                       Ver        Date        Author           Description
                     ---------  ----------  ---------------  ------------------------------------
                       1.0     09SEP2006                       Base Scripts
                       1.1     25APR2007      Kiran/Gandhi     Changes done in the scripts and reviewed the codes
                       1.2     15AUG2007      Rajuvan          Changes done in the scripts
                       1.3                    Rajuvan          Used 'Trial' package for as Dashboard process
                       1.4     18JUL2008      Kiran            Selecting default parameters from mig_process_control
                       1.5     12AUG2008      Kiran            Procedure mig_address_prc called to populate the address dtls
                                                               Mig_Log_Error added to log the errors
                       1.6     25FEB2009      Kiran            Changes done in the scripts and reviewed the codes
                       1.7     10AUG2009      A.N.Shukla       Customized for PNG-BeMobile
                       1.8     20APR2018      Shivanand.S.M    Customised for MT GSM& FXL
       NOTES:
   ******************************************************************************/
   l_subs_code_n                   NUMBER;
   --add_cur                         stg_address_dtls%ROWTYPE; commented by shiva
   l_debug_code_v                  VARCHAR2 (50);
   l_debug_msg_v                   VARCHAR2 (400);
   l_debug_mesg_v                  VARCHAR2 (400);
   l_procedure_name                VARCHAR2 (50)              := $$plsql_unit;
   l_acc_code_n                    NUMBER;
   l_acc_link_code_n               NUMBER;
   l_no_of_services_n              NUMBER;
   l_contact_mobl_num_n            NUMBER;
   l_status_code_v                 VARCHAR2 (2)                       := '10';
   l_credit_rating_n               NUMBER;
   l_dunning_schdl_code_v          VARCHAR2 (6);
   l_cnt                           NUMBER;
   l_tot_cnt                       NUMBER                                := 0;
   l_ctr_flg                       NUMBER;
   l_process_id                    NUMBER                                := 2;
   l_subscriber_level_n            NUMBER;
   --l_check_acc_code_n              NUMBER;
   l_sucees_flag_n                 NUMBER;
   l_quit_e                        EXCEPTION;
   l_user_code_n                   NUMBER;
   l_country_code_v                VARCHAR2 (6);
   l_deflt_country_code_v          VARCHAR2 (6);
   l_nationality_v                 VARCHAR2 (50);
   l_deflt_nationality_v           VARCHAR2 (50);
   l_currency_code_v               VARCHAR2 (5);
   l_language_code_v               VARCHAR2 (10);
   l_stat                          VARCHAR2 (10);
   l_despatch_media_indicators_v   VARCHAR2 (10)              := '1000000000';
   l_despatch_to_optn_n            NUMBER                                := 3;
   l_location_code_v               VARCHAR2 (30);
   l_activated_by_n                NUMBER;
   l_risk_category_v               VARCHAR2 (5);
   l_buss_nature                   VARCHAR2 (3);
   l_credit_controler              NUMBER (4)                            := 1;
   ------Added default value 19 jul
   l_account_manager               NUMBER (4)                           := 52;
   l_service_code                  VARCHAR2 (3);
   l_sub_service_code              VARCHAR2 (10);
   l_virtual_inv_acct_flag         VARCHAR2 (1);
   l_citizen_id                    VARCHAR2 (10);
   op_success_flag_n               NUMBER;
   op_err_code_v                   VARCHAR2 (100);
   op_err_mesg_v                   VARCHAR2 (400);
   l_prof_acc_flag_v               VARCHAR2 (1);
   l_error_flag                    NUMBER (2)                            := 0;   
   l_subscriber_cat_code_v         VARCHAR2 (8)                        :='CORP';
   l_subscriber_sub_category_v     VARCHAR2 (8)                        :='CORPNG';
   -- Added by Kiran on 18-july-2008 to get default values from mig_control_param
   l_dflt_subs_cate_code_v         VARCHAR2 (8);
   l_dflt_subs_sub_cate_code_v     VARCHAR2 (8);
   l_mig_user_code_n               NUMBER (4);
   l_deflt_bill_region_v           VARCHAR2 (10)                       := ' ';
   l_pref_pay_method               VARCHAR2 (50) :='1:2:3:4:11:12:42:10:20:30:17';
   l_dflt_contact_v                VARCHAR2 (20);
   l_bill_cycle_id                 cb_bill_cycle.bill_cycl_code_n%TYPE;
   l_bill_region_id                cb_billing_region_master.billing_region_v%TYPE;
   l_abillity_title_code_v         VARCHAR2 (20);
   l_risk_cat_code_v               VARCHAR2 (8);
   l_profile_type_v                VARCHAR2 (8);
   l_tax_policy_v                  cb_account_master.tax_policy_v%TYPE;
   l_state_code_v                  cb_city.state_code_v%TYPE;
   l_city_code_v                   cb_city.city_code_v%TYPE;
   l_occupation_cd_v               cb_occupation.occupation_code_v%TYPE;
BEGIN
   
   l_debug_code_v := '00.00';
   l_debug_msg_v :='Initializing  MIG_PROCESS_CONTROL for ' || l_procedure_name;
   
   mig_pack.start_process (l_procedure_name);
    
   l_cnt := 0;

   FOR prof_cur IN
     (SELECT  TO_TIMESTAMP (sub.registration_date_d,'DD-MON-YYYY HH24:MI:SS') reg_date_d,
              --NVL(TO_DATE(sub.birth_date_d,'DD-MON-YY'),TO_DATE('01-JAN-1900','DD-MON-YY')) dob,
              TO_DATE('01-JAN-1900','DD-MON-YY') dob,              
              TO_TIMESTAMP (sub.identification_date_d,'DD-MON-YYYY HH24:MI:SS')id_date_d,
              DECODE (TRIM (sub.gender_v),
                              'M', 'M',
                              'F', 'F',
                              'N' 
                             )prof_gender_v,
              DECODE (sub.marital_status_v,
                              'M', 'M',
                              'S', 'S',                              
                              'N'
                             )prof_marital_status_v, 
              DECODE (sub.profile_type_v,'I','I','C','B',' ')prof_type_v,                                          
              sub.*
      FROM stg_subscriber_dtls sub 
      WHERE sub.profile_type_v='C' ---'I'(C for Corporate I For Retail)
      AND EXISTS(SELECT 1 FROM stg_account_dtls acc WHERE sub.subscriber_code_v=acc.subscriber_code_v)              
      AND NOT EXISTS (SELECT 1 FROM map_stg_cust_abillity_codes stg WHERE stg.stg_profile_code = sub.subscriber_code_v))
            
   LOOP
      BEGIN  
      
         EXIT WHEN mig_pack.check_stop_flag (l_procedure_name);
         
         SAVEPOINT mysavepoint;
         
         l_prof_acc_flag_v := 'Y';
      
         l_debug_code_v := '03';
         l_debug_msg_v := 'Unable to get Generate value from mig_seq_prof_code_n for '|| prof_cur.subscriber_code_v;

         SELECT mig_seq_prof_code_n.NEXTVAL
           INTO l_subs_code_n
           FROM DUAL;
         
        BEGIN
         l_debug_code_v := '03';
         l_debug_msg_v :='Unable to get profile status code from MAP_STATUS' || prof_cur.subscriber_code_v;

         SELECT status_code_v
           INTO l_status_code_v
           FROM map_status
          WHERE stg_status_v = prof_cur.status_code_v 
          AND account_type_v = 'P';
        EXCEPTION
            WHEN NO_DATA_FOUND
            THEN                                  
              l_status_code_v :='NA';            
        END;   

        /* BEGIN
            l_debug_code_v := '04';
            l_debug_msg_v := 'Unable to get category details from MAP_SUBSCRIBER_CATEGORY :' || prof_cur.subscriber_code_v;

            SELECT map.abillity_catgeory, map.abillity_sub_catgeory                   
              INTO l_subscriber_cat_code_v, l_subscriber_sub_category_v                   
              FROM map_subscriber_category map
             WHERE map.stg_catgeory = prof_cur.category_code_v
             AND  map.stg_sub_catgeory =prof_cur.sub_category_code_v
             AND  map.group_info_v =prof_cur.grouped;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               IF prof_cur.prof_type_v = 'I'
               THEN
                  l_subscriber_cat_code_v := mig_pack.gc_category_code_pos_v;
                  l_subscriber_sub_category_v :=mig_pack.gc_subcategory_code_pos_v;
                  l_tax_policy_v := 'Y';
               ELSE
                  l_subscriber_cat_code_v := mig_pack.gc_category_code_bus_v;
                  l_subscriber_sub_category_v :=mig_pack.gc_subcategory_code_bus_v;                  
               END IF;
         END;  */     ---- Temp Commented have to revert once Corp Migration over.                           

         l_debug_code_v := '05';
         l_debug_msg_v := 'Unable to select data from CB_SUBSCRIBER_SUB_CATEGORY :' || prof_cur.subscriber_code_v;

         SELECT NVL (MIN (default_xdir_level_n), 9),
                NVL (MIN (risk_category_v), '')
           INTO l_subscriber_level_n,
                l_risk_category_v
           FROM cb_subscriber_sub_category
          WHERE subs_category_code_v = l_subscriber_cat_code_v
            AND subs_sub_category_code_v = l_subscriber_sub_category_v;

         l_debug_code_v := '06';
         l_debug_msg_v := 'Unable to select location_v from MAP_LOCATION '|| prof_cur.subscriber_code_v;

         SELECT NVL (MIN (abillity_locn_code_v), mig_pack.gc_location_code_v)
           INTO l_location_code_v
           FROM map_location
          WHERE source_locn_code_v = prof_cur.location_code_v;

         l_debug_code_v := '07';
         l_debug_msg_v :='Unable to select citizen_id_v from map_citizen_id ' || prof_cur.subscriber_code_v;

         SELECT NVL (MIN (ab_id), 'OTHERS')
           INTO l_citizen_id
           FROM map_citizen_id
          WHERE src_id = prof_cur.id_type_v;
          
         l_debug_code_v := '07';
         l_debug_msg_v :='Unable to select data from MAP_COCCUPATION ' || prof_cur.subscriber_code_v;

         SELECT NVL (MIN (ab_ocupation_id), 'U01')
           INTO l_occupation_cd_v
           FROM map_occupation
          WHERE src_ocupation_id =prof_cur.occupation_code_v;
           
         
         BEGIN                              
            l_debug_code_v := '09.1';
            l_debug_msg_v :='Unable to select citizen_id_v from MAP_COUNTRY_CODE ' || prof_cur.subscriber_code_v;

            SELECT ab_id, UPPER (ab_category)
              INTO l_country_code_v, l_nationality_v
              FROM map_country_code
             WHERE src_id = prof_cur.country_code_v;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               l_country_code_v := mig_pack.gc_country_code_v;
               l_nationality_v := mig_pack.gc_nationality_v;
         END;

         BEGIN
            l_debug_code_v := '09.2';
            l_debug_msg_v :='Unable to info from MAP_TITLE for' || prof_cur.subscriber_code_v;

            SELECT abl_title_code_v
              INTO l_abillity_title_code_v
              FROM map_title
             WHERE src_title_code_v = prof_cur.title_v;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               l_abillity_title_code_v :=mig_pack.gc_title_v;
         END;

         l_debug_code_v := '10';
         l_debug_msg_v :='Unable to insert into CB_SUBSCRIBER_MASTER ' || prof_cur.subscriber_code_v;

         INSERT      /*+ APPEND */INTO CB_SUBSCRIBER_MASTER
                     (subscriber_code_n,
                      identification_num_v,
                      identification_date_d,
                      passport_num_v,
                      profile_type_v,
                      group#sub_group_code_n, 
                      subs_category_code_v,
                      subs_sub_category_code_v, 
                      id_type_v,
                      title_v,
                      first_name_v,
                      middle_name_v,
                      last_name_v,
                      status_code_v, 
                      subscriber_level_n,
                      interacting_user_code_n, 
                      cpv_score_n, 
                      cpv_grade_v,
                      gender_v,
                      marital_status_v,
                      birth_date_d, 
                      country_code_v,
                      nationality_v,
                      occupation_code_v,
                      blood_grp_v,
                      language_code_v,
                      representative_flg_v,
                      contact_mobl_num_v,
                      contact_home_num_v,
                      contact_office_num_v,
                      fax_num_v, 
                      email_id1_v,
                      email_id2_v,
                      blacklisted_flg_v,
                      blacklisted_date_d,
                      activated_by_user_code_n, 
                      registration_date_d,
                      erased_date_d, 
                      risk_category_v,
                      credit_rating_n,
                      passport_expiry_d,
                      app_mode_v,
                      location_code_v,
                      bill_presentation_lang_v,                      
                      prospect_code_n,
                      contract_number_n, 
                      sales_rep_code_n,
                      access_id_n, 
                      num_of_prepaid_profiles_n, 
                      vip_flag_v,
                      send_notification_mail_flg_v,
                      notification_email_id_v,
                      send_notification_sms_flg_v,
                      notification_sms_number_v,
                      pref_comm_lang_code_v,
                      preferred_curr_code_v,
                      bill_present_curr_code_v, 
                      billing_region_v,
                      tax_applicable_flg_v,
                      tax_policy_v,
                      migrated_from_prospect_v,
                      pref_pay_methods_v,
                      prof_cr_vetting_type_v,
                      bill_cycl_code_n,
                      bill_cycl_applicable_flg_v,
                      subs_attr_1_v,
                      subs_attr_2_v,
                      subs_attr_3_v,
                      subs_attr_4_v,
                      subs_attr_5_v,
                      subs_attr_1_n,
                      subs_attr_2_n,
                      subs_attr_3_n, 
                      subs_attr_4_n,
                      subs_attr_5_n,
                      posted_flag_n, 
                      posted_date_d,
                      contact_person_first_name_v,
                      contact_person_family_name_v,
                      contact_person_phone_number_v,
                      contact_person_email_v,
                      contact_person_fax_v,
                      ext_subscriber_code_v
                     )
              VALUES (l_subs_code_n,                                                        --SUBSCRIBER_CODE_N
                      NVL (prof_cur.identification_num_v,'MTG'|| prof_cur.subscriber_code_v),--IDENTIFICATION_NUM_V
                      prof_cur.id_date_d,                                                    --IDENTIFICATION_DATE_D
                      SUBSTR (TRIM (prof_cur.passport_num_v), 1, 15),                        --PASSPORT_NUM_V
                      prof_cur.prof_type_v,                                                  --l_profile_type_v, 
                      0,                                                                     --GROUP#SUB_GROUP_CODE_N
                      l_subscriber_cat_code_v,                                               --SUBS_CATEGORY_CODE_V
                      l_subscriber_sub_category_v,                                           --SUBS_SUB_CATEGORY_CODE_V
                      l_citizen_id,                                                          --ID_TYPE_V
                      l_abillity_title_code_v,                                               --TITLE_V
                      NVL (prof_cur.first_name_v,'.'),                                       --FIRST_NAME_V
                      SUBSTR(prof_cur.middle_name_v,1,40),                                   --MIDDLE_NAME_V
                      NVL(SUBSTR(prof_cur.last_name_v,1,40),'.'),                            --LAST_NAME_V
                      l_status_code_v,                                                       --STATUS_CODE_V
                      l_subscriber_level_n,                                                  --SUBSCRIBER_LEVEL_N
                      mig_pack.gc_mig_user_code_n,                                           --prof_cur.interacting_user_code_n
                      NULL,                                                                  --CPV_SCORE_N
                      'N',                                                                   --CPV_GRADE_V
                      prof_cur.prof_gender_v,                                                --GENDER_V
                      prof_cur.prof_marital_status_v,                                        --MARITAL_STATUS_V
                      prof_cur.dob,                                                          --DOB
                      l_country_code_v,                                                      --COUNTRY_CODE_V
                      mig_pack.gc_nationality_v,                                             --NATIONALITY_V
                      l_occupation_cd_v,                                                     --OCCUPATION_CODE_V
                      prof_cur.blood_grp_v,                                                  --BLOOD_GRP_V
                      mig_pack.gc_language_code_v,                                           --LANGUAGE_CODE_V
                      DECODE (prof_cur.prof_type_v, 'B', 'Y', 'N'),                          --Prof_cur.REPRESENTATIVE_FLG_V,
                      NVL (SUBSTR (prof_cur.contact_mobl_num_v, 1, 20),l_dflt_contact_v),    --CONTACT_MOBL_NUM_V
                      SUBSTR (prof_cur.contact_home_num_v, 1, 20),                           --CONTACT_HOME_NUM_V
                      SUBSTR (prof_cur.contact_office_num_v, 1, 20),                         --CONTACT_OFFICE_NUM_V
                      prof_cur.fax_num_v,                                                    --FAX_NUM_V
                      TRIM (prof_cur.email_id1_v),                                           --EMAIL_ID1_V
                      TRIM (prof_cur.email_id2_v),                                           --EMAIL_ID2_V
                      prof_cur.blacklisted_flg_v,                                            --BLACKLISTED_FLG_V
                      prof_cur.blacklisted_date_d,                                           --BLACKLISTED_DATE_D
                      mig_pack.gc_mig_user_code_n,                                           --user_code_n
                      prof_cur.reg_date_d,                                                   --REGISTRATION_DATE_D
                      prof_cur.erased_date_d,                                                --ERASED_DATE_D
                      l_risk_category_v,                                                     --RISK_CATEGORY_V
                      3,                                                                     --CREDIT_RATING_N
                      prof_cur.passport_expiry_d,                                            --PASSPORT_EXPIRY_D
                      prof_cur.app_mode_v,                                                   --APP_MODE_V
                      l_location_code_v,                                                     --LOCATION_CODE_V
                      mig_pack.gc_language_code_v,                                           -- BILL_PRESENTATION_LANG_V
                      0,                                                                     --PROSPECT_CODE_N
                      NULL,                                                                  --CONTRACT_NUMBER_N
                      1,                                                                     --SALES_REP_CODE_N
                      1,                                                                     --ACCESS_ID_N
                      0,                                                                     --NUM_OF_PREPAID_PROFILES_N
                      'N',                                                                   --VIP_FLAG_V
                      NVL (prof_cur.send_notification_mail_flg_v, 'N'),                      --SEND_NOTIFICATION_MAIL_FLG_V
                      TRIM (prof_cur.notification_email_id_v),                               --NOTIFICATION_EMAIL_ID_V
                      NVL (prof_cur.send_notification_sms_flg_v, 'Y'),                       --SEND_NOTIFICATION_SMS_FLG_V
                      TRIM (prof_cur.notification_sms_number_v),                             --NOTIFICATION_SMS_NUMBER_V
                      mig_pack.gc_language_code_v,                                           --PREF_COMM_LANG_CODE_V
                      mig_pack.gc_currency_code_v,                                           --PREFERRED_CURR_CODE_V
                      NVL (prof_cur.bill_present_curr_code_v,mig_pack.gc_currency_code_v),   --BILL_PRESENT_CURR_CODE_V
                      NVL (l_bill_region_id, mig_pack.gc_billing_region_v),                  --BILLING_REGION_V
                      'A',                                                                   --TAX_APPLICABLE_FLG_V
                      l_tax_policy_v,                                                        --TAX_POLICY_V
                      'N',                                                                   --MIGRATED_FROM_PROSPECT_V
                      l_pref_pay_method,                                                     --PREF_PAY_METHODS_V
                      'N',                                                                   -- VALUE FOR THE KEY REG_FLASH_APP IN CB_CONTROL_KEYS IS 'Y'
                      mig_pack.gc_bill_cycl_code_n,                                          --BILL_CYCL_CODE_N
                      'P',                                                                   --BILL_CYCL_APPLICABLE_FLG_V
                      NULL,                                                                  --SUBS_ATTR_1_V
                      'N',                                                                   --SUBS_ATTR_2_V
                      NULL,                                                                  --SUBS_ATTR_3_V
                      NULL,                                                                  --SUBS_ATTR_4_V
                      prof_cur.VAT_CODE,                                                     --SUBS_ATTR_5_V 
                      0,                                                                     --SUBS_ATTR_1_N
                      0,                                                                     --SUBS_ATTR_2_N
                      NULL,                                                                  --SUBS_ATTR_3_N
                      NULL,                                                                  --SUBS_ATTR_4_N
                      NULL,                                                                  --SUBS_ATTR_5_N
                      NULL,                                                                  --POSTED_FLAG_N
                      NULL,                                                                  --POSTED_DATE_D
                      NVL(prof_cur.first_name_v, '.'),                                       --CONTACT_PERSON_FIRST_NAME_V
                      SUBSTR (prof_cur.last_name_v,1,40),                                    --CONTACT_PERSON_FAMILY_NAME_V
                      NULL,                                                                  --CONTACT_PERSON_PHONE_NUMBER_V
                      NULL,                                                                  --CONTACT_PERSON_EMAIL_V
                      NULL,                                                                  --CONTACT_PERSON_FAX_V
                      'F'||prof_cur.subscriber_code_v);                                      --EXT_SUBSCRIBER_CODE_V
                                            

         l_debug_code_v := '11';
         l_debug_msg_v :='Unable to insert into CB_PERSONAL_DETAILS ' || prof_cur.subscriber_code_v;

         INSERT      /*+ APPEND */INTO cb_personal_details
                     (subscriber_code_n,
                      monthly_income_n,
                      net_disposable_income_n,
                      dependents_n, 
                      race_code_v,
                      employer_name_v,
                      employed_since_date_d,
                      spouses_nationality,
                      spouses_dob_d,
                      spouses_working_v,
                      children_n,
                      children_in_school_n,
                      children_overseas_n,
                      business_nature,
                      no_of_employees_n,
                      household_income_range_n,
                      spouse_title_v, 
                      spouse_name_v
                     )
              VALUES (l_subs_code_n,                                                    --SUBSCRIBER_CODE_N
                      NULL,                                                                     --MONTHLY_INCOME_N
                      prof_cur.net_disposable_income_n,                                         --NET_DISPOSABLE_INCOME_N
                      prof_cur.dependents_n,                                                    --DEPENDENTS_N
                      prof_cur.race_code_v,                                                     --RACE_CODE_V
                      prof_cur.employer_name_v,                                                 --EMPLOYER_NAME_V
                      prof_cur.employed_since_date_d,                                           --EMPLOYED_SINCE_DATE_D
                      prof_cur.spouses_nationality,                                             --SPOUSES_NATIONALITY
                      prof_cur.spouses_dob_d,                                                   --SPOUSES_DOB_D
                      prof_cur.spouses_working_v,                                               --SPOUSES_WORKING_V
                      prof_cur.children_n,                                                      --CHILDREN_N
                      prof_cur.children_in_school_n,                                            --CHILDREN_IN_SCHOOL_N
                      prof_cur.children_overseas_n,                                             --CHILDREN_OVERSEAS_N
                      prof_cur.business_nature,                                                 --BUSINESS_NATURE
                      prof_cur.no_of_employees_n,                                               --NO_OF_EMPLOYEES_N
                      NULL,                                                                     --HOUSEHOLD_INCOME_RANGE_N
                      prof_cur.spouse_title_v,                                                  --SPOUSE_TITLE_V
                      prof_cur.spouse_name_v);                                                  --SPOUSE_NAME_V

         -- Added by Kiran for corporate customers on 23-Feb 2009
         IF prof_cur.prof_type_v = 'B' 
         THEN
            l_debug_code_v := '12';
            l_debug_msg_v :=
                  'Unable to insert into CB_REPRESENTATIVE_DETAILS '|| prof_cur.subscriber_code_v;

            INSERT      /*+ APPEND */INTO cb_representative_details
                 VALUES (l_subs_code_n,                                                  --SUBSCRIBER_CODE_N
                         DECODE(prof_cur.prof_type_v,'B','G'),                                   --PROFILE_TYPE_V
                         l_citizen_id,                                                           --ID_TYPE_V
                         NVL (TRIM (prof_cur.identification_num_v),'MTG' || l_subs_code_n),--IDENTIFICATION_NUM_V
                         prof_cur.passport_expiry_d,                                             --ID_EXPIRY_DATE_D
                         TRIM (prof_cur.passport_num_v),                                         --PASSPORT_NUM_V
                         l_subscriber_cat_code_v,                                                --SUBS_CATEGORY_CODE_V
                         l_subscriber_sub_category_v,                                            --SUBS_SUB_CATEGORY_CODE_V
                         prof_cur.dob,                                                           --DATE_OF_BIRTH
                         l_abillity_title_code_v,                                                --TITLE_V
                         NVL (prof_cur.first_name_v,'.'),                                      --NAME_V
                         NULL,                                                                   --RELATIONSHIP_V
                         prof_cur.prof_gender_v,                                                 --GENDER_V
                         l_occupation_cd_v,                                                      --OCCUPATION_CODE_V
                         NULL,                                                                   --ORGANISATION_CODE_V
                         prof_cur.monthly_income_n,                                              --MONTHLY_INCOME_N
                         prof_cur.race_code_v,                                                   --RACE_CODE_V
                         mig_pack.gc_nationality_v,                                              --NATIONALITY_V
                         NULL,                                                                   --PO_BOX_NUM_V
                         NULL,                                                                   --ZIP_CODE_V
                         NULL,                                                                   --ADDRESS_1
                         NULL,                                                                   --ADDRESS_2
                         NULL,                                                                   --ADDRESS_3
                         NULL,                                                                   --ADDRESS_4
                         NULL,                                                                   --CITY_CODE_V
                         l_country_code_v,                                                       --COUNTRY_CODE_V
                         NVL (SUBSTR (prof_cur.contact_mobl_num_v, 1, 20),l_dflt_contact_v),     --MOBILE_NUMBER_V
                         SUBSTR (prof_cur.contact_home_num_v, 1, 20),                            --HOUSE_NUMBER_V
                         SUBSTR (prof_cur.contact_office_num_v, 1, 20),                          --OFFICE_NUMBER_V
                         TRIM (prof_cur.email_id1_v),                                            --EMAIL_ID_V
                         NULL,                                                                   --EMPLOYED_SINCE_DT_D
                         mig_pack.gc_language_code_v,                                            --LANGUAGE_CODE_V
                         l_occupation_cd_v,                                                      --DESIGNATION
                         NULL,                                                                   --YEARS_OF_EXPERIENCE
                         NULL,                                                                   --BIZ_NATURE_CODE_V
                         NULL,                                                                   --BIZ_NUMBER_V
                         'N',                                                                    --PRINT_REP_IN_BILL_V
                         1,                                                                      --SL_NO_N
                         NULL);                                                                  --FAX_NUMBER_V
         END IF;

         /* l_debug_code_v := '13';
          l_debug_msg_v := 'Unable to insert data into CB_SUBS_HEIRARCHY'|| prof_cur.subscriber_code_v;

          INSERT INTO cb_subs_heirarchy
                      (heirarchial_option_v,
                       subscriber_code_n,
                       parent_group_code_n,
                       subscriber_name_v,
                       entity_type_v,
                       main_group_code_n
                      )
               VALUES (NVL (prof_cur.group_sub_group_code_flg_v, 'N'),                           --HEIRARCHIAL_OPTION_V,
                       l_subs_code_n,                                                            --SUBSCRIBER_CODE_N,
                       0,                                                                        --PARENT_GROUP_CODE_N
                       prof_cur.first_name_v,                                                    --SUBSCRIBER_NAME_V
                       P',                                                                       --ENTITY_TYPE_V
                       0                                                                         --MAIN_GROUP_CODE_N
                      );   */
         /*BEGIN
            l_debug_code_v := '13';
            l_debug_msg_v :=
                  'Unable to get state_code_v,city_code_v from CB_CITY For PROFILE ' || prof_cur.subscriber_code_v;

            SELECT state_code_v, city_code_v, country_code_v
              INTO l_state_code_v, l_city_code_v, l_country_code_v
              FROM cb_city
             WHERE city_code_v = UPPER(prof_cur.ccity);
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               l_state_code_v := mig_pack.gc_state_code_v;
               l_city_code_v := mig_pack.gc_city_code_v;
               l_country_code_v := mig_pack.gc_country_code_v;
         END;  */                        

         l_debug_code_v := '14.00';
         l_debug_mesg_v :='Unable to insert into CB_ADDRESS for PROFILE ' || prof_cur.subscriber_code_v;

         INSERT      /*+ APPEND*/INTO cb_address
                     (account_link_code_n, 
                      address_type_n, 
                      account_type_v,
                      address_1_v,
                      address_2_v,
                      address_3_v,
                      address_4_v,
                      city_code_v,
                      state_code_v, 
                      country_code_v,
                      zip_code_v,
                      phone_number_v,                      
                      email_id_v,
                      po_box_v, 
                      local_flg_v,
                      area_code_v, 
                      district_v,                      
                      country_v,
                      province_v,
                      street_desc_v,
                      postofficecode,
                      city_desc,
                      district_desc,
                      landmark                      
                     )
              VALUES (l_subs_code_n,                                                             --ACCOUNT_LINK_CODE_N
                      1,                                                                         --ADDRESS_TYPE_N
                      'P',                                                                       --ACCOUNT_TYPE_V
                      prof_cur.street_code,                                                      --ADDRESS_1_V
                      prof_cur.plot_number,                                                      --ADDRESS_2_V
                      prof_cur.building,                                                         --ADDRESS_3_V
                      prof_cur.floor,                                                            --ADDRESS_4_V
                      prof_cur.sublocality_code,                                                 --CITY_CODE_V
                      prof_cur.cdistrict,                                                        -- STATE_CODE_V
                      mig_pack.gc_country_code_v,                                                --COUNTRY_CODE_V
                      prof_cur.postal_code,                                                      --ZIP_CODE_V
                      NULL,                                                                      --PHONE_NUMBER_V                      
                      NULL,                                                                      --EMAIL_ID_V
                      prof_cur.po_box,                                                           --PO_BOX_V
                      'P',                                                                       --LOCAL_FLG_V
                      NULL,                                                                      --AREA_CODE_V
                      prof_cur.locality_code,                                                    --DISTRICT_V                      
                      mig_pack.gc_country_name_v,                                                --COUNTRY_V
                      prof_cur.cdistrict,                                                        --PROVINCE_V
                      prof_cur.street,                                                           --STREET_DESC_V
                      prof_cur.postoffice_code,                                                  --POSTOFFICE_CODE
                      prof_cur.sublocality,                                                      --CITY_DESC,/SUB_LOCALITY_DESC
                      prof_cur.locality,                                                         --DISTRICT_DESC,
                      prof_cur.compl_adr                                                         --LANDMARK,                                            
                     );
                     
         l_debug_code_v := '14-1';
         l_debug_msg_v :='Unable to fetch data from CB_SERV_ADDNL_ATTRIBUTES for PROFILE' || prof_cur.subscriber_code_v;

         INSERT      /*+ APPEND */INTO cb_serv_addnl_attributes
                     (subscriber_code_n, 
                      account_code_n, 
                      entity_level_v,
                      account_link_code_n,
                      serv_addnl_fld_31_v,
                      serv_addnl_fld_32_v,
                      serv_addnl_fld_33_v,
                      serv_addnl_fld_34_v,
                      serv_addnl_fld_35_v,
                      serv_addnl_fld_36_v,
                      serv_addnl_fld_37_v, 
                      serv_addnl_fld_38_v,
                      serv_addnl_fld_39_v,
                      serv_addnl_fld_51_d,
                      serv_addnl_fld_40_v, 
                      serv_addnl_fld_41_v,
                      serv_addnl_fld_42_v, 
                      serv_addnl_fld_43_v,
                      serv_addnl_fld_44_v,
                      serv_addnl_fld_45_v,
                      serv_addnl_fld_46_v, 
                      serv_addnl_fld_52_d,
                      serv_addnl_fld_7_v
                     )
              VALUES (l_subs_code_n,                                                             --subscriber_code_n
                      NULL,                                                                      --account_code_n
                      'P',                                                                       --entity_level_v
                      NULL,                                                                      --account_link_code_n
                      prof_cur.prof_gender_v,                                                    --serv_addnl_fld_31_v --GENDER_V
                      prof_cur.prof_marital_status_v,                                            --l_marital_status_v
                      mig_pack.gc_nationality_v,                                                 --l_nationality_v
                      NULL,                                                                      --l_work_status_code_v
                      NULL,                                                                      --ser_cur.mobl_num_voice_v,
                      NULL,                                                                      --ser_cur.mobl_num_data_v,--l_data_v
                      NULL,                                                                      --l_mobile_money_v
                      mig_pack.gc_mig_user_code_n,                                               --l_agent_id_v
                      NULL,                                                                      --l_agent_name_v
                      prof_cur.dob,                                                              --l_date_of_birth_t
                      TRIM (prof_cur.email_id1_v),                                               --email_id_v
                      NULL,                                                                      --l_altr_telephone_number_v
                      NULL,                                                                      --l_registration_status_v
                      NULL,                                                                      --l_job_designation_v
                      SUBSTR (prof_cur.contact_office_num_v, 1, 20),                             --CONTACT_OFFICE_NUM_V--l_telephone_number_v
                      NULL,                                                                      --l_country_of_residence_v
                      NULL,                                                                      --l_product_type_v
                      prof_cur.reg_date_d,                                                       --registration_date_d
                      NVL(SUBSTR (prof_cur.contact_mobl_num_v, 1, 20),l_dflt_contact_v)          --CONTACT_MOBL_NUM_V
                     );
              
         l_debug_code_v := '15';
         l_debug_msg_v :='Unable to fetch data from STG_ACCOUNT_DTLS ' || prof_cur.subscriber_code_v;

         FOR acc_cur IN
            (SELECT 
                   acc.activation_date_d act_date_d,                                      
                   DECODE (tax_policy_v, '15% Vat', 'Y', 'N') tax_policy,
                   DECODE(notification_type,'Email notification for CCBS invoices','Y','N') email_notifcn,
                   DECODE(bill_cycl_code_n,'B',107000001,'C',114000001,'D',121000001,101000001) bill_cycle_code,
                   DECODE(despatch_media_indicators_v,'Post','8','EMAIL','4','')bill_dispatch,
                   acc.*
               FROM stg_account_dtls acc
              WHERE acc.subscriber_code_v = prof_cur.subscriber_code_v
              )
         LOOP
            BEGIN
                 
                 SAVEPOINT mysavepoint1; 
                 
                 l_debug_code_v := '16';
                 l_debug_msg_v :='Unable to generate MIG_SEQ_ACC_CODE_N ' || prof_cur.subscriber_code_v;
                 
               SELECT mig_seq_acc_code_n.NEXTVAL
                 INTO l_acc_code_n
                 FROM DUAL;      
                                 
                l_debug_code_v := '17';
                l_debug_msg_v :='Unable to generate MIG_SEQ_ACC_LINK_CODE_N ' || prof_cur.subscriber_code_v; 
               
               SELECT mig_seq_acc_link_code_n.NEXTVAL
                 INTO l_acc_link_code_n
                 FROM DUAL;
                      
            BEGIN   
               l_debug_code_v := '16.4';
               l_debug_msg_v :='Unable to get target account status from MAP_STATUS : '|| acc_cur.status_code_v;

               SELECT status_code_v
                 INTO l_status_code_v
                 FROM map_status
                WHERE stg_status_v = acc_cur.status_code_v
                AND account_type_v = 'A'; 
            EXCEPTION
                WHEN NO_DATA_FOUND
                THEN                                  
                l_status_code_v :=' ';            
            END;   
                

               BEGIN
                  l_debug_code_v := '16.5';
                  l_debug_msg_v :='Unable to select citizen_id_v from MAP_CITIZEN_ID '|| prof_cur.subscriber_code_v;

                  SELECT ab_id
                    INTO l_citizen_id
                    FROM map_citizen_id
                   WHERE src_id= prof_cur.id_type_v;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     l_citizen_id := 'OTHERS';
               END;

               l_debug_code_v := '17';
               l_debug_msg_v :=
                     'Unable to insert into CB_ACCOUNT_MASTER for staging ACCOUNT : '|| acc_cur.account_number_v;

               INSERT      /*+ APPEND */INTO cb_account_master
                           (subscriber_code_n,
                            account_code_n,
                            account_link_code_n,
                            status_code_v,
                            activation_date_d, 
                            erased_date_d,
                            cr_days_optn_v,
                            cr_days_n, 
                            revenue_grade_code_v,
                            tax_policy_v,
                            chrg_intrst_optn_v,
                            chrg_intrst_rate_n,
                            invoice_in_base_curncy_flg_v,
                            currency_code_v,
                            installment_flag_v,
                            bill_cycl_code_n,
                            billing_region_v,
                            bill_group_n,
                            --despatch_media_indicators_v,
                            dtls_to_fax_num_v,
                            email_list_v, 
                            -- despatch_to_optn_n,
                            payer_optn_flg_v,
                            payer_code_n,
                            payments_v,
                            auto_debit_flg_v,
                            dunning_schdl_code_v,
                            next_invoice_seq_num_n,
                            subscriber_category_v,
                            risk_category_v,
                            credit_rating_n,
                            subscriber_sub_category_v,
                            activated_by_n,
                            account_name_v,
                            account_title_v,
                            exclude_lpf,
                            pre_termination_date_d,
                            bill_presentation_lang_v,
                            virtual_invoice_acc_flg_v, 
                            account_manager_n,
                            credit_controler_n, 
                            account_type_v,
                            mul_applicable_flg_v, 
                            preferred_curr_code_v,
                            bill_present_curr_code_v,
                            old_account_num_v, 
                            spl_attribute_v, 
                            acc_attr_1_v,
                            acc_attr_2_v, 
                            acc_attr_3_v, 
                            acc_attr_4_v,
                            acc_attr_5_v, 
                            acc_attr_1_n, 
                            acc_attr_2_n,
                            acc_attr_3_n, 
                            acc_attr_4_n, 
                            acc_attr_5_n,
                            posted_flag_n, 
                            posted_date_d,
                            pref_communication_lang_v,             --new fields
                            id_type_v,                             --new fields
                            ind_shared_flag_v,                     --new fields
                            no_of_services_n,
                            credit_control_applic_v,
                            ext_account_code_v,
                            tax_optn_v,
                            account_type_flag_v,
                            add_acc_serv_flag_v,
                            send_email_notification_v, -- added newly by shiva
                            dispatch_notify_optns_n  -- added newly by shiva                                 
                           )
                    VALUES (l_subs_code_n,                                                   --SUBSCRIBER_CODE_N
                            l_acc_code_n,                                                     --ACCOUNT_CODE_N
                            l_acc_link_code_n,                                                --ACCOUNT_LINK_CODE_N
                            l_status_code_v,                                                    --STATUS_CODE_V
                            acc_cur.act_date_d,                                                  --ACTIVATION_DATE_D
                            acc_cur.erased_date_d,                                               --ERASED_DATE_D
                            'Y',                                                                 --CR_DAYS_OPTN_V
                            NVL (acc_cur.cr_days_n, 45),                                         --CR_DAYS_N modified  by shiva on 1704 for MT
                            'N',                                                                 --revenue_grade_code_v
                            acc_cur.tax_policy,                                                --TAX_POLICY_V
                            'N',                                                                 --CHRG_INTRST_OPTN_V
                            0,                                                                   --CHRG_INTRST_RATE_N
                            NVL (acc_cur.invoice_in_base_curncy_flg_v, 'Y'),                     --INVOICE_IN_BASE_CURNCY_FLG_V
                            mig_pack.gc_currency_code_v,                                         --CURRENCY_CODE_V
                            NVL (acc_cur.installment_flag_v, 'N'),                               --INSTALLMENT_FLAG_V
                            acc_cur.bill_cycle_code,                                            --BILL_CYCL_CODE_N
                            mig_pack.gc_billing_region_v,                                        --BILLING_REGION_V
                            NULL,                                                                --BILL_GROUP_N
                            --DESPATCH_MEDIA_INDICATORS_V
                            acc_cur.dtls_to_fax_num_v,                                            --DTLS_TO_FAX_NUM_V
                            SUBSTR (acc_cur.email_list_v, 1, 100),                                --EMAIL_LIST_V
                            'N',                                                                  -- PAYER_OPTN_FLG_V
                            0,                                                                   --PAYER_CODE_N
                            NULL,                                                                --PAYMENTS_V
                            DECODE (TRIM(acc_cur.auto_debit_flg_v),'Direct Debit','Y','N'),       --AUTO_DEBIT_FLG_V
                            mig_pack.gc_dunning_schdl_code_v ,                                    --DUNNING_SCHDL_CODE_V
                            0,                                                                   --NEXT_INVOICE_SEQ_NUM_N
                            l_subscriber_cat_code_v,                                             --SUBSCRIBER_CATEGORY_V
                            l_risk_category_v,                                                   --RISK_CATEGORY_V
                            3,                                                                   --CREDIT_RATING_N
                            l_subscriber_sub_category_v,                                         --SUBSCRIBER_SUB_CATEGORY_V
                            mig_pack.gc_mig_user_code_n,                                         --ACTIVATED_BY_N                            
                            NVL (SUBSTR(acc_cur.account_name_v||' '||acc_cur.lastname,1,40),'.'),  --ACCOUNT_NAME_V
                            l_abillity_title_code_v,                                              --NVL (acc_cur.account_title_v, 'MR'),ACCOUNT_TITLE_V
                            'N',                                                                  --EXCLUDE_LPF
                            acc_cur.pre_termination_date_d,                                       --PRE_TERMINATION_DATE_D
                            mig_pack.gc_language_code_v,                                          -- BILL_PRESENTATION_LANG_V
                            'N',                                                                  --VIRTUAL_INVOICE_ACC_FLG_V
                            l_account_manager,                                                    --ACCOUNT_MANAGER_N
                            l_credit_controler,                                                   --CREDIT_CONTROLER_N
                            mig_pack.gc_account_type_v,                                           --ACCOUNT_TYPE_V
                            'Y',                                                                  --MUL_APPLICABLE_FLG_V
                            mig_pack.gc_currency_code_v,                                          --l_currency_code_v
                            mig_pack.gc_currency_code_v,                                          --BILL_PRESENT_CURR_CODE_V
                            acc_cur.account_number_v,                                             --OLD_ACCOUNT_NUM_V
                            'N',                                                                  --SPL_ATTRIBUTE_V
                            NULL,                                                                 -- ACC_ATTR_1_V
                            NULL,                                                                 -- ACC_ATTR_2_V
                            NULL,                                                                 -- ACC_ATTR_3_V
                            NULL,                                                                 -- ACC_ATTR_4_V
                            mig_pack.gc_billing_region_v,                                         -- ACC_ATTR_5_V
                            NULL,                                                                 -- ACC_ATTR_1_N
                            NULL,                                                                 -- ACC_ATTR_2_N
                            NULL,                                                                 -- ACC_ATTR_3_N
                            NULL,                                                                 -- ACC_ATTR_4_N
                            NULL,                                                                 -- ACC_ATTR_5_N
                            0,                                                                    -- POSTED_FLAG_N
                            NULL,                                                                 -- POSTED_DATE_D
                            mig_pack.gc_language_code_v,                                          --PREF_COMMUNICATION_LANG_V
                            l_citizen_id,                                                         -- ID_TYPE_V
                            'I',                                                                  --IND_SHARED_FLAG
                             0,                                                                   --NO_OF_SERVICES_N
                            'N',                                                                  --CREDIT_CONTROL_APPLIC_V
                             l_acc_code_n,                                                        --EXT_ACCOUNT_CODE_V
                            'A',                                                                  --TAX_OPTN_V
                            'I',                                                                  --ACCOUNT_TYPE_FLAG_V
                            'S',                                                                  --ADD_ACC_SERV_FLG_V
                             acc_cur.email_notifcn,                                                --SEND_EMAIL_NOTIFICATION_V
                             acc_cur.bill_dispatch                                                --DISPATCH_NOTIFY_OPTNS_N
                           );

                 l_debug_code_v := '18';
                 l_debug_msg_v :='Unable to insert data into CB_ENTITY_ADDNL_ATTRIBUTES for staging account :'|| acc_cur.account_number_v;                 
                  
                   INSERT INTO /*+APPEND*/CB_ENTITY_ADDNL_ATTRIBUTES
                               (
                                entity_type_v,
                                entity_code_n,
                                group_code_v,
                                created_date_d,
                                attribute_value_1_v,
                                attribute_value_2_v,
                                attribute_value_3_v,
                                attribute_value_4_v,
                                last_modified_by_n,
                                last_modified_date_d
                               )
                   
                     VALUES   ( 'I',                                            -- ENTITY_TYPE_V
                                 l_acc_code_n ,                                 -- ENTITY_CODE_N
                                'ACCHDR' ,                                      --GROUP_CODE_V
                                 acc_cur.act_date_d,                                  --CREATED_DATE_D
                                 NVL (SUBSTR(acc_cur.account_name_v,1,40),'.'),  -- ATTRIBUTE_VALUE_1_V ACCOUNT FIRST NAME                                  
                                 NULL ,
                                 NVL (SUBSTR(acc_cur.lastname,1,40),'.'),       --  ATTRIBUTE_VALUE_3_VACCOUNT LAST NAME
                                 NVL (SUBSTR(acc_cur.label,1,40),'.'),          --ATTRIBUTE_VALUE_4_V ACCOUNT HOLDER NAME
                                 mig_pack.gc_mig_user_code_n,                    -- LAST_MODIFIED_BY_N
                                 SYSTIMESTAMP                                   --LAST_MODIFIED_DATE_D
                                );
                                

                /* INSERT INTO cb_subs_heirarchy
                             (heirarchial_option_v,
                              subscriber_code_n,
                              parent_group_code_n,
                              subscriber_name_v, 
                              entity_type_v,
                              parent_entity_type_v,
                              main_group_code_n
                             )
                      VALUES (NVL (prof_cur.group_sub_group_code_flg_v, 'N'),                       --HEIRARCHIAL_OPTION_V,
                              l_check_acc_code_n,                                                   --SUBSCRIBER_CODE_N,
                              l_subs_code_n,                                                        --PARENT_GROUP_CODE_N,
                              prof_cur.first_name_v,                                                --SUBSCRIBER_NAME_V
                              'I',                                                                  --ENTITY_TYPE_V
                              DECODE (prof_cur.group_sub_group_code_flg_v,'N', 'P','P'),            --PARENT_ENTITY_TYPE_V
                              l_subs_code_n                                                         --MAIN_GROUP_CODE_N
                             ); */               

                 l_debug_code_v := '18.00';
                 l_debug_mesg_v :='Unable to insert into CB_ADDRESS for ACCOUNT ' || prof_cur.subscriber_code_v;

         INSERT      /*+ APPEND*/INTO cb_address
                     (account_link_code_n, 
                      address_type_n, 
                      account_type_v,
                      address_1_v,
                      address_2_v,
                      address_3_v,
                      address_4_v,
                      city_code_v,
                      state_code_v, 
                      country_code_v,
                      zip_code_v,
                      phone_number_v,                      
                      email_id_v,
                      po_box_v, 
                      local_flg_v,
                      area_code_v, 
                      district_v,                      
                      country_v,
                      province_v,
                      street_desc_v,
                      postofficecode,
                      city_desc,
                      district_desc,
                      landmark                      
                     )
              VALUES (l_acc_link_code_n,                                                         --ACCOUNT_LINK_CODE_N
                      3,                                                                         --ADDRESS_TYPE_N
                      'A',                                                                       --ACCOUNT_TYPE_V
                      acc_cur.street_code,                                                       --ADDRESS_1_V
                      acc_cur.plot_number,                                                       --ADDRESS_2_V
                      acc_cur.building,                                                          --ADDRESS_3_V
                      acc_cur.floor,                                                             --ADDRESS_4_V
                      acc_cur.sublocality_code,                                                  --CITY_CODE_V
                      acc_cur.cdistrict,                                                         -- STATE_CODE_V
                      mig_pack.gc_country_code_v,                                                --COUNTRY_CODE_V
                      acc_cur.postal_code,                                                       --ZIP_CODE_V
                      NULL,                                                                      --PHONE_NUMBER_V                      
                      NULL,                                                                      --EMAIL_ID_V
                      acc_cur.po_box,                                                            --PO_BOX_V
                      'P',                                                                       --LOCAL_FLG_V
                      NULL,                                                                      --AREA_CODE_V
                      acc_cur.locality_code,                                                     --DISTRICT_V                      
                      mig_pack.gc_country_name_v,                                                --COUNTRY_V
                      acc_cur.cdistrict,                                                         --PROVINCE_V
                      acc_cur.street,                                                            --STREET_DESC_V
                      acc_cur.postoffice_code,                                                   --POSTOFFICE_CODE
                      acc_cur.sublocality,                                                       --CITY_DESC,/SUB_LOCALITY_DESC
                      acc_cur.locality,                                                          --DISTRICT_DESC,
                      prof_cur.compl_adr                                                         --LANDMARK,                       
                     ); 
                      
               l_prof_acc_flag_v := 'Y';
               
               l_debug_code_v := '20';
               l_debug_msg_v :='Unable to insert into MAP_STG_CUST_ABILLITY_CODES for staging ACCOUNTS :'
                                || prof_cur.subscriber_code_v ||':'||acc_cur.account_number_v;

               INSERT      /*+ APPEND */INTO MAP_STG_CUST_ABILLITY_CODES
                           (stg_profile_code,
                            stg_account_code, 
                            subscriber_code,
                            account_code, 
                            main_account_link_code,
                            status, 
                            stg_customer_type,
                            bill_cycle_code,
                            subscriber_category_v,
                            subscriber_sub_category_v,
                            old_account_link_code,
                            group_subgroup_v,
                            cr_limit_n, 
                            title_v,
                            account_name,
                            id_type, 
                            risk_category,
                            despatch_media_v,
                            profile_type_v,
                            marital_status_v,
                            nationality_v,
                            contact_mobl_num_v,
                            contact_home_num_v,
                            contact_office_num_v,
                            email_id1_v, 
                            email_id2_v, 
                            birth_place_v,
                            business_type_v,
                            gender_v,
                            prof_first_name_v,
                            prof_last_name_v,
                            language_code_v, 
                            date_of_birth_d,
                            identification_num_v                           
                           )
                    VALUES (prof_cur.subscriber_code_v,                                           --STG_PROFILE_CODE
                            acc_cur.account_number_v,                                             --STG_ACCOUNT_CODE
                            l_subs_code_n,                                                        --SUBSCRIBER_CODE
                            l_acc_code_n,                                                         --ACCOUNT_CODE
                            l_acc_link_code_n,                                                  --MAIN_ACCOUNT_LINK_CODE
                            l_status_code_v,                                                    --STATUS
                            prof_cur.profile_type_v,                                            --STG_CUSTOMER_TYPE
                            acc_cur.bill_cycle_code,                                            --BILL_CYCLE_CODE
                            l_subscriber_cat_code_v,                                            --SUBSCRIBER_CATEGORY_V
                            l_subscriber_sub_category_v,                                        --SUBSCRIBER_SUB_CATEGORY
                            acc_cur.account_number_v,                                           --account_link_code_v
                            DECODE (prof_cur.group_sub_group_code_flg_v,
                                    'G', 'G',
                                    'S', 'S',
                                    'N'
                                   ),                                                           --GROUP_SUBGROUP_V
                            acc_cur.cr_limit_n, 
                            l_abillity_title_code_v,                                            --ACC_CUR.ACCOUNT_TITLE_V,
                            NVL (acc_cur.ACCOUNT_NAME_V||' '||acc_cur.lastname,'.'),           --ACCOUNT_NAME_V
                            NVL (l_citizen_id, 'OTHERS'), 
                            l_risk_category_v,                                                  --L_RISK_CAT_CODE_V
                            l_despatch_media_indicators_v,
                            prof_cur.prof_type_v,
                            prof_cur.prof_marital_status_v,                                     --MARITAL_STATUS_V
                            l_nationality_v,
                            NVL (SUBSTR (prof_cur.contact_mobl_num_v, 1, 20),l_dflt_contact_v), --MOBILE_NUMBER_V
                            SUBSTR (prof_cur.contact_home_num_v, 1, 20),                        --HOUSE_NUMBER_V
                            SUBSTR (prof_cur.contact_office_num_v, 1, 20),                      --OFFICE_NUMBER_V
                            TRIM (prof_cur.email_id1_v), NULL, NULL,
                            NULL,
                            prof_cur.prof_gender_v,                                             --GENDER_V
                            NVL (prof_cur.first_name_v,'.'),                                    -- PROF_FIRST_NAME_V
                            NVL(SUBSTR(prof_cur.last_name_v,1,40),'.'),                         -- PROF_LAST_NAME_V
                            mig_pack.gc_language_code_v,                                        --LANGUAGE_CODE_V
                            prof_cur.dob,                                                       -- DATE_OF_BIRTH_D 
                            NVL (prof_cur.identification_num_v,'MTG'||l_subs_code_n)--IDENTIFICATION_NUM_V                            
                           );
            END;
         END LOOP;

           l_cnt := l_cnt + 1;

            IF l_cnt =1500
            THEN
               mig_pack.log_progress (l_procedure_name, l_cnt);
               l_cnt := 0;
               COMMIT;
            END IF;
         
      EXCEPTION
         WHEN OTHERS
         THEN
            ROLLBACK TO mysavepoint;
            mig_pack.log_error (l_debug_code_v || ' @L' || $$plsql_line,
                                l_debug_msg_v || ':' || SQLERRM,
                                l_procedure_name,
                                l_subs_code_n
                                || ':'
                                || l_acc_link_code_n
                                || ':'
                                || prof_cur.subscriber_code_v
                               );
      END;
   END LOOP;

   COMMIT;
   l_debug_code_v := '09.00';
   l_debug_mesg_v := 'Finishing MIG_PROCESS_CONTROL for ' || l_procedure_name;
   mig_pack.end_process (l_procedure_name, l_cnt);
EXCEPTION
   WHEN OTHERS
   THEN
      ROLLBACK TO mysavepoint;
      mig_pack.log_error (l_debug_code_v || ' @L' || $$plsql_line,
                          l_debug_msg_v || ':' || SQLERRM,
                          l_procedure_name
                         );                       
      mig_pack.end_process (l_procedure_name);
END;