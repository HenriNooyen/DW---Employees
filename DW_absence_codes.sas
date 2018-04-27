data Absence_codes;
   infile datalines dsd;

   input ID : $3. Name : $100. ;   
   datalines;
   11,"Egen sygdom"
   12,"Barn syg"
   16,"Fædreorlov"
   21,"Barselsrelateret sygdom"
   22,"Barsel med løn"
   23,"Forældreorlov"
   26,"Kursus internt"
   27,"Kursus eksternt"
   32,"Barns hospitalsindlæggelse"
   44,"Fritstillet"
   47,"Afholdt timebank"
   48,"Seniordage"
   50,"Ferie"
   55,"Afholdt afspadsering/flex"
   56,"Fri egen regning"
   60,"Omsorgsdag"
   61,"Orlov i timer u/løn m/pension"
   62,"Delbarsel uden/løn efter 60 uger"
   63,"Barsel u/løn og m/pension"
   70,"Barsel u/løn og u/pension"
   71,"Orlov timer u/løn, m/pension"
   72,"Time orlov u/løn, m/pension"
   9,"Timer sygedagpenge"
   99,"Aftalt fravær med løn"
   999,"Ferie overført fra iNet"
;
run;

proc sql; create table input_absence_codes as
   select
        md5(catx(';',aa.id,'ORK')) as H_key format $hex32. length=16
      , md5(catx(';',aa.Name,'ORK')) as H_Diff format $hex32. length=16
      , &LoadDate as DW_Insert_DateTime
      , -1 as DW_LogRef
      , aa.id as DW_Bus_Key
      , 'ORK' as DW_RecordSource
      , aa.Name
    from work.absence_codes as aa
    ; 

data work.input_absence_codes;
   set work.input_absence_codes;
   SeqNo = _n_;
run;

proc sort data=work.input_absence_codes;
   by H_Key;

proc sort data=nooyehe.rh_ORK_absence_codes;
   by h_Key;

data Add_absence_code Update_absence_code Removed_absence_code;
   merge
      work.input_absence_codes (in=DS1)
      nooyehe.rh_ORK_absence_codes
      (in=DS2 keep=H_Key);
   by H_Key;

   if DS2=1 and DS1=1 then do;
      output Update_absence_code;
   end;
   if DS2=0 and DS1=1 then do;
      DW_Bus_Key = ID;
      output Add_Absence_code;
   end;
   if DS2=1 and DS1=0 then do;
      output Removed_absence_code;
   end;
   drop  SeqNo
         Name ID H_Diff;
run;

proc append base=nooyehe.rh_ORK_absence_codes data=work.Add_absence_code force;

proc sort data=work.input_absence_codes;
   by h_diff;
run;

proc sort data=nooyehe.s_rh_ORK_absence_info;
   by h_diff;
run;

data Exist_absence_info add_absence_info end_absence_info;
   merge nooyehe.s_rh_ORK_absence_info (in=SD2)
         work.input_absence_codes (in=SD1);
      by h_diff;
      
   if SD1 = 1 and SD2 = 1 then do;
      output Exist_absence_info;
   end;
      
   if SD1 = 0 and SD2 = 1 then do;
      output End_absence_info;
   end;
   
   if SD1 = 1 and SD2 = 0 then do;
      output add_absence_info;
   end;
   
   keep h_key DW_Insert_DateTime LoadEndDate DW_LogRef DW_RecordSource h_diff Name;
run;

proc append base=nooyehe.s_rh_ORK_absence_Info data=work.Add_absence_info force;
run;

proc sql;
   update nooyehe.s_rh_ork_absence_info
      set LoadEndDate = &LoadDate
   where h_key in
      (select h_key from work.add_absence_info)
      and LoadEndDate eq . and DW_Insert_DateTime ^= &LoadDate;
