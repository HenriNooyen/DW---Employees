%let CompanyAreaData = "&InputPath.DW_area_Base.xlsx";

proc import
   replace
   datafile=&CompanyAreaData
   out=St_Areas_Base_input
   dbms=xlsx;
run;

data St_Areas_Base;
   set St_Areas_Base_input;

   SeqNo = _n_;
   DW_RecordSource = 'ORK'; 
   DW_Insert_DateTime = &LoadDate;
   DW_LogRef = -1;
run;

proc sql; Create table st_Areas_base_updated as
   select
        md5(catx(';',strip(a.AreaID),'ORK')) as AreaHashKey format $hex32. length=16
      , md5(catx(';',strip(Name),strip(abbr),'ORK')) as hd_area_info format $hex32. length=16
      , a.AreaID
      , a.DW_Insert_DateTime
      , a.DW_RecordSource
      , a.DW_LogRef
      , a.abbr
      , a.Name
      , a.SeqNo
   from work.st_Areas_base as a;

proc sql;
   Delete * from nooyehe.St_Areas_Base;

proc append base=nooyehe.st_Areas_Base data=work.St_Areas_Base_updated force;

proc sort data=nooyehe.st_areas_Base;
   by AreaHashkey;
run;

proc sort data=nooyehe.h_Company_Area;
   by h_Key;

data add_areas;
   merge nooyehe.St_Areas_Base (in=DS1 drop=SeqNo rename=(AreaHashKey=h_key))
         nooyehe.h_Company_Area (in=DS2);
      by h_key;
      
   if DS1=1 and DS2=0 then do;
      DW_Bus_Key = AreaID;
      output add_areas;
   end;
   
   drop AreaID Name Abbr hd_area_info;
run;

proc append Base=nooyehe.h_Company_Area data=add_areas force;
run;

proc sort data=nooyehe.St_Areas_Base;
   by hd_area_info;
run;

proc sort data=nooyehe.s_h_ORK_company_area_info;
   by h_diff;
run;

data Exist_area_info add_area_info end_area_info;
   merge nooyehe.s_h_ORK_company_area_info (in=SD1) nooyehe.st_areas_base (in=SD2 rename=(hd_area_info=h_diff areahashkey=h_key));
      by h_diff;
      
   if SD1 = 1 and SD2 = 1 then do;
      output Exist_area_info;
   end;
      
   if SD1 = 1 and SD2 = 0 then do;
      output End_area_info;
   end;
   
   if SD1 = 0 and SD2 = 1 then do;
      output add_area_info;
   end;
   
   keep  h_key DW_Insert_DateTime LoadEndDate DW_RecordSource
         dw_LogRef h_diff
         abbr name;
run;

proc append base=nooyehe.s_h_ORK_company_area_Info data=work.add_area_info force;
run;

proc sql;
   update nooyehe.s_h_ORK_company_area_info
      set LoadEndDate = &LoadDate
   where h_key in
      (select h_key from work.add_area_info)
      and LoadEndDate eq . and DW_Insert_DateTime ^= &LoadDate;



