proc sql; create table St_Depts_PNK as
   select
        a.call_center
      , a.ch_due_list
      , a.ch_own_cases
      , a.result_unit
      , a.team_type
      , a.Name
      , md5(catx(';',strip(a.call_center),strip(a.ch_due_list),strip(a.ch_own_cases),strip(a.result_unit),strip(a.team_type),strip(a.Name),strip(a.office_place),strip(a.usable),'PNK')) as hd_Dept_info format $hex32. length=16
      , MD5(catx(';',strip(Team),'PNK')) as DeptHashKey format $hex32. length=16
      , 'PNK' as DW_RecordSource
      , -1 as DW_LogRef
      , a.Team
      , a.Office_Place
      , a.usable
   from dw_pink.Team as a;

data St_Depts_PNK;
   set St_Depts_PNK;
   SeqNo = _n_;
   DW_Insert_DateTime = &LoadDate;
run;

proc sort data=st_Depts_PNK;
   by DeptHashKey;

proc sort data=nooyehe.h_Department;
   by h_Key;

data Add_dept Update_dept Removed_dept;
   merge
      st_Depts_PNK (rename=(DeptHashKey=H_Key) in=DS1)
      nooyehe.h_Department
      (in=DS2 keep=H_Key);
   by H_Key;

   if DS2=1 and DS1=1 then do;
      output Update_dept;
   end;
   if DS2=0 and DS1=1 then do;
      DW_Bus_Key = Team;
      output Add_Dept;
   end;
   if DS2=1 and DS1=0 then do;
      output Removed_Dept;
   end;
   drop  hd_dept_info SeqNo
         call_center ch_due_list ch_own_cases result_unit team_type
         Name Office_Place Team usable
;
run;

proc append base=nooyehe.h_Department data=work.Add_dept force;

proc sort data=st_Depts_PNK;
   by hd_dept_info;
run;

proc sort data=nooyehe.s_h_PNK_Dept_info;
   by h_diff;
run;

data Exist_dept_info add_dept_info end_dept_info;
   merge nooyehe.s_h_PNK_dept_info (in=SD1) st_depts_PNK (in=SD2 rename=(hd_dept_info=h_diff Depthashkey=h_key));
      by h_diff;
      
   if SD1 = 1 and SD2 = 1 then do;
      output Exist_dept_info;
   end;
      
   if SD1 = 1 and SD2 = 0 then do;
      output End_dept_info;
   end;
   
   if SD1 = 0 and SD2 = 1 then do;
      output add_dept_info;
   end;
   
   keep  h_key DW_Insert_DateTime DW_LogRef LoadEndDate DW_RecordSource h_diff
         call_center ch_due_list ch_own_cases result_unit team_type
         Name Office_Place usable;
run;

proc append base=nooyehe.s_h_PNK_dept_Info data=work.Add_dept_info force;
run;

proc sql;
   update nooyehe.s_h_PNK_dept_info
      set LoadEndDate = &LoadDate
   where h_key in
      (select h_key from work.add_dept_info)
      and LoadEndDate eq . and DW_Insert_DateTime ^= &LoadDate;



/*
%let DepartmentData = "&InputPath.DW_Department_Base_20171201.xlsx";

proc import
   replace
   datafile=&DepartmentData
   out=St_Depts_Base_input
   dbms=xlsx;
run;

data St_Depts_Base;
   informat Dept_ID $10.;
   set St_Depts_Base_input;
   SeqNo = _n_;
   Dept_ID = DeptID;
   DW_RecordSource = 'ORK';
   DW_Insert_DateTime = &LoadDate;
   DW_LogRef = -1;
   
   drop DeptID;
run;

proc sql; create table st_Depts_Base_updated as
   select
        a.Dept_ID as DeptID
      , a.DeptName
      , a.DebtAbb as DeptAbb
      , a.AreaID
      , a.SeqNo
      , a.DW_RecordSource
      , a.DW_Insert_DateTime
      , a.DW_LogRef
      , md5(catx(';',strip(a.Dept_ID),strip(a.AreaID),'ORK')) as LinkHashKey format $hex32. length=16
      , md5(catx(';',strip(a.AreaID),'ORK')) as AreaHashKey format $hex32. length=16
      , md5(catx(';',strip(a.Dept_ID),'ORK')) as DeptHashKey format $hex32. length=16
      , md5(catx(';',strip(a.DeptName),strip(a.DebtAbb),'ORK')) as hd_Dept_info format $hex32. length=16
   from work.st_Depts_Base as a;

proc sql;
   Delete * from nooyehe.St_Depts_Base;

proc append base=nooyehe.st_Depts_Base data=work.St_Depts_Base_updated force;

proc sort data=nooyehe.st_Depts_Base;
   by DeptHashKey;

proc sort data=nooyehe.h_Department;
   by h_Key;

data Add_dept Update_dept Removed_dept;
   merge
      nooyehe.st_depts_base (rename=(DeptHashKey=H_Key) in=DS1)
      nooyehe.h_Department
      (in=DS2 keep=H_Key);
   by H_Key;

   if DS2=1 and DS1=1 then do;
      output Update_dept;
   end;
   if DS2=0 and DS1=1 then do;
      DW_Bus_Key = DeptID;
      output Add_Dept;
   end;
   if DS2=1 and DS1=0 then do;
      output Removed_Dept;
   end;
   drop  LinkHashKey AreaHashKey DeptID SeqNo hd_dept_info Deptabb DeptName AreaID;
run;

proc append base=nooyehe.h_Department data=work.Add_dept force;

proc sort data=nooyehe.st_Depts_base;
   by hd_dept_info;
run;

proc sort data=nooyehe.s_h_ORK_Dept_info;
   by h_diff;
run;

data Exist_dept_info add_dept_info end_dept_info;
   merge nooyehe.s_h_ORK_dept_info (in=SD1) nooyehe.st_depts_Base (in=SD2 rename=(hd_dept_info=h_diff Depthashkey=h_key));
      by h_diff;
      
   if SD1 = 1 and SD2 = 1 then do;
      output Exist_dept_info;
   end;
      
   if SD1 = 1 and SD2 = 0 then do;
      output End_dept_info;
   end;
   
   if SD1 = 0 and SD2 = 1 then do;
      output add_dept_info;
   end;
   
   keep h_key DW_Insert_DateTime DW_LogRef LoadEndDate DW_RecordSource h_diff
   DeptName DeptAbb;
run;

proc append base=nooyehe.s_h_ORK_dept_Info data=work.Add_dept_info force;
run;

proc sql;
   update nooyehe.s_h_ORK_dept_info
      set LoadEndDate = &LoadDate
   where h_key in
      (select h_key from work.add_dept_info)
      and LoadEndDate eq . and DW_Insert_DateTime ^= &LoadDate;

proc sort data=nooyehe.st_Depts_base;
   by LinkHashKey;
run;

proc sort data=nooyehe.l_Dept_area;
   by h_Key;
run;

data Exist_dept_area_link add_dept_area_link end_dept_area_link;
   merge nooyehe.l_dept_area (in=SD1) nooyehe.st_depts_Base (in=SD2 rename=(Linkhashkey=h_key));
      by h_key;
      
   if SD1 = 1 and SD2 = 1 then do;
      output Exist_dept_area_link;
   end;
      
   if SD1 = 1 and SD2 = 0 then do;
      output End_dept_area_link;
   end;
   
   if SD1 = 0 and SD2 = 1 then do;
      dw_log_ref = -1;
      output add_dept_area_link;
   end;
   
   keep h_key DW_Insert_DateTime DW_LogRef DW_RecordSource
   AreahashKey DeptHashkey;
run;

proc append base=nooyehe.l_dept_area data=work.Add_dept_area_link force;
run;
