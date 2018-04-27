
%let EmployeeData = "&InputPath.DW_Employee_Update_20180401.xlsx";


/* Import data into Staging */

/* 1. Orkide */


proc import
   replace
   datafile=&EmployeeData
   out=St_Employees_ORK
   dbms=xlsx;
run;

data work.st_employees_ORK_input;
   informat SourceID $10. UserID $10. Dept_ID $10. CPR_nr $10.;
   set work.st_employees_ORK;
   SourceID = SourceUID;
   CPR_nr = CPR;
   Dept_ID = DeptID;
   DW_RecordSource = 'ORK';
   SeqNo = _n_;
   DW_Insert_DateTime = &LoadDate;
   DW_LogRef = -1;
   
   drop SourceUID CPR;
run;

proc sql; Create table work.St_Employees_ORK as
   select
        SourceID
      , CPR_nr
      , UserID
      , DeptAbb
      , Name
      , EmploymentDate
      , TerminationDate
      , EmploymentHoursNorm
      , strip(Dept_ID) as DeptID
      , md5(catx(';',CPR_nr,'ORK')) as EmployeeHashKey format $hex32. length=16
      , md5(catx(';',CPR_nr,strip(Dept_ID),'ORK')) as Emp_dept_HashKey format $hex32. length=16
      , md5(catx(';',SourceID,'ORK')) as EmploymentHashKey format $hex32. length=16
      , md5(catx(';',strip(Dept_ID),'ORK')) as DeptHashKey format $hex32. length=16
      , md5(catx(';',strip(Name),strip(UserID),'ORK')) as hd_Employee_info format $hex32. length=16
      , md5(catx(';',SourceID,EmploymentHoursNorm,EmploymentDate,TerminationDate,P_Number,'ORK')) as hd_employment_info format $hex32. length=16
      , P_Number
      , DW_RecordSource
      , SeqNo
      , DW_Insert_DateTime
      , DW_LogRef
   from work.st_Employees_ORK_input
   ;

proc sql;
   Delete * from nooyehe.St_Employees_ORK;

proc append base=nooyehe.st_Employees_ORK data=work.St_Employees_ORK force;

/*
   Update Raw hubs

   1. Orkide
*/

proc sort data=nooyehe.st_Employees_ORK;
   by EmployeeHashKey;

proc sort data=&libName..h_employee;
   by h_Key;

data Add_Employee Update_Employee Removed_Employee;
   merge
      nooyehe.st_employees_ORK (rename=(EmployeeHashKey=H_Key) in=DS1)
      &libName..h_Employee
      (in=DS2 keep=H_Key);
   by H_Key;

   if DS2=1 and DS1=1 then do;
      output Update_Employee;
   end;
   if DS2=0 and DS1=1 then do;
      DW_LastSeenDate = &LoadDate;
      DW_Bus_Key = CPR_nr;
      output Add_Employee;
   end;
   if DS2=1 and DS1=0 then do;
      output Removed_Employee;
   end;
   drop  CPR_nr AreaID UserID DeptHashKey
         Name P_Number DeptID EmploymentDate
         TerminationDate EmploymentHoursNorm SourceID
         SeqNo hd_employee_info hd_employment_info
         AreaHashKey employmentHashKey;
run;

proc append base=&libName..h_employee data=work.Add_Employee force;

proc sql;
   update &libName..h_employee
      set DW_LastSeenDate = &LoadDate
   where H_Key in
      (select H_Key from work.update_employee);

proc sort data=nooyehe.st_employees_ork;
   by hd_employee_info;
run;

/*
   Update Raw satellites

   1. Orkide 
*/

proc sort data=&libName..s_h_ORK_employee_info;
   by h_diff;
run;

data Exist_employee_info add_employee_info end_employee_info;
   merge &libName..s_h_ORK_employee_info (in=SD1)
         nooyehe.st_Employees_ORK
         (
            in=SD2
            rename=
            (
               employeehashkey=h_key
               hd_employee_info=h_diff
            )
         );
      by h_diff;
      
   if SD1 = 1 and SD2 = 1 then do;
      output Exist_employee_info;
   end;
      
   if SD1 = 1 and SD2 = 0 then do;
      output End_employee_info;
   end;
   
   if SD1 = 0 and SD2 = 1 then do;
      output add_employee_info;
   end;
   
   keep h_key DW_Insert_DateTime LoadEndDate DW_RecordSource h_diff Name UserID;
run;

proc append base=&libName..s_h_ORK_employee_Info data=work.Add_Employee_info force;
run;

proc sql;
   update &libName..s_h_ork_employee_info
      set LoadEndDate = &LoadDate
   where h_key in
      (select h_key from work.add_employee_info)
      and LoadEndDate eq . and DW_Insert_DateTime ^= &LoadDate;

proc sort data=nooyehe.st_employees_ork;
   by hd_employment_info;
run;

proc sort data=&libName..s_h_ORK_employment_info;
   by h_diff;
run;

data Exist_employment_info add_employment_info end_employment_info;
   merge &libName..s_h_ORK_employment_info 
         (
            in=SD1
         )
         nooyehe.st_Employees_ORK
         (
            in=SD2
            rename=
            (
               employeehashkey=h_key
               hd_employment_info=h_diff
            )
         );
      by h_diff;
      
   if SD1 = 1 and SD2 = 1 then do;
      output Exist_employment_info;
   end;
      
   if SD1 = 1 and SD2 = 0 then do;
      output End_employment_info;
   end;
   
   if SD1 = 0 and SD2 = 1 then do;
      output add_employment_info;
   end;
   
   keep  h_key DW_Insert_DateTime LoadEndDate DW_RecordSource
         dw_LogRef h_diff
         employmentHoursNorm employmentDate terminationDate P_Number SourceID;
run;

proc append base=&libName..s_h_ORK_employment_Info data=work.add_employment_info force;
run;

proc sql;
   update &libName..s_h_ork_employment_info
      set LoadEndDate = &LoadDate
   where h_key in
      (select h_key from work.add_employment_info)
      and LoadEndDate eq . and DW_Insert_DateTime ^= &LoadDate;

/*
   Update Raw Links

1. Orkide
*/

proc sort data=nooyehe.st_employees_ORK;
   by Emp_dept_HashKey;
run;

proc sort data=&libName..l_emp_Dept;
   by h_Key;
run;

data Exist_emp_dept_link add_emp_dept_link end_emp_dept_link;
   merge &libName..l_emp_dept (in=SD1) nooyehe.st_employees_ORK (in=SD2 rename=(Emp_dept_HashKey=h_key));
      by h_key;
      
   if SD1 = 1 and SD2 = 1 then do;
      output Exist_emp_dept_link;
   end;
      
   if SD1 = 1 and SD2 = 0 then do;
      output End_emp_dept_link;
   end;
   
   if SD1 = 0 and SD2 = 1 then do;
      dw_log_ref = -1;
      output add_emp_dept_link;
   end;
   
   keep h_key DW_Insert_DateTime DW_LogRef DW_RecordSource
   employeeHashkey DeptHashkey;
run;

proc append base=&libName..l_emp_dept data=work.Add_emp_dept_link force;
run;


