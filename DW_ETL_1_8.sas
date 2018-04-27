/* STATUS

Alle medarbejdere er indlæst i staging. Orkide er indlæst i RAW (hubs, satellites og links)
Den gamle afdelingsstruktur er indlæst i staging og raw

Næste skridt:
1.    Done - Indlæsning af medarbejdere fra Orkide i Raw hub, link og satellite
2.    Done - Implementering af fraværskoder i RAW hub og satellite
3.    Done - Create back-up and restore set-up
4.    indlæsning af medarbejdere fra GuG i Raw hub og satellite
5.    indlæsning af medarbejdere fra PINK i Raw hub og satellite
6.    Oprettelse af same-as link på medarbejdere med et c-ident på tværs af systemerne
7.    Oprettelse af "MDM" business satellite på medarbejder info
8.    Done - Oprettelse af business hub på medarbejderne (for at skjule CPR-nummer fra almindelige brugere)
9.    Done - Oprettelse af nye afdelinger (cost center niveau) i raw
10.   Done - test af sammenhænge mellem medarbejdere, costcentre (afdelinger) i RAW
11.   Done - test af sammenhænge mellem costcentre (afdelinger) og divisioner i RAW
12.   Done - Indlæsning af afdelinger fra PINK i raw hub og satellite
13.   Indlæsning af afdelinger fra GuG i raw hub og satellite
14.   Indlæsning af medarbejder/afdelings relation fra GuG
15.   Indlæsning af medarbejder/afdelings relation fra PINK 
16.   Oprettelse af "MDM" business hubs for teams, afdelinger, business units
17.   Oprettelse af cost struktur på "Department" og business area niveau (business vault)
18.   OPrettelse af roller/afdelingsreference model til styring af ledelses rapportering
19.   Implementering af fraværsregistreringeer i Absence link tabel
20.   Udvikling af datamart for cost afdelingsstruktur
21.   udviking af datamart for reference afdelingsstruktur
22.   Udvikling af datamart for fraværsrapportering
23.   Udvkiling af datamart for sagshistorik i relation til afdelinger

*/

%let InputPath = \\groupad1.com\Home\Denmark-Horsens\nooyehe\Data\report_files\input_files\;
%let ProgramPath = \\groupad1.com\Home\Denmark-Horsens\nooyehe\Data\sas_programs\DW_programs\;
%let LoadDate = '01apr2018'd;
%let LibName = nooyehe;

%INCLUDE "&ProgramPath.DW_RAW_employees_1_1.sas" /lrecl=10000;
/* %INCLUDE "&ProgramPath.DW_RAW_company_area.sas" /lrecl=10000; */
/* %INCLUDE "&ProgramPath.Dw_RAW_Departments.sas" /lrecl=10000; */
/* %INCLUDE "&ProgramPath.Dw_absence_codes.sas" /lrecl=10000; */
/* %INCLUDE "&programPath.DW_BUS_employees.sas" /lrecl=10000;

proc sql; create table work.St_Saksbehandler as
   select
        a.Team
      , a.bevillingshaver
      , a.date_changed
      , a.date_created
      , a.e_post_domene
      , a.e_post_navn
      , a.kontorsted
      , a.Navn
      , a.P_asc
      , a.saksbeh
      , a.short_name
      , a.show_phone_no_deb
      , a.skriver
      , a.telefax
      , a.telefonnr
      , a.tittel
      , a.cident_user_id
      , MD5(catx(';',strip(a.cident_user_id),'PNK')) as EmployeeHashKey format $hex32. length=16
      , MD5(catx(';',strip(a.Team),'PNK')) as DeptHashKey format $hex32. length=16
      , MD5(catx(';'
         ,strip(a.bevillingshaver)
         ,strip(a.e_post_domene)
         ,strip(a.e_post_navn)
         ,strip(a.kontorsted)
         ,strip(a.Navn)
         ,strip(a.P_asc)
         ,strip(a.saksbeh)
         ,strip(a.short_name)
         ,strip(a.show_phone_no_deb)
         ,strip(a.skriver)
         ,strip(a.telefax)
         ,strip(a.telefonnr)
         ,strip(a.tittel)
         ,strip(a.usable)
         ,strip(a.ch_due_list)
         ,strip(a.ch_own_cases)
         ,strip(a.list_active_inquiry)
         ,'PNK')) as hd_Employee_info format $hex32. length=16
      , a.usable
      , a.ch_due_list
      , a.ch_own_cases
      , a.list_active_inquiry
      , -1 as DW_LogRef
      , 'PNK' as DW_RecordSource
      , &LoadDate as DW_Insert_DateTime
   from dw_pink.saksbehandler as a
;

data work.St_Saksbehandler;
   set work.St_Saksbehandler;

   SeqNo = _n_;
run;

proc sql;
   Delete * from nooyehe.St_Saksbehandler_PNK;

proc append base=nooyehe.st_Saksbehandler_PNK data=work.St_Saksbehandler force;

proc sort data=nooyehe.st_Saksbehandler_PNK;
   by EmployeeHashKey;

proc sort data=nooyehe.h_employee;
   by h_Key;

data Add_Employee Update_Employee Removed_Employee;
   merge
      nooyehe.st_saksbehandler_PNK (rename=(EmployeeHashKey=H_Key) in=DS1)
      nooyehe.h_Employee
      (in=DS2 keep=H_Key);
   by H_Key;

   if DS2=1 and DS1=1 then do;
      output Update_Employee;
   end;
   if DS2=0 and DS1=1 then do;
      DW_LastSeenDate = &LoadDate;
      DW_Bus_Key = UserID;
      output Add_Employee;
   end;
   if DS2=1 and DS1=0 then do;
      output Removed_Employee;
   end;
   drop  DeptHashKey SeqNo hd_employee_info
         bevillingshaver e_post_domene e_post_navn kontorsted Navn P_asc
         saksbeh short_name show_phone_no_deb skriver telefax telefonnr
         tittel cident_user_id usable ch_due_list ch_own_cases list_active_inquiry
         date_changed date_created;
run;

proc append base=nooyehe.h_employee data=work.Add_Employee force;

proc sql;
   update nooyehe.h_employee
      set DW_LastSeenDate = &LoadDate
   where H_Key in
      (select H_Key from work.update_employee);

proc sort data=nooyehe.st_saksbehandler_PNK;
   by hd_employee_info;
run;

proc sort data=nooyehe.s_h_PNK_employee_info;
   by h_diff;
run;

data Exist_employment_info add_employment_info end_employment_info;
   merge nooyehe.s_h_PNK_employee_info 
         (
            in=SD1
         )
         nooyehe.st_saksbehandler_PNK
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
         bevillingshaver e_post_domene e_post_navn kontorsted Navn
         P_asc saksbeh short_name show_phone_no_deb skriver
         telefax telefonnr tittel usable ch_due_list
         ch_own_cases list_active_inquiry
;
run;

proc append base=nooyehe.s_h_PNK_employee_Info data=work.add_employee_info force;
run;

proc sql;
   update nooyehe.s_h_PNK_employment_info
      set LoadEndDate = &LoadDate
   where h_key in
      (select h_key from work.add_employee_info)
      and LoadEndDate eq . and DW_Insert_DateTime ^= &LoadDate;

/*
proc sql; create table work.St_CaseWorkers as   
   select
        a.initials
      , a.Name
      , a.departmentId
      , 'GUG' as DW_RecordSource
      , &LoadDate as DW_Insert_DateTime
      , -1 as DW_LogRef
      , md5(catx(';',a.active,a.retired,strip(b.loginName),'GUG')) as HD_Employee_info format $hex32. length=16
      , md5(catx(';',Strip(a.departmentID),'GUG')) as DeptHashKey format $hex32. length=16
      , a.active
      , a.retired
      , b.loginName
      , md5(catx(';',Strip(a.initials),'GUG')) as EmployeeHashKey format $hex32. length=16
   from gugdrift.v_caseworkers as a
      left join gugdrift.caseworkers as b
         on a.caseworker_oid = b.caseworker_oid
   order by a.initials
   ;

data work.St_CaseWorkers;
   set work.St_CaseWorkers;
   SeqNo = _n_;
run;

proc sql;
   Delete * from nooyehe.St_CaseWorkers_GuG;

proc append base=nooyehe.st_CaseWorkers_GuG data=work.st_CaseWorkers force;

proc sort data=nooyehe.st_CaseWorkers_GuG;
   by EmployeeHashKey;

proc sort data=nooyehe.h_employee;
   by h_Key;

data Add_Employee Update_Employee Removed_Employee;
   merge
      nooyehe.st_caseworkers_GuG (rename=(EmployeeHashKey=H_Key) in=DS1)
      nooyehe.h_Employee
      (in=DS2 keep=H_Key);
   by H_Key;

   if DS2=1 and DS1=1 then do;
      output Update_Employee;
   end;
   if DS2=0 and DS1=1 then do;
      DW_LastSeenDate = &LoadDate;
      DW_Bus_Key = Initials;
      output Add_Employee;
   end;
   if DS2=1 and DS1=0 then do;
      output Removed_Employee;
   end;
   drop  DeptHashKey SeqNo hd_employee_info
         Name departmentId active retired LoginName initials;
run;

proc append base=nooyehe.h_employee data=work.Add_Employee force;

proc sql;
   update nooyehe.h_employee
      set DW_LastSeenDate = &LoadDate
   where H_Key in
      (select H_Key from work.update_employee);

proc sort data=nooyehe.st_caseworkers_GuG;
   by hd_employee_info;
run;

proc sort data=nooyehe.s_h_GuG_employee_info;
   by h_diff;
run;

data Exist_employment_info add_employment_info end_employment_info;
   merge nooyehe.s_h_GuG_employee_info 
         (
            in=SD1
         )
         nooyehe.st_caseworkers
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
         active retired loginName
;
run;

proc append base=nooyehe.s_h_GuG_employee_Info data=work.add_employee_info force;
run;

proc sql;
   update nooyehe.s_h_GuG_employment_info
      set LoadEndDate = &LoadDate
   where h_key in
      (select h_key from work.add_employee_info)
      and LoadEndDate eq . and DW_Insert_DateTime ^= &LoadDate;

proc sql; create table St_depts_GuG as
   select     a.DepartmentID as DeptID
         , a.Name
         , a.DepartmentLocation
   from gugdrift.departments as a;

data St_Depts_GuG;
   set St_Depts_GuG;
   DeptHashKey = MD5(catx(';',DeptID,'GUG');
   SeqNo = _n_;
   RecordSource = 'GUG';
   LoadDate = Today();
run;

%let InputFile4 = "&InputPath.DW_Roles_Base.xlsx";

proc import
   replace
   datafile=&InputFile4
   out=St_Roles_Base
   dbms=xlsx;
run;

data St_Roles_Base;
   set St_Roles_Base;

   SeqNo = _n_;
   RecordSource = 'RBF'; /* Role Base File
   LoadDate = &LoadDate;
run;

proc sql; create table st_Roles_base as
   select
        a.RoleID
      , md5(strip(a.RoleID)) as RoleHashKey format $hex32. length=16
      , a.SeqNo
      , a.LoadDate
      , a.RecordSource
      , a.Title_DK
      , a.Title_EN
   from work.st_Roles_base as a;

proc sql;
   Delete * from nooyehe.St_Roles_Base;

proc append base=nooyehe.st_Roles_Base data=work.St_Roles_Base force;

proc sort data=nooyehe.st_Roles_Base;
   by RoleID;
run;

end on not imported data */

