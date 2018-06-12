   
libname dw "F:\DK_DataWarehouse_test";

proc datasets library=dw kill;
run; 

 /*  proc datasets library=nooyehe;
      delete h_Employee;
      delete h_Bus_Employee;
      delete h_company_area;
      delete h_Department;
      delete st_Employees_ORK;
      delete st_CaseWorkers_GuG;
      delete st_Saksbehandler_PNK;
      delete st_Areas_Base;
      delete st_Depts_Base;
      delete st_Depts_PNK;
      delete st_Roles_Base;
      delete s_h_ORK_employee_info;
      delete s_h_BUS_employee_info;
      delete s_h_Ork_employment_info; 
      delete s_h_ORK_Company_area_info;
      delete s_h_ORK_dept_info;
      delete l_dept_area;
      delete l_emp_dept;
      delete rh_ork_absence_codes;
      delete s_rh_ork_absence_info;
      delete s_h_PNK_employee_info;
      delete s_h_GuG_employee_info; 
      delete s_h_PNK_dept_info; 
   run;	*/

/* -- Staging tables -- */

     proc sql;
      Create table dw.st_Depts_base
         (
            DeptHashKey char(16)
          , LinkHashKey char(16)
          , AreaHashKey char(16)
          , hd_dept_info char(16)
          , DW_Insert_DateTime num(8)
          , DW_LogRef num(20)
          , DW_RecordSource char(100)
          , SeqNo num(8)
          , AreaID char(10)
          , DeptID char(10)
          , DeptName char(100)
          , DeptAbb char(10)
         );
         
      proc sql;
       Create table dw.st_Depts_PNK
         (
            DeptHashKey char(16)
          , hd_dept_info char(16)
          , DW_Insert_DateTime num(8)
          , DW_LogRef num(20)
          , DW_RecordSource char(100)
          , SeqNo num(8)
          , Team char(10)
          , Name char(100)
          , OfficePlace char(10)
          , usable char(10)

         )
;

   proc sql;
      Create table dw.st_Areas_Base
         (
           AreaID char(10) 
         , Abbr char(16)
         , Name char(15)
         , AreaHashKey char(16)
         , HD_Area_info char(16)
         , DW_Insert_DateTime num(8)
         , DW_LogRef num(20)
         , DW_RecordSource char(100)
         , SeqNo num(8)
         );

   proc sql;
      Create Table dw.st_Employees_ORK
         (
           SourceID char(10)
         , CPR_nr char(10)
         , SeqNo num(8)
         , Emp_dept_HashKey char(16)
         , EmployeeHashKey char(16)
         , DeptHashKey char(16)
         , HD_employee_info char(16)
         , HD_employment_info char(16)
         , DW_Insert_DateTime num(8)
         , DW_LogRef num(20)
         , DW_Recordsource char(100)
         , UserID char(10)
         , DeptAbb char(6)
         , Name char(36)
         , P_Number Num(8)
         , DeptID char(10)
         , EmploymentDate Num(8)
         , TerminationDate Num(8)
         , EmploymentHoursNorm Num(8)
         );

   proc sql;
      Create Table dw.st_CaseWorkers_GuG
         (
           loginName char(255)
         , SeqNo num(8)
         , EmployeeHashKey char(16)
         , DeptHashKey char(16)
         , HD_employee_info char(16)
         , DW_Insert_DateTime num(8)
         , DW_LogRef num(20)
         , DW_RecordSource char(100)
         , Initials char(10)
         , DepartmentID char(16)
         , Name char(30)
         , Active num(8)
         , Retired num(8)
         );

   proc sql;
      Create Table dw.st_Saksbehandler_PNK
         (
           UserID char(20)
         , SeqNo num(8)
         , EmployeeHashKey char(16)
         , DW_Insert_DateTime num(8)
         , DW_LogRef num(20)
         , hd_employee_info char(16)
         , DeptHashKey char(16)
         , DW_RecordSource char(100)
         , bevillingshaver char(12)
         , ch_due_list char(1)
         , ch_own_cases char(1)
         , Date_Changed num(8)
         , Date_Created num(8)
         , e_post_domene char(30)
         , e_post_navn char(30)
         , kontorsted char(2)
         , list_active_inquiry char(1)
         , P_ASC char(8)
         , show_phone_no_deb char(1)
         , skriver char(14)
         , telefax char(14)
         , telefonnr char(14)
         , CIdent_User_ID char(20)
         , Team char(6)
         , Navn char(36)
         , short_Name char(16)
         , tittel char(20)
         , Usable char(1)
         , CH_Own_Cases char(1)
         , CH_Due_List char(1)
         , Saksbeh char(6)
         , List_Active_Inquiry char(1)
         );
/*
   proc sql;
      Create table dw.st_Roles_Base
      (
        SourceID char(4)
      , RoleHashKey char(16)
      , DW_Insert_DateTime num(8)
      , DW_LogRef num(20)
      , DW_RecordSource char(100)
      , Title_DK char(40)
      , Title_EN char(40)
      , SeqNo num(8)
      );
*/
/* -- staging tables end */

/* -- Raw Hub tables start -- */

   proc sql;
      Create Table dw.H_Employee
         (
           H_Key char(16)
         , DW_Bus_Key char(100)
         , DW_RecordSource char(100)
         , DW_Insert_DateTime num(8)
         , DW_LogRef num(20)
         , DW_LastSeenDate num(8)
         );

   proc sql;
      Create Table dw.H_Bus_Employee
         (
           H_Key char(16)
         , DW_Bus_Key char(100)
         , DW_RecordSource char(100)
         , DW_Insert_DateTime num(8)
         , DW_LogRef num(20)
         , DW_LastSeenDate num(8)
         );

   proc sql;
      Create Table dw.H_Company_Area
         (
           H_Key char(16)
         , DW_Bus_Key char(100)
         , DW_RecordSource char(100)
         , DW_Insert_DateTime num(8)
         , DW_LogRef num(20)
         );

   proc sql;
      Create Table dw.H_Department
         (
           H_Key char(16)
         , DW_Bus_Key char(100)
         , DW_RecordSource char(100)
         , DW_Insert_DateTime num(8)
         , DW_LogRef num(20)
         );

	proc sql;
		create table dw.H_customer
        	H_Key char(16)
         	, DW_Bus_Key char(100)
	        , DW_RecordSource char(100)
	        , DW_Insert_DateTime num(8)
			, DW_lastSeenDate num(8)
	        , DW_LogRef num(20)

/* --  RAW Hub tables slut -- */

/* -- RAW sat tables start -- */

   proc sql;
      Create table dw.s_h_Ork_Employee_info
      (
        H_Key char(16)
      , DW_Insert_DateTime num(8)
      , LoadEndDate num(8)
      , DW_RecordSource char(100)
      , DW_LogRef num(20)
      , h_diff char(16)
      , Name char(50)
      , UserID char(8)
      );
      
   proc sql;
      Create table dw.S_H_ORK_Employment_Info
      (
        H_Key char(16)
      , DW_Insert_DateTime num(8)
      , LoadEndDate num(8)
      , DW_RecordSource char(100)
      , DW_LogRef num(20)
      , h_diff char(16)
      , EmploymentHoursNorm num(8)
      , EmploymentDate num(8)
      , TerminationDate num(8)
      , P_Number num(8)
      , SourceID char(10)
      );

   proc sql;
      Create table dw.s_h_PNK_employee_info
      (
        H_Key char(16)
      , DW_Insert_DateTime num(8)
      , LoadEndDate num(8)
      , DW_RecordSource char(100)
      , DW_LogRef num(20)
      , h_diff char(16)
      , bevillingshaver char(12)
      , e_post_domene char(30)
      , e_post_navn char(30)
      , kontorsted char(2)
      , Navn char(36)
      , P_asc char(8)
      , saksbeh char(6)
      , short_name char(16)
      , show_phone_no_deb char(1)
      , skriver char(14)
      , telefax char(14)
      , telefonnr char(14)
      , tittel char(20)
      , cident_user_id char(20)
      , usable char(1)
      , ch_due_list char(1)
      , ch_own_cases char(1)
      , list_active_inquiry char(1)
      , date_changed num(8)
      , date_created num(8)
      );

   proc sql;
      Create table dw.s_h_GuG_employee_info
      (
        H_Key char(16)
      , DW_Insert_DateTime num(8)
      , LoadEndDate num(8)
      , DW_RecordSource char(100)
      , DW_LogRef num(20)
      , h_diff char(16)
      , active num(8)
      , retired num(8)
      , loginName char(255)
      );

   proc sql;
      Create table dw.s_h_ORK_Company_area_info
      (
        H_Key char(16)
      , DW_Insert_DateTime num(8)
      , LoadEndDate num(8)
      , DW_RecordSource char(100)
      , DW_LogRef num(20)
      , h_diff char(16)
      , Abbr char(16)
      , Name char(15)
      );

   proc sql;
      Create table dw.S_H_ORK_Dept_Info
      (
        H_Key char(16)
      , DW_Insert_DateTime num(8)
      , LoadEndDate num(8)
      , DW_RecordSource char(100)
      , DW_LogRef num(20)
      , h_diff char(16)
      , DeptName char(100)
      , DeptAbb char(10)
      );

   proc sql;
      Create table dw.S_H_PNK_Dept_Info
      (
        H_Key char(16)
      , DW_Insert_DateTime num(8)
      , LoadEndDate num(8)
      , DW_RecordSource char(100)
      , DW_LogRef num(20)
      , h_diff char(16)
      , call_center char(1)
      , ch_due_list char(1)
      , ch_own_cases char(1)
      , result_unit char(4)
      , team_type char(4)
      , Name char(100)
      , Office_Place char(2)
      , usable char(10)
      );


   proc sql;
      Create table dw.s_rh_ork_absence_info
      (
        H_Key char(16)
      , DW_Insert_DateTime num(8)
      , LoadEndDate num(8)
      , DW_RecordSource char(100)
      , DW_LogRef num(20)
      , h_diff char(16)
      , Name char(100)
      );

proc sql;
	create table dw.s_h_pink_cust_business_info
		(
		h_key char(16)
		DW_insert_DateTime num(8),
		DW_loadEndDate num(8),
		debitornr,
		CVR, 
		)
		;

proc sql;
	create table dw.s_h_GUG_cust_privat_info
		(
		h_key char(16)
		DW_insert_DateTime num(8),
		DW_loadEndDate num(8),
		debitornr,
		CPR, 
		)
		;

proc sql;
	create table dw.s_h_GUG_cust_business_info
		(
		h_key char(16)
		DW_insert_DateTime num(8),
		DW_loadEndDate num(8),
		debitornr,
		CVR, 
		)
		;


/* --  RAW Sat tables slut -- */

/* -- Businesss Sat Tables start -- */

   proc sql;
      Create table dw.s_h_BUS_Employee_info
      (
        H_Key char(16)
      , DW_Insert_DateTime num(8)
      , LoadEndDate num(8)
      , DW_RecordSource char(100)
      , DW_LogRef num(20)
      , Name char(50)
      , UserID char(8)
      , BornYear num (4)
      , BornMonth num (2)
      );

/* -- Business Sat Tables slut -- */

/* -- RAW Link table start -- */

   proc sql;
      Create table dw.l_dept_area
      (
        H_Key char(16)
      , DW_Insert_DateTime num(8)
      , DW_RecordSource char(100)
      , DW_LogRef num(20)
      , AreaHashkey char(16)
      , DeptHashkey char(16)
      );

   proc sql;
      Create table dw.l_emp_dept
      (
        H_Key char(16)
      , DW_Insert_DateTime num(8)
      , DW_RecordSource char(100)
      , DW_LogRef num(20)
      , EmployeeHashkey char(16)
      , DeptHashkey char(16)
      );

	proc sql;
		create table dw.l_a_emp_cust
		(
		EmplCustHashKey char(16),
      , DW_Insert_DateTime num(8)
      , DW_RecordSource char(100)
      , DW_LogRef num(20)
      , EmployeeHashkey char(16)
      , CustHashkey char(16)
		);

/* -- Raw Link tables end -- */

/* -- Reference hub tables start -- */

   proc sql;
      create table dw.rh_ork_absence_codes
      (
           H_Key char(16)
         , DW_Bus_Key char(100)
         , DW_RecordSource char(100)
         , DW_Insert_DateTime num(8)
         , DW_LogRef num(20)
      );

/* -- Reference tables slut -- */

/* %let InputPath = \\groupad1.com\Home\Denmark-Horsens\nooyehe\Data\report_files\input_files\; */
/* %let ProgramPath = \\groupad1.com\Home\Denmark-Horsens\nooyehe\Data\sas_programs\; */

/* %INCLUDE "&ProgramPath.DW_Restore_content_1_0.sas" /lrecl=10000; */
