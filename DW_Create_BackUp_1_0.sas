   proc datasets library=nooyehe;
      delete bu_h_Employee;
      delete bu_h_bus_employee;
      delete bu_h_Employment_time_types;
      delete bu_h_Employment;
      delete bu_h_company_area;
      delete bu_h_Department;
      delete bu_rh_ork_absence_codes;
      delete bu_l_dept_area;
      delete bu_l_emp_dept;
      delete bu_s_h_ORK_employee_info;
      delete bu_s_h_BUS_employee_info;
      delete bu_s_h_ORK_employment_info;
      delete bu_s_h_ORK_Company_area_info;
      delete bu_s_h_ORK_dept_info;
      delete bu_s_h_PNK_employee_info;
      delete bu_s_h_GuG_employee_info;
      delete bu_s_rh_ork_absence_info;
      delete bu_S_H_PNK_Dept_Info;
   run;

proc sql;
   Create Table nooyehe.BU_H_Employee
      (
        H_Key char(16)
      , DW_Bus_Key char(100)
      , DW_RecordSource char(100)
      , DW_Insert_DateTime num(8)
      , DW_LogRef num(20)
      , DW_LastSeenDate num(8)
      );

proc sql;
   Create Table nooyehe.BU_H_Bus_Employee
      (
        H_Key char(16)
      , DW_Bus_Key char(100)
      , DW_RecordSource char(100)
      , DW_Insert_DateTime num(8)
      , DW_LogRef num(20)
      , DW_LastSeenDate num(8)
      );

proc append base=nooyehe.BU_h_employee data=nooyehe.h_employee;
run;

proc append base=nooyehe.BU_h_Bus_employee data=nooyehe.h_Bus_employee;
run;


proc sql;
   create table nooyehe.bu_h_employment
      (
        H_Key char(16)
      , DW_Bus_Key char(100)
      , DW_RecordSource char(100)
      , DW_Insert_DateTime num(8)
      , DW_LogRef num(20)
      );

proc append base=nooyehe.BU_h_employment data=nooyehe.h_employment;
run;

proc sql;
   create table nooyehe.bu_h_employment_time_types
      (
        H_Key char(16)
      , DW_Bus_Key char(100)
      , DW_RecordSource char(100)
      , DW_Insert_DateTime num(8)
      , DW_LogRef num(20)
      );

proc append base=nooyehe.BU_h_employment_time_types data=nooyehe.h_employment_time_types;
run;

proc sql;
   Create Table nooyehe.bu_H_Company_Area
      (
        H_Key char(16)
      , DW_Bus_Key char(100)
      , DW_RecordSource char(100)
      , DW_Insert_DateTime num(8)
      , DW_LogRef num(20)
      );

proc append base=nooyehe.BU_h_company_area data=nooyehe.h_company_area;
run;

proc sql;
   Create Table nooyehe.BU_H_Department
      (
        H_Key char(16)
      , DW_Bus_Key char(100)
      , DW_RecordSource char(100)
      , DW_Insert_DateTime num(8)
      , DW_LogRef num(20)
      );

proc append base=nooyehe.BU_h_department data=nooyehe.h_department;
run;

proc sql;
   create table nooyehe.bu_rh_ork_absence_codes
      (
        H_Key char(16)
      , DW_Bus_Key char(100)
      , DW_RecordSource char(100)
      , DW_Insert_DateTime num(8)
      , DW_LogRef num(20)
      );

proc append base=nooyehe.BU_rh_ork_absence_codes data=nooyehe.rh_ork_absence_codes;
run;

proc sql;
   Create table nooyehe.bu_l_dept_area
   (
     H_Key char(16)
   , DW_Insert_DateTime num(8)
   , DW_RecordSource char(100)
   , DW_LogRef num(20)
   , AreaHashkey char(16)
   , DeptHashkey char(16)
   );

proc append base=nooyehe.BU_l_dept_area data=nooyehe.l_dept_area;
run;

proc sql;
   Create table nooyehe.BU_l_emp_dept
   (
     H_Key char(16)
   , DW_Insert_DateTime num(8)
   , DW_RecordSource char(100)
   , DW_LogRef num(20)
   , EmployeeHashkey char(16)
   , DeptHashkey char(16)
   );

proc append base=nooyehe.BU_l_emp_dept data=nooyehe.l_emp_dept;
run;

proc sql;
   Create table nooyehe.bu_s_h_Ork_Employee_info
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

proc append base=nooyehe.BU_s_h_ork_employee_info data=nooyehe.s_h_ork_employee_info;
run;

proc sql;
   Create table nooyehe.s_h_BUS_Employee_info
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

proc append base=nooyehe.BU_s_h_BUS_employee_info data=nooyehe.s_h_BUS_employee_info;
run;
      
proc sql;
   Create table nooyehe.bu_s_h_Ork_employment_info
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


proc append base=nooyehe.BU_s_h_Ork_employment_info data=nooyehe.s_h_Ork_employment_info;
run;

proc sql;
   Create table nooyehe.bu_s_h_PNK_employee_info
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

proc append base=nooyehe.BU_s_h_PNK_employee_info data=nooyehe.s_h_PNK_employee_info;
run;

proc sql;
   Create table nooyehe.bu_s_h_GuG_employee_info
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

proc append base=nooyehe.BU_s_h_GuG_employee_info data=nooyehe.s_h_GuG_employee_info;
run;

proc sql;
   Create table nooyehe.bu_s_h_ORK_Company_area_info
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

proc append base=nooyehe.BU_s_h_ORK_Company_area_info data=nooyehe.s_h_ORK_Company_area_info;
run;

proc sql;
   Create table nooyehe.bu_s_h_ORK_dept_info
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

proc append base=nooyehe.BU_s_h_ORK_dept_info data=nooyehe.s_h_ORK_dept_info;
run;

proc sql;
   Create table nooyehe.bu_s_rh_ork_absence_info
   (
     H_Key char(16)
   , DW_Insert_DateTime num(8)
   , LoadEndDate num(8)
   , DW_RecordSource char(100)
   , DW_LogRef num(20)
   , h_diff char(16)
   , Name char(100)
   );

proc append base=nooyehe.BU_s_rh_ork_absence_info data=nooyehe.s_rh_ork_absence_info;
run;

   proc sql;
      Create table nooyehe.BU_S_H_PNK_Dept_Info
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

proc append base=nooyehe.BU_S_H_PNK_Dept_Info data=nooyehe.S_H_PNK_Dept_Info;
run;
