
proc datasets library=dwh_dk;
   delete H_Employee;
   delete S_H_ORK_Employee_Info;
   delete S_H_ORK_Employment_Info;
   delete H_Department;
   delete S_H_PNK_Dept_Info;
   delete S_H_ORK_Dept_Info;
   delete L_Emp_Dept;

run;

proc sql; create table dwh_dk.H_Employee as
   select aa.* from nooyehe.h_employee as aa
;

proc sql; create table dwh_dk.S_H_ORK_Employee_Info as
   select aa.* from nooyehe.S_H_Ork_Employee_Info as aa
;

proc sql; create table dwh_dk.S_H_ORK_Employment_Info as
   select aa.* from nooyehe.S_H_ORK_Employment_Info as aa
;

proc sql; create table dwh_dk.H_Department as
   select aa.* from nooyehe.H_Department as aa
;

proc sql; create table dwh_dk.S_H_ORK_Dept_Info as
   select aa.* from nooyehe.S_H_ORK_Dept_Info as aa
;

proc sql; create table dwh_dk.S_H_PNK_Dept_Info as
   select aa.* from nooyehe.S_H_PNK_Dept_Info as aa
;

proc sql; create table dwh_dk.L_Emp_Dept as
   select aa.* from nooyehe.L_Emp_Dept as aa
;

