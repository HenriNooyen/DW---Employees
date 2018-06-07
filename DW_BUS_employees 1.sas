
/* der mangler et comment her, test  */

proc sql;
   create table work.Bus_employee_Base;
   select
        aa.H_Key
      , md5(catx(';',DW_Bus_Key,'SYS')) as BusEmployeeHashKey format $hex32. length=16
      , 'SYS' as DW_RecordSource
      , aa.DW_Insert_DateTime
      , aa.DW_LogRef
      , aa.DW_LastSeenDate
   from nooyehe.h_employee as aa;
run;

proc sql;
   create table  test as
      select
        aa.H_Key
      , aa.DW_Insert_DateTime
      , aa.LoadEndDate
      , 'SYS' as DW_RecordSource
      , aa.DW_LogRef
      , aa.Name
      , aa.UserID
      , length(trim(bb.dw_bus_key)) as test
      , case length(bb.dw_bus_key)
         when length(trim(bb.dw_bus_key)) = 10 then
            substr(trim(bb.dw_bus_key),4,2)
         else
            substr(trim(bb.dw_bus_key),4,2)
        end as BornYear
      , bb.dw_bus_key as BornMonth
 from nooyehe.s_h_ork_employee_info as aa left join nooyehe.h_employee as bb
   on aa.h_key = bb.h_key;