/* dette er foeste aendring i testsas.sas */
data cars;
	set sashelp.cars;
	where origin ="Asia" | origin="Europe";

run;


