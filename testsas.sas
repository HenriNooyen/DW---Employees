/* dette er foeste aendring i testsas.sas */
data cars;
	set sashelp.cars(keep origin);
	where origin ="Asia" | origin="Europe";

run;


