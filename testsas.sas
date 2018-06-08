
/* endnu et comment*/
/* dette er foeste aendring i testsas.sas */
data cars;
	set sashelp.cars(keep origin type);
	where origin ="Asia" | origin="Europe";

run;


