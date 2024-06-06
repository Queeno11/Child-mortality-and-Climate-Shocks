global PROJECT = "Z:\Laboral\World Bank\Paper - Child mortality and Climate Shocks"
global OUTPUTS = "${PROJECT}\Outputs"
global DATA = "${PROJECT}\Data"
global DATA_IN = "${DATA}\Data_in"
global DATA_PROC = "${DATA}\Data_proc"
global DATA_OUT = "${DATA}\Data_out"

clear all
set maxvar 120000

*############################################################*
*### 	 CONFIGURATION
*############################################################*


*############################################################*

*##################*
*#   DESCRIPTIVE STATISTICS
*##################*

use "$DATA_OUT/DHSBirthsGlobal&ClimateShocks.dta", clear

global childvars = "child_agedeath_30d child_agedeath_30d3m child_agedeath_3m6m child_agedeath_6m12m"
global precvars = "spi1_q1 spi1_q2 spi1_q3 spi1_30d spi1_30d3m spi1_3m6m spi1_6m12m"
global tempvars = "temp_q1 temp_q2 temp_q3 temp_30d temp_30d3m temp_3m6m temp_6m12m"
global droughtvars = "drought1_1_5_q1 drought1_1_5_q2 drought1_1_5_q3 drought1_1_5_30d drought1_1_5_30d3m drought1_1_5_3m6m drought1_1_5_6m12m"
global excessiverainvars = "excessiverain1_1_5_q1 excessiverain1_1_5_q2 excessiverain1_1_5_q3 excessiverain1_1_5_30d excessiverain1_1_5_30d3m excessiverain1_1_5_3m6m excessiverain1_1_5_6m12m"
global eqvars = "$childvars $precvars $tempvars $droughtvars $excessiverainvars"

preserve
keep $eqvars
outreg2 using "$OUTPUTS/descriptive_table.xls", replace sum(detail) keep(${eqvars})  eqkeep(N mean sd min p10 p50 p90 max) label
restore 
stop
// preserve
// collapse (count) ID, by(chb_month ID_cell)
// h reshape
// reshape wide ID, i(ID_cell) j(chb_month)
// rename ID* month*
// rename month_cell ID_cell
// export excel using "$OUTPUTS/obs_by_cell_and_month.xls", firstrow(variables) replace
// restore
//
// preserve
// collapse (count) ID, by(chb_month ID_cell_2)
// h reshape
// reshape wide ID, i(ID_cell_2) j(chb_month)
// rename ID* month*
// rename month_cell ID_cell_2
// export excel using "$OUTPUTS/obs_by_cell_2_and_month.xls", firstrow(variables) replace
// restore
//
// preserve
// collapse (count) ID, by(chb_month ID_cell_3)
// h reshape
// reshape wide ID, i(ID_cell_3) j(chb_month)
// rename ID* month*
// rename month_cell ID_cell_3
// export excel using "$OUTPUTS/obs_by_cell_3_and_month.xls", firstrow(variables) replace
// restore
//
// preserve
// collapse (count) ID, by(chb_month ID_cell_4)
// h reshape
// reshape wide ID, i(ID_cell_4) j(chb_month)
// rename ID* month*
// rename month_cell ID_cell_4
// export excel using "$OUTPUTS/obs_by_cell_4_and_month.xls", firstrow(variables) replace
// restore


*############################################################*
*### 	 				 UNCONTROLLED
*############################################################*

foreach var in "temp" "spi1" "spi3" "spi6" "spi12" {
	preserve
	
	egen prec_prev_q = xtile(`var'_30d), n(1000)
	bysort prec_prev_q: egen mean_child_agedeath_30d = mean(child_agedeath_30d)
	collapse (first) mean_child_agedeath_30d (median) `var'_30d, by(prec_prev_q)

	lowess mean_child_agedeath_30d `var'_30d  
	graph export "`var'_mortality_uncontrolled.png", replace

	restore
	stop
}

preserve

egen temp_prev_q = xtile(temp_30d), n(1000)
bysort temp_prev_q: egen mean_child_agedeath_30d = mean(child_agedeath_30d)
collapse (first) mean_child_agedeath_30d (median) ${precipitation}_30d, by(temp_prev_q)

lowess mean_child_agedeath_30d temp_30d  
graph export "temp_mortality_uncontrolled.png", replace

restore
stop

*############################################################*
*### 	 				 CONTROLLED
*############################################################*
** FWL theorem. Takes A LOT of time...

global controls = "child_fem child_mulbirth birth_order mother_ageb* mother_eduy* rural d_weatlh_ind_*"

reghdfe child_agedeath_30d $controls, absorb(c.time#i.ID_cell i.chb_month#i.ID_cell) vce(cluster i.ID_cell) residuals(child_agedeath_30d_res)

save "$DATA_PROC/residuals_FWL.dta", replace

foreach var in "temp" "spi3" "spi6" "spi12" { // "spi1"  {

	preserve
	* SPI on controls 
	reghdfe `var'_30d $controls, absorb(c.time#i.ID_cell i.chb_month#i.ID_cell) vce(cluster i.ID_cell) residuals(`var'_30d_res)

	keep `var'_30d_res child_agedeath_30d_res
	label var `var'_30d_res "Climate Residuals"
	label var child_agedeath_30d_res "Child Mortality 30d Residuals"

	save "residuals_`var'_30d.dta", replace
	
	egen climate_q = xtile(`var'_30d_res), n(1000)
	bysort climate_q: egen mean_child_agedeath_30d_res = mean(child_agedeath_30d_res)
	collapse (first) mean_child_agedeath_30d (median) `var'_30d, by(climate_q)

	lowess mean_child_agedeath_30d `var'_30d  
	graph export "`var'_30d_mortality_controlled.png", replace

	restore
}


















