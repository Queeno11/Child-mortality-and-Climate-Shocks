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

// use "$DATA_OUT/DHSBirthsGlobal&ClimateShocks.dta", clear
//
// global childvars = "child_agedeath_30d child_agedeath_30d3m child_agedeath_3m6m child_agedeath_6m12m"
// global precvars = "prec_m_q1 prec_m_q2 prec_q3 prec_30d prec_30d3m prec_3m6m prec_6m12m prec_neg_q1 prec_neg_q2 prec_neg_q3 prec_neg_30d prec_neg_30d3m prec_neg_3m6m prec_neg_6m12m prec_pos_q1 prec_pos_q2 prec_pos_q3 prec_pos_30d prec_pos_30d3m prec_pos_3m6m prec_pos_6m12m prec_m_q1 prec_m_q2 prec_m_q3 prec_m_30d prec_m_30d3m prec_m_3m6m prec_m_6m12m prec_m_neg_q1 prec_m_neg_q2 prec_m_neg_q3 prec_m_neg_30d prec_m_neg_30d3m prec_m_neg_3m6m prec_m_neg_6m12m prec_m_pos_q1 prec_m_pos_q2 prec_m_pos_q3 prec_m_pos_30d prec_m_pos_30d3m prec_m_pos_3m6m prec_m_pos_6m12m"
// global droughtvars = "drought_m_2_5_q1 drought_m_2_5_q2 drought_m_2_5_q3 drought_m_2_5_30d drought_m_2_5_30d3m drought_m_2_5_3m6m drought_m_2_5_6m12m drought_2_5_q1 drought_2_5_q2 drought_2_5_q3 drought_2_5_30d drought_2_5_30d3m drought_2_5_3m6m drought_2_5_6m12m"
// global excessiverainvars = "excessiverain_m_2_5_q1 excessiverain_m_2_5_q2 excessiverain_m_2_5_q3 excessiverain_m_2_5_30d excessiverain_m_2_5_30d3m excessiverain_m_2_5_3m6m excessiverain_m_2_5_6m12m excessiverain_2_5_q1 excessiverain_2_5_q2 excessiverain_2_5_q3 excessiverain_2_5_30d excessiverain_2_5_30d3m excessiverain_2_5_3m6m excessiverain_2_5_6m12m"
// global eqvars = "$childvars $precvars $droughtvars $excessiverainvars"
//
// preserve
// keep $eqvars
// outreg2 using "$OUTPUTS/descriptive_table.xls", replace sum(detail) keep(${eqvars})  eqkeep(N mean sd min max p10 p50 p90) label
// restore 
//
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

use "$DATA_OUT/DHSBirthsGlobal&ClimateShocks.dta", clear

keep if since_2003==1 & last_10_years==1

encode v000, gen(IDsurvey_country)

replace child_agedeath_30d = child_agedeath_30d * 1000
replace child_agedeath_30d3m = child_agedeath_30d3m * 1000
replace child_agedeath_3m6m = child_agedeath_3m6m * 1000
replace child_agedeath_6m12m = child_agedeath_6m12m * 1000

gen time = chb_year - 1989
gen time_sq = time*time


*############################################################*
*### 	 				 UNCONTROLLED
*############################################################*

foreach var in "spi1" "spi3" "spi6" "spi12" "temp" {
	preserve
	
	egen prec_prev_q = xtile(`var'_30d), n(1000)
	bysort prec_prev_q: egen mean_child_agedeath_30d = mean(child_agedeath_30d)
	collapse (first) mean_child_agedeath_30d (median) `var'_30d, by(prec_prev_q)

	lowess mean_child_agedeath_30d `var'_30d  
	graph export "`var'_mortality_uncontrolled.png", replace

	restore
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
*### 	 				 REGRESSIONS
*############################################################*

global precipitation = "spi12"
global temp = "temp"
global controls = "child_fem child_mulbirth birth_order mother_ageb* mother_eduy* rural"

*** MODEL 1 - Precipitaion
global prec_previous= "${precipitation}_q1 ${precipitation}_q2 ${precipitation}_q3"
egen prec_prev_max = rowmax(${precipitation}_q1 ${precipitation}_q2 ${precipitation}_q3 ${precipitation}_30d)

** FWL theorem
*child_agedeath_30d on controls
reghdfe child_agedeath_30d $controls, absorb(c.time#i.ID_cell i.chb_month#i.ID_cell) vce(cluster i.ID_cell) residuals(child_agedeath_30d_res)

* SPI on controls 
reghdfe prec_prev_max $controls, absorb(c.time#i.ID_cell i.chb_month#i.ID_cell) vce(cluster i.ID_cell) residuals(prec_prev_res)

keep prec_prev_res child_agedeath_30d_res
label var prec_prev_res "SPI Residuals"
label var child_agedeath_30d_res "child_agedeath_30d Residuals"

save "residuals_FWL_spi.dta", replace
use "residuals_FWL_spi.dta", replace

* lowess

egen prec_prev_res_d = xtile(prec_prev_res), n(20)
bysort prec_prev_res_d: egen mean_child_agedeath_30d_res = mean(child_agedeath_30d_res)

collapse (first) mean_child_agedeath_30d_res (mean) prec_prev_res, by(prec_prev_res_d)

lowess mean_child_agedeath_30d_res prec_prev_res  
graph export "spi_mean_by_20iles.png", replace
restore



*** MODEL 2 - Temperature
use "$DATA_OUT/DHSBirthsGlobal&ClimateShocks.dta", clear

encode v000, gen(IDsurvey_country)

gen time = chb_year - 1989
gen time_sq = time*time

global precipitation = "spi12"
global temp = "temp"
global controls = "child_fem child_mulbirth birth_order mother_ageb* mother_eduy* rural"

global temp_previous= "${temp}_q1 ${temp}_q2 ${temp}_q3"
egen temp_prev_max = rowmax(${temp}_q1 ${temp}_q2 ${temp}_q3 ${temp}_30d)

** FWL theorem
*child_agedeath_30d on controls
reghdfe child_agedeath_30d $controls, absorb(c.time#i.ID_cell i.chb_month#i.ID_cell) vce(cluster i.ID_cell) residuals(child_agedeath_30d_res)

* SPI on controls 
reghdfe temp_prev_max $controls, absorb(c.time#i.ID_cell i.chb_month#i.ID_cell) vce(cluster i.ID_cell) residuals(temp_prev_res)

keep temp_prev_res child_agedeath_30d_res
label var temp_prev_res "Temperature Residuals"
label var child_agedeath_30d_res "child_agedeath_30d Residuals"

save "residuals_FWL_temp.dta", replace
use "residuals_FWL_temp.dta", replace

* lowess

egen temp_prev_res_d = xtile(temp_prev_res), n(20)
bysort temp_prev_res_d: egen mean_child_agedeath_30d_res = mean(child_agedeath_30d_res)

collapse (first) mean_child_agedeath_30d_res (mean) temp_prev_res, by(temp_prev_res_d)

lowess mean_child_agedeath_30d_res temp_prev_res  
graph export "temp_mean_by_20iles.png", replace
restore



















