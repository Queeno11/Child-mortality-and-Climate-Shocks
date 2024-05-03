global PROJECT = "Z:\Laboral\World Bank\Paper - Child mortality and Climate Shocks"
global OUTPUTS = "${PROJECT}\Outputs"
global DATA = "${PROJECT}\Data"
global DATA_IN = "${DATA}\Data_in"
global DATA_PROC = "${DATA}\Data_proc"
global DATA_OUT = "${DATA}\Data_out"



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


*############################################################*
*### 	 				 REGRESSIONS
*############################################################*


   
**# Models with thresholds

foreach threshold in "3_5" "4_0" {

use "$DATA_OUT/DHSBirthsGlobal&ClimateShocks.dta", clear
global historic_means = "_m" // "`historic_means'"
global threshold = "`threshold'"

* DONT CHANGE THIS
global drought = "drought${historic_means}_${threshold}"
global excessiverain = "excessiverain${historic_means}_${threshold}"
global controls = "child_fem child_mulbirth birth_order mother_ageb* mother_eduy* rural"

* We will run the "same regression" for all the models, only keeping survivors for each age, and controlling for previous shocks.
* See that the general command applied to for all the cases is the same:
* 	reghdfe child_agedeath_{p}  ${drought_previous} ${drought}_{age} ${excessiverain_previous} ${excessiverain}_{p} $controls, absorb(i.chb_month#i.ID_country) vce(cluster i.ID_country)
* And the drought_previous and excessiverain_previous are the sum of all previous shocks, i.e. \sum_{rho=-3}^P \beta_p D_p + \gamma_p R_p

*** JED COMMENTS
* 1. Replacing the cell FE (delta sub-j) with a cell indicator interacted with dummies for the 12 calendar months. --DONE, lets see if that works
* 2. For the x-vector, let's begin by parsimony and consider: child gender, multiple birth indicator, birth order, age of mother (a cubic to be flexible), years of maternal education (cubic). We can also consider an urban/rural indicator although depending on size of geo-cell this indicator may lead to dropping exclusively urban or rural cells. --DONE!
* 3. For sub-group analysis, again we should focus on only a few dimensions: boy/girl, urban/rural, possibly climactic zone?
* 4. As mentioned, it may be helpful to divide the first quarter of life into 2 periods: the first month (for neo-natal mortality), and the next two months. --DONE!
* 5. If a child dies at period q, we don't want the weather after death to influence mortality, so we may wish to estimate a phased/stepped set of regressions: 1. Survival to first month of life 2. Survival to first quarter of life (conditional on having survived to first month). 3. Survival to second Q (conditional on having survived to first Q). Etc. --DONE!


*** MODEL 1 - all childs
global drought_previous= "${drought}_q1 ${drought}_q2 ${drought}_q3"
global excessiverain_previous = "${excessiverain}_q1 ${excessiverain}_q2 ${excessiverain}_q3"

reghdfe child_agedeath_30d  ${drought_previous} ${drought}_30d ${excessiverain_previous} ${excessiverain}_30d $controls, absorb(i.chb_year#i.ID_cell i.chb_month#i.ID_cell) vce(cluster i.ID_cell)
outreg2 using "$OUTPUTS/regression_outs_timefe_${historic_means}_${threshold}", tex excel label append addtext(Cell#Year FE, Yes, Cell#Month FE, Yes, Cell size, 0.5deg)  nonotes addnote(SE clustered by Cell, *p<.05; **p<.01; ***p<.001)

reghdfe child_agedeath_30d  ${drought_previous} ${drought}_30d ${excessiverain_previous} ${excessiverain}_30d $controls, absorb(i.chb_year#i.ID_cell_2 i.chb_month#i.ID_cell_2) vce(cluster i.ID_cell_2)
outreg2 using "$OUTPUTS/regression_outs_timefe_${historic_means}_${threshold}", tex excel label append addtext(Cell#Year FE, Yes, Cell#Month FE, Yes, Cell size, 1deg)  nonotes addnote(SE clustered by Cell, *p<.05; **p<.01; ***p<.001)

reghdfe child_agedeath_30d  ${drought_previous} ${drought}_30d ${excessiverain_previous} ${excessiverain}_30d $controls, absorb(i.chb_year#i.ID_cell_3 i.chb_month#i.ID_cell_3) vce(cluster i.ID_cell_3)
outreg2 using "$OUTPUTS/regression_outs_timefe_${historic_means}_${threshold}", tex excel label append addtext(Cell#Year FE, Yes, Cell#Month FE, Yes, Cell size, 2deg)  nonotes addnote(SE clustered by Cell, *p<.05; **p<.01; ***p<.001)

reghdfe child_agedeath_30d  ${drought_previous} ${drought}_30d ${excessiverain_previous} ${excessiverain}_30d $controls, absorb(i.chb_year#i.ID_cell_4 i.chb_month#i.ID_cell_4) vce(cluster i.ID_cell_4)
outreg2 using "$OUTPUTS/regression_outs_timefe_${historic_means}_${threshold}", tex excel label append addtext(Cell#Year FE, Yes, Cell#Month FE, Yes, Cell size, 4deg)  nonotes addnote(SE clustered by Cell, *p<.05; **p<.01; ***p<.001)


*** MODEL 2 - all childs that survived 30days
drop if child_agedeath_30d==1
global drought_previous = "${drought_previous} ${drought}_30d"
global excessiverain_previous = "${excessiverain_previous} ${excessiverain}_30d"

reghdfe child_agedeath_30d3m ${drought_previous} ${drought}_30d3m ${excessiverain_previous} ${excessiverain}_30d3m $controls, absorb(i.chb_year#i.ID_cell i.chb_month#i.ID_cell) vce(cluster i.ID_cell)
outreg2 using "$OUTPUTS/regression_outs_timefe_${historic_means}_${threshold}", tex excel label append addtext(Cell#Year FE, Yes, Cell#Month FE, Yes, Cell size, 0.5deg)  nonotes addnote(SE clustered by Cell, *p<.05; **p<.01; ***p<.001)

reghdfe child_agedeath_30d3m ${drought_previous} ${drought}_30d3m ${excessiverain_previous} ${excessiverain}_30d3m $controls, absorb(i.chb_year#i.ID_cell_2 i.chb_month#i.ID_cell_2) vce(cluster i.ID_cell_2)
outreg2 using "$OUTPUTS/regression_outs_timefe_${historic_means}_${threshold}", tex excel label append addtext(Cell#Year FE, Yes, Cell#Month FE, Yes, Cell size, 1deg)  nonotes addnote(SE clustered by Cell, *p<.05; **p<.01; ***p<.001)

reghdfe child_agedeath_30d3m ${drought_previous} ${drought}_30d3m ${excessiverain_previous} ${excessiverain}_30d3m $controls, absorb(i.chb_year#i.ID_cell_3 i.chb_month#i.ID_cell_3) vce(cluster i.ID_cell_3)
outreg2 using "$OUTPUTS/regression_outs_timefe_${historic_means}_${threshold}", tex excel label append addtext(Cell#Year FE, Yes, Cell#Month FE, Yes, Cell size, 2deg)  nonotes addnote(SE clustered by Cell, *p<.05; **p<.01; ***p<.001)

reghdfe child_agedeath_30d3m ${drought_previous} ${drought}_30d3m ${excessiverain_previous} ${excessiverain}_30d3m $controls, absorb(i.chb_year#i.ID_cell_4 i.chb_month#i.ID_cell_4) vce(cluster i.ID_cell_4)
outreg2 using "$OUTPUTS/regression_outs_timefe_${historic_means}_${threshold}", tex excel label append addtext(Cell#Year FE, Yes, Cell#Month FE, Yes, Cell size, 4deg)  nonotes addnote(SE clustered by Cell, *p<.05; **p<.01; ***p<.001)


*** MODEL 3 - all childs that survived 3months
drop if child_agedeath_30d3m==1
global drought_previous = "${drought_previous} ${drought}_30d3m"
global excessiverain_previous = "${excessiverain_previous} ${excessiverain}_30d3m"

reghdfe child_agedeath_3m6m ${drought_previous} ${drought}_3m6m ${excessiverain_previous} ${excessiverain}_3m6m $controls, absorb(i.chb_year#i.ID_cell i.chb_month#i.ID_cell) vce(cluster i.ID_cell)
outreg2 using "$OUTPUTS/regression_outs_timefe_${historic_means}_${threshold}", tex excel label append addtext(Cell#Year FE, Yes, Cell#Month FE, Yes, Cell size, 0.5deg)  nonotes addnote(SE clustered by Cell, *p<.05; **p<.01; ***p<.001)

reghdfe child_agedeath_3m6m ${drought_previous} ${drought}_3m6m ${excessiverain_previous} ${excessiverain}_3m6m $controls, absorb(i.chb_year#i.ID_cell_2 i.chb_month#i.ID_cell_2) vce(cluster i.ID_cell_2)
outreg2 using "$OUTPUTS/regression_outs_timefe_${historic_means}_${threshold}", tex excel label append addtext(Cell#Year FE, Yes, Cell#Month FE, Yes, Cell size, 1deg)  nonotes addnote(SE clustered by Cell, *p<.05; **p<.01; ***p<.001)

reghdfe child_agedeath_3m6m ${drought_previous} ${drought}_3m6m ${excessiverain_previous} ${excessiverain}_3m6m $controls, absorb(i.chb_year#i.ID_cell_3 i.chb_month#i.ID_cell_3) vce(cluster i.ID_cell_3)
outreg2 using "$OUTPUTS/regression_outs_timefe_${historic_means}_${threshold}", tex excel label append addtext(Cell#Year FE, Yes, Cell#Month FE, Yes, Cell size, 2deg)  nonotes addnote(SE clustered by Cell, *p<.05; **p<.01; ***p<.001)

reghdfe child_agedeath_3m6m ${drought_previous} ${drought}_3m6m ${excessiverain_previous} ${excessiverain}_3m6m $controls, absorb(i.chb_year#i.ID_cell_4 i.chb_month#i.ID_cell_4) vce(cluster i.ID_cell_4)
outreg2 using "$OUTPUTS/regression_outs_timefe_${historic_means}_${threshold}", tex excel label append addtext(Cell#Year FE, Yes, Cell#Month FE, Yes, Cell size, 4deg)  nonotes addnote(SE clustered by Cell, *p<.05; **p<.01; ***p<.001)


*** MODEL 4 - all childs that survived 6months
drop if child_agedeath_3m6m==1
global drought_previous = "${drought_previous} ${drought}_3m6m"
global excessiverain_previous = "${excessiverain_previous} ${excessiverain}_3m6m"

reghdfe child_agedeath_6m12m ${drought_previous} ${drought}_6m12m ${excessiverain_previous} ${excessiverain}_6m12m $controls, absorb(i.chb_year#i.ID_cell i.chb_month#i.ID_cell) vce(cluster i.ID_cell)
outreg2 using "$OUTPUTS/regression_outs_timefe_${historic_means}_${threshold}", tex excel label append addtext(Cell#Year FE, Yes, Cell#Month FE, Yes, Cell size, 0.5deg)  nonotes addnote(SE clustered by Cell, *p<.05; **p<.01; ***p<.001)

reghdfe child_agedeath_6m12m ${drought_previous} ${drought}_6m12m ${excessiverain_previous} ${excessiverain}_6m12m $controls, absorb(i.chb_year#i.ID_cell_2 i.chb_month#i.ID_cell_2) vce(cluster i.ID_cell_2)
outreg2 using "$OUTPUTS/regression_outs_timefe_${historic_means}_${threshold}", tex excel label append addtext(Cell#Year FE, Yes, Cell#Month FE, Yes, Cell size, 1deg)  nonotes addnote(SE clustered by Cell, *p<.05; **p<.01; ***p<.001)

reghdfe child_agedeath_6m12m ${drought_previous} ${drought}_6m12m ${excessiverain_previous} ${excessiverain}_6m12m $controls, absorb(i.chb_year#i.ID_cell_3 i.chb_month#i.ID_cell_3) vce(cluster i.ID_cell_3)
outreg2 using "$OUTPUTS/regression_outs_timefe_${historic_means}_${threshold}", tex excel label append addtext(Cell#Year FE, Yes, Cell#Month FE, Yes, Cell size, 2deg)  nonotes addnote(SE clustered by Cell, *p<.05; **p<.01; ***p<.001)

reghdfe child_agedeath_6m12m ${drought_previous} ${drought}_6m12m ${excessiverain_previous} ${excessiverain}_6m12m $controls, absorb(i.chb_year#i.ID_cell_4 i.chb_month#i.ID_cell_4) vce(cluster i.ID_cell_4)
outreg2 using "$OUTPUTS/regression_outs_timefe_${historic_means}_${threshold}", tex excel label append addtext(Cell#Year FE, Yes, Cell#Month FE, Yes, Cell size, 4deg)  nonotes addnote(SE clustered by Cell, *p<.05; **p<.01; ***p<.001)

}

stop

**# Bookmark #1
**************** MODELS WITH PRECIPITATION
use "$DATA_OUT/DHSBirthsGlobal&ClimateShocks.dta", clear

global precipitation = "prec${historic_means}"
global controls = "child_fem child_mulbirth birth_order mother_ageb* mother_eduy* rural"

*** MODEL 1 - all childs
global prec_previous= "${precipitation}_q1 ${precipitation}_q2 ${precipitation}_q3"

reghdfe child_agedeath_30d  ${prec_previous} ${precipitation}_30d $controls, absorb(i.chb_year#i.ID_cell i.chb_month#i.ID_cell) vce(cluster i.ID_cell)
outreg2 using "$OUTPUTS/regression_outs_timefe_prec${historic_means}", tex excel label replace addtext(Cell#Year FE, Yes, Cell#Month FE, Yes, Cell size, 0.5deg)  nonotes addnote(SE clustered by Cell, *p<.05; **p<.01; ***p<.001)

// reghdfe child_agedeath_30d  ${prec_previous} ${precipitation}_30d $controls, absorb(i.chb_year#i.ID_cell_2 i.chb_month#i.ID_cell_2) vce(cluster i.ID_cell_2)
// outreg2 using "$OUTPUTS/regression_outs_timefe_prec${historic_means}", tex excel label append addtext(Cell#Year FE, Yes, Cell#Month FE, Yes, Cell size, 1deg)  nonotes addnote(SE clustered by Cell, *p<.05; **p<.01; ***p<.001)
//
// reghdfe child_agedeath_30d  ${prec_previous} ${precipitation}_30d $controls, absorb(i.chb_year#i.ID_cell_3 i.chb_month#i.ID_cell_3) vce(cluster i.ID_cell_3)
// outreg2 using "$OUTPUTS/regression_outs_timefe_prec${historic_means}", tex excel label append addtext(Cell#Year FE, Yes, Cell#Month FE, Yes, Cell size, 2deg)  nonotes addnote(SE clustered by Cell, *p<.05; **p<.01; ***p<.001)
//
// reghdfe child_agedeath_30d  ${prec_previous} ${precipitation}_30d $controls, absorb(i.chb_year#i.ID_cell_4 i.chb_month#i.ID_cell_4) vce(cluster i.ID_cell_4)
// outreg2 using "$OUTPUTS/regression_outs_timefe_prec${historic_means}", tex excel label append addtext(Cell#Year FE, Yes, Cell#Month FE, Yes, Cell size, 4deg)  nonotes addnote(SE clustered by Cell, *p<.05; **p<.01; ***p<.001)


*** MODEL 2 - all childs that survived 30days
drop if child_agedeath_30d==1
global prec_previous = "${prec_previous} ${precipitation}_30d"

reghdfe child_agedeath_30d3m ${prec_previous} ${precipitation}_30d3m $controls, absorb(i.chb_year#i.ID_cell i.chb_month#i.ID_cell) vce(cluster i.ID_cell)
outreg2 using "$OUTPUTS/regression_outs_timefe_prec${historic_means}", tex excel label append addtext(Cell#Year FE, Yes, Cell#Month FE, Yes, Cell size, 0.5deg)  nonotes addnote(SE clustered by Cell, *p<.05; **p<.01; ***p<.001)

// reghdfe child_agedeath_30d3m ${prec_previous} ${precipitation}_30d3m $controls, absorb(i.chb_year#i.ID_cell_2 i.chb_month#i.ID_cell_2) vce(cluster i.ID_cell_2)
// outreg2 using "$OUTPUTS/regression_outs_timefe_prec${historic_means}", tex excel label append addtext(Cell#Year FE, Yes, Cell#Month FE, Yes, Cell size, 1deg)  nonotes addnote(SE clustered by Cell, *p<.05; **p<.01; ***p<.001)
//
// reghdfe child_agedeath_30d3m ${prec_previous} ${precipitation}_30d3m $controls, absorb(i.chb_year#i.ID_cell_3 i.chb_month#i.ID_cell_3) vce(cluster i.ID_cell_3)
// outreg2 using "$OUTPUTS/regression_outs_timefe_prec${historic_means}", tex excel label append addtext(Cell#Year FE, Yes, Cell#Month FE, Yes, Cell size, 2deg)  nonotes addnote(SE clustered by Cell, *p<.05; **p<.01; ***p<.001)
//
// reghdfe child_agedeath_30d3m ${prec_previous} ${precipitation}_30d3m $controls, absorb(i.chb_year#i.ID_cell_4 i.chb_month#i.ID_cell_4) vce(cluster i.ID_cell_4)
// outreg2 using "$OUTPUTS/regression_outs_timefe_prec${historic_means}", tex excel label append addtext(Cell#Year FE, Yes, Cell#Month FE, Yes, Cell size, 4deg)  nonotes addnote(SE clustered by Cell, *p<.05; **p<.01; ***p<.001)




*** MODEL 3 - all childs that survived 3months
drop if child_agedeath_30d3m==1
global prec_previous = "${prec_previous} ${precipitation}_30d3m"

reghdfe child_agedeath_3m6m ${prec_previous} ${precipitation}_3m6m $controls, absorb(i.chb_year#i.ID_cell i.chb_month#i.ID_cell) vce(cluster i.ID_cell)
outreg2 using "$OUTPUTS/regression_outs_timefe_prec${historic_means}", tex excel label append addtext(Cell#Year FE, Yes, Cell#Month FE, Yes, Cell size, 0.5deg)  nonotes addnote(SE clustered by Cell, *p<.05; **p<.01; ***p<.001)

// reghdfe child_agedeath_3m6m ${prec_previous} ${precipitation}_3m6m $controls, absorb(i.chb_year#i.ID_cell_2 i.chb_month#i.ID_cell_2) vce(cluster i.ID_cell_2)
// outreg2 using "$OUTPUTS/regression_outs_timefe_prec${historic_means}", tex excel label append addtext(Cell#Year FE, Yes, Cell#Month FE, Yes, Cell size, 1deg)  nonotes addnote(SE clustered by Cell, *p<.05; **p<.01; ***p<.001)
//
// reghdfe child_agedeath_3m6m ${prec_previous} ${precipitation}_3m6m $controls, absorb(i.chb_year#i.ID_cell_3 i.chb_month#i.ID_cell_3) vce(cluster i.ID_cell_3)
// outreg2 using "$OUTPUTS/regression_outs_timefe_prec${historic_means}", tex excel label append addtext(Cell#Year FE, Yes, Cell#Month FE, Yes, Cell size, 2deg)  nonotes addnote(SE clustered by Cell, *p<.05; **p<.01; ***p<.001)
//
// reghdfe child_agedeath_3m6m ${prec_previous} ${precipitation}_3m6m $controls, absorb(i.chb_year#i.ID_cell_4 i.chb_month#i.ID_cell_4) vce(cluster i.ID_cell_4)
// outreg2 using "$OUTPUTS/regression_outs_timefe_prec${historic_means}", tex excel label append addtext(Cell#Year FE, Yes, Cell#Month FE, Yes, Cell size, 4deg)  nonotes addnote(SE clustered by Cell, *p<.05; **p<.01; ***p<.001)



*** MODEL 4 - all childs that survived 6months
drop if child_agedeath_3m6m==1
global prec_previous = "${prec_previous} ${precipitation}_3m6m"

reghdfe child_agedeath_6m12m ${prec_previous} ${precipitation}_6m12m $controls, absorb(i.chb_year#i.ID_cell i.chb_month#i.ID_cell) vce(cluster i.ID_cell)
outreg2 using "$OUTPUTS/regression_outs_timefe_prec${historic_means}", tex excel label append addtext(Cell#Year FE, Yes, Cell#Month FE, Yes, Cell size, 0.5deg)  nonotes addnote(SE clustered by Cell, *p<.05; **p<.01; ***p<.001)

// reghdfe child_agedeath_6m12m ${prec_previous} ${precipitation}_6m12m $controls, absorb(i.chb_year#i.ID_cell_2 i.chb_month#i.ID_cell_2) vce(cluster i.ID_cell_2)
// outreg2 using "$OUTPUTS/regression_outs_timefe_prec${historic_means}", tex excel label append addtext(Cell#Year FE, Yes, Cell#Month FE, Yes, Cell size, 1deg)  nonotes addnote(SE clustered by Cell, *p<.05; **p<.01; ***p<.001)
//
// reghdfe child_agedeath_6m12m ${prec_previous} ${precipitation}_6m12m $controls, absorb(i.chb_year#i.ID_cell_3 i.chb_month#i.ID_cell_3) vce(cluster i.ID_cell_3)
// outreg2 using "$OUTPUTS/regression_outs_timefe_prec${historic_means}", tex excel label append addtext(Cell#Year FE, Yes, Cell#Month FE, Yes, Cell size, 2deg)  nonotes addnote(SE clustered by Cell, *p<.05; **p<.01; ***p<.001)
//
// reghdfe child_agedeath_6m12m ${prec_previous} ${precipitation}_6m12m $controls, absorb(i.chb_year#i.ID_cell_4 i.chb_month#i.ID_cell_4) vce(cluster i.ID_cell_4)
// outreg2 using "$OUTPUTS/regression_outs_timefe_prec${historic_means}", tex excel label append addtext(Cell#Year FE, Yes, Cell#Month FE, Yes, Cell size, 4deg)  nonotes addnote(SE clustered by Cell, *p<.05; **p<.01; ***p<.001)


