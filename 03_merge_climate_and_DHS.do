clear all
set maxvar 120000

global PROJECT = "Z:\Laboral\World Bank\Paper - Child mortality and Climate Shocks"
global OUTPUTS = "${PROJECT}\Outputs"
global DATA = "${PROJECT}\Data"
global DATA_IN = "${DATA}\Data_in"
global DATA_PROC = "${DATA}\Data_proc"
global DATA_OUT = "${DATA}\Data_out"

*############################################################*
*### 	 Read data and Merge with Climate Data
*############################################################*

use "${DATA_IN}/DHS/DHSBirthsGlobalAnalysis_04172024", clear
gen ID = _n - 1
merge 1:1 ID using "${DATA_PROC}/ClimateShocks_assigned"
count if _merge!=3
// assert r(N)==0
keep if _merge==3
drop _merge


local historic_means = "_m"
foreach threshold in 2.0 2.5 3.0 3.5 4.0 4.5 {
	local threshold_str = subinstr("`threshold'",".","_",.)
	if "`historic_means'"=="_m"{
		local m_text = " month"
	}
	else {
		local m_text = ""
	}

	*############################################################*
	*# 	 Crate dummy variables
	*############################################################*
	
	
	* Drought
	count if prec_inutero_q1`historic_means'_min<-`threshold'
	if r(n)<2000 {
	    continue
	}
	gen drought`historic_means'_`threshold_str'_q1 	 = (prec_inutero_q1`historic_means'_min<-`threshold')
	gen drought`historic_means'_`threshold_str'_q2 	 = (prec_inutero_q2`historic_means'_min<-`threshold')
	gen drought`historic_means'_`threshold_str'_q3 	 = (prec_inutero_q3`historic_means'_min<-`threshold')
	gen drought`historic_means'_`threshold_str'_30d 	 = (prec_born_1m`historic_means'_min<-`threshold')
	gen drought`historic_means'_`threshold_str'_30d3m	 = (prec_born_2to3m`historic_means'_min<-`threshold')
	gen drought`historic_means'_`threshold_str'_3m6m	 = (prec_born_3to6m`historic_means'_min<-`threshold')
	gen drought`historic_means'_`threshold_str'_6m12m	 = (prec_born_6to9m`historic_means'_min<-`threshold') | (prec_born_9to12m_min<-`threshold')
	
	label var drought`historic_means'_`threshold_str'_q1 "Affected by Drought 1stQ in Utero (rain <`threshold'std`m_text')"
	label var drought`historic_means'_`threshold_str'_q2 "Affected by Drought 2ndQ in Utero (rain <`threshold'std`m_text')"
	label var drought`historic_means'_`threshold_str'_q3 "Affected by Drought 3rdQ in Utero (rain <`threshold'std`m_text')"
	label var drought`historic_means'_`threshold_str'_30d "Affected by Drought 0-30 days (rain <`threshold'std`m_text')"
	label var drought`historic_means'_`threshold_str'_30d3m "Affected by Drought 1-3 months (rain <`threshold'std`m_text')"		
	label var drought`historic_means'_`threshold_str'_3m6m "Affected by Drought 3-6 months (rain <`threshold'std`m_text')"
	label var drought`historic_means'_`threshold_str'_6m12m "Affected by Drought 6-12 months (rain <`threshold'std`m_text')"	
	
	
	* Excessive Rain
	count if prec_inutero_q1`historic_means'_max>`threshold'
	if r(n)<2000 {
	    continue
	}
	gen excessiverain`historic_means'_`threshold_str'_q1 	 = (prec_inutero_q1`historic_means'_max>`threshold')
	gen excessiverain`historic_means'_`threshold_str'_q2 	 = (prec_inutero_q2`historic_means'_max>`threshold')
	gen excessiverain`historic_means'_`threshold_str'_q3 	 = (prec_inutero_q3`historic_means'_max>`threshold')
	gen excessiverain`historic_means'_`threshold_str'_30d 	 = (prec_born_1m`historic_means'_max>`threshold')
	gen excessiverain`historic_means'_`threshold_str'_30d3m	 = (prec_born_2to3m`historic_means'_max>`threshold')
	gen excessiverain`historic_means'_`threshold_str'_3m6m	 = (prec_born_3to6m`historic_means'_max>`threshold')
	gen excessiverain`historic_means'_`threshold_str'_6m12m	 = (prec_born_6to9m`historic_means'_max>`threshold') | (prec_born_9to12m_max>`threshold')

	label var excessiverain`historic_means'_`threshold_str'_q1 "Affected by Ex. Rain 1stQ in Utero (rain >`threshold'std`m_text')"
	label var excessiverain`historic_means'_`threshold_str'_q2 "Affected by Ex. Rain 2ndQ in Utero (rain >`threshold'std`m_text')"
	label var excessiverain`historic_means'_`threshold_str'_q3 "Affected by Ex. Rain 3rdQ in Utero (rain >`threshold'std`m_text')"
	label var excessiverain`historic_means'_`threshold_str'_30d "Affected by Ex. Rain 0-30 days (rain >`threshold'std`m_text')"
	label var excessiverain`historic_means'_`threshold_str'_30d3m "Affected by Ex. Rain 1-3 months (rain >`threshold'std`m_text')"		
	label var excessiverain`historic_means'_`threshold_str'_3m6m "Affected by Ex. Rain 3-6 months (rain >`threshold'std`m_text')"
	label var excessiverain`historic_means'_`threshold_str'_6m12m "Affected by Ex. Rain 6-12 months (rain >`threshold'std`m_text')"		
	
}

* Rain
gen prec`historic_means'_q1 	 = (prec_inutero_q1`historic_means'_min)
gen prec`historic_means'_q2 	 = (prec_inutero_q2`historic_means'_min)
gen prec`historic_means'_q3 	 = (prec_inutero_q3`historic_means'_min)
gen prec`historic_means'_30d 	 = (prec_born_1m`historic_means'_min)
gen prec`historic_means'_30d3m	 = (prec_born_2to3m`historic_means'_min)
gen prec`historic_means'_3m6m	 = (prec_born_3to6m`historic_means'_min)
gen prec`historic_means'_6m12m	 = min(prec_born_6to9m`historic_means'_min, prec_born_9to12m_min)

gen prec`historic_means'_neg_q1 	 =  prec`historic_means'_q1 	if prec`historic_means'_q1 		<0
gen prec`historic_means'_neg_q2 	 =  prec`historic_means'_q2 	if prec`historic_means'_q2 		<0
gen prec`historic_means'_neg_q3 	 =  prec`historic_means'_q3 	if prec`historic_means'_q3 		<0
gen prec`historic_means'_neg_30d 	 =  prec`historic_means'_30d 	if prec`historic_means'_30d 	<0
gen prec`historic_means'_neg_30d3m	 =  prec`historic_means'_30d3m	if prec`historic_means'_30d3m	<0
gen prec`historic_means'_neg_3m6m	 =  prec`historic_means'_3m6m	if prec`historic_means'_3m6m	<0
gen prec`historic_means'_neg_6m12m	 =  prec`historic_means'_6m12m	if prec`historic_means'_6m12m	<0

gen prec`historic_means'_pos_q1 	 =  prec`historic_means'_q1 	if prec`historic_means'_q1 		>0
gen prec`historic_means'_pos_q2 	 =  prec`historic_means'_q2 	if prec`historic_means'_q2 		>0
gen prec`historic_means'_pos_q3 	 =  prec`historic_means'_q3 	if prec`historic_means'_q3 		>0
gen prec`historic_means'_pos_30d 	 =  prec`historic_means'_30d 	if prec`historic_means'_30d 	>0
gen prec`historic_means'_pos_30d3m	 =  prec`historic_means'_30d3m	if prec`historic_means'_30d3m	>0
gen prec`historic_means'_pos_3m6m	 =  prec`historic_means'_3m6m	if prec`historic_means'_3m6m	>0
gen prec`historic_means'_pos_6m12m	 =  prec`historic_means'_6m12m	if prec`historic_means'_6m12m	>0

label var prec`historic_means'_q1 	 "Standarized Precipitation 1stQ in Utero (min)"
label var prec`historic_means'_q2 	 "Standarized Precipitation 2ndQ in Utero (min)"
label var prec`historic_means'_q3 	 "Standarized Precipitation 3rdQ in Utero (min)"
label var prec`historic_means'_30d 	 "Standarized Precipitation 0-30 days (min)"
label var prec`historic_means'_30d3m "Standarized Precipitation 1-3 months (min)"		
label var prec`historic_means'_3m6m  "Standarized Precipitation 3-6 months (min)"
label var prec`historic_means'_6m12m "Standarized Precipitation 6-12 months (min)"		

label var prec`historic_means'_neg_q1 	 "Standarized Precipitation 1stQ in Utero (min<0)"
label var prec`historic_means'_neg_q2 	 "Standarized Precipitation 2ndQ in Utero (min<0)"
label var prec`historic_means'_neg_q3 	 "Standarized Precipitation 3rdQ in Utero (min<0)"
label var prec`historic_means'_neg_30d 	 "Standarized Precipitation 0-30 days (min<0)"
label var prec`historic_means'_neg_30d3m "Standarized Precipitation 1-3 months (min<0)"		
label var prec`historic_means'_neg_3m6m  "Standarized Precipitation 3-6 months (min<0)"
label var prec`historic_means'_neg_6m12m "Standarized Precipitation 6-12 months (min<0)"	

label var prec`historic_means'_pos_q1 	 "Standarized Precipitation 1stQ in Utero (min>0)"
label var prec`historic_means'_pos_q2 	 "Standarized Precipitation 2ndQ in Utero (min>0)"
label var prec`historic_means'_pos_q3 	 "Standarized Precipitation 3rdQ in Utero (min>0)"
label var prec`historic_means'_pos_30d 	 "Standarized Precipitation 0-30 days (min>0)"
label var prec`historic_means'_pos_30d3m "Standarized Precipitation 1-3 months (min>0)"		
label var prec`historic_means'_pos_3m6m  "Standarized Precipitation 3-6 months (min>0)"
label var prec`historic_means'_pos_6m12m "Standarized Precipitation 6-12 months (min>0)"	

drop prec_inutero* prec_born* index

*############################################################*
*# 	 Create control variables for the regressions
*############################################################*

* Genero ID_cell con las celdas originales
tostring lon_climate lat_climate , generate(lon_climate_str lat_climate_str )
gen ID_cell_str = lat_climate_str + "-" + lon_climate_str
encode ID_cell_str, gen(ID_cell)
drop ID_cell_str lon_climate_str lat_climate_str 

* Celdas agrupadas de a 4
gen lat_climate_2 = round(lat_climate, 1)
gen lon_climate_2 = round(lon_climate, 1)
tostring lon_climate_2 lat_climate_2 , generate(lon_climate_str lat_climate_str )
gen ID_cell_str = lat_climate_str + "-" + lon_climate_str
encode ID_cell_str, gen(ID_cell2)
drop ID_cell_str lon_climate_str lat_climate_str 

* Celdas agrupadas de a 8
gen lat_climate_3 = lat_climate_2 - mod(lat_climate_2, 2) // Substract one if value is odd
gen lon_climate_3 = lon_climate_2 - mod(lon_climate_2, 2)
tostring lon_climate_3 lat_climate_3 , generate(lon_climate_str lat_climate_str )
gen ID_cell_str = lat_climate_str + "-" + lon_climate_str
encode ID_cell_str, gen(ID_cell3)
drop ID_cell_str lon_climate_str lat_climate_str 

* Celdas agrupadas de a 16
gen lat_climate_4 = lat_climate_2 - mod(lat_climate_2, 4) // Make divisible by 4
gen lon_climate_4 = lon_climate_2 - mod(lon_climate_2, 4)
tostring lon_climate_4 lat_climate_4 , generate(lon_climate_str lat_climate_str )
gen ID_cell_str = lat_climate_str + "-" + lon_climate_str
encode ID_cell_str, gen(ID_cell4)
drop ID_cell_str lon_climate_str lat_climate_str 

encode code_iso3, generate(ID_country)

foreach var in mother_ageb mother_eduy {

	gen `var'_squ = `var'^2
	gen `var'_cub = `var'^3

}

sort ID_R chb_year chb_month
by ID_R: gen birth_order = _n 


*############################################################*
*# 	 Create fixed effects variables
*############################################################*

foreach cell_id in ID_cell ID_cell2 ID_cell3 ID_cell4 {
	levelsof `cell_id', local(celdas)
	foreach celda in `celdas' {
		gen `cell_id'_`celda' = 0
		replace `cell_id'_`celda' = 1 if `cell_id'==`celda'
	}
	tab `var', gen(`var'_)
	break
}

save "$DATA_OUT/DHSBirthsGlobal&ClimateShocks.dta", replace