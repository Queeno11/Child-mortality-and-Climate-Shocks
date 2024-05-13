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

import excel using "Z:\Laboral\World Bank\Data-Portal-Brief-Generator\Data\Data_Raw\Country codes & metadata\country_classification.xlsx", clear first
rename wbcode code_iso3
save "${DATA_IN}/Income level.dta", replace

use "${DATA_IN}/DHS/DHSBirthsGlobalAnalysis_04172024", clear
gen ID = _n - 1
merge 1:1 ID using "${DATA_PROC}/ClimateShocks_assigned_v3"
keep if _merge==3
drop _merge

merge m:1  code_iso3 using "${DATA_IN}/Income Level.dta"
keep if _merge==3
drop _merge

foreach months in "1" "3" "6" "9" "12" {
	foreach threshold in 1.5 2.0 2.5 {
		local threshold_str = subinstr("`threshold'",".","_",.)

		*############################################################*
		*# 	 Crate dummy variables
		*############################################################*
		
		* Drought
		count if spi`months'_inutero_q1<-`threshold'
		if r(n)<2000 {
			display in red "Less than 2000 treated droughts for SPI`months'<`threshold'"
			continue
		}
		gen drought`months'_`threshold_str'_q1 		 = (spi`months'_inutero_q1<-`threshold')
		gen drought`months'_`threshold_str'_q2 	 	 = (spi`months'_inutero_q2<-`threshold')
		gen drought`months'_`threshold_str'_q3 	 	 = (spi`months'_inutero_q3<-`threshold')
		gen drought`months'_`threshold_str'_30d 	 = (spi`months'_born_1m<-`threshold')
		gen drought`months'_`threshold_str'_30d3m	 = (spi`months'_born_2to3m<-`threshold')
		gen drought`months'_`threshold_str'_3m6m	 = (spi`months'_born_3to6m<-`threshold')
		gen drought`months'_`threshold_str'_6m12m	 = (spi`months'_born_6to12m<-`threshold')
		
		label var drought`months'_`threshold_str'_q1 "Affected by Drought 1stQ in Utero (SPI`months' <`threshold'std)"
		label var drought`months'_`threshold_str'_q2 "Affected by Drought 2ndQ in Utero (SPI`months' <`threshold'std)"
		label var drought`months'_`threshold_str'_q3 "Affected by Drought 3rdQ in Utero (SPI`months' <`threshold'std)"
		label var drought`months'_`threshold_str'_30d "Affected by Drought 0-30 days (SPI`months' <`threshold'std)"
		label var drought`months'_`threshold_str'_30d3m "Affected by Drought 1-3 months (SPI`months' <`threshold'std)"		
		label var drought`months'_`threshold_str'_3m6m "Affected by Drought 3-6 months (SPI`months' <`threshold'std)"
		label var drought`months'_`threshold_str'_6m12m "Affected by Drought 6-12 months (SPI`months' <`threshold'std)"	
		
		
		* Excessive Rain
		count if spi`months'_inutero_q1>`threshold'
		if r(n)<2000 {
			display in red "Less than 2000 treated droughts for SPI`months'>`threshold'"	
			continue
		}
		gen excessiverain`months'_`threshold_str'_q1 	 = (spi`months'_inutero_q1>`threshold')
		gen excessiverain`months'_`threshold_str'_q2 	 = (spi`months'_inutero_q2>`threshold')
		gen excessiverain`months'_`threshold_str'_q3 	 = (spi`months'_inutero_q3>`threshold')
		gen excessiverain`months'_`threshold_str'_30d 	 = (spi`months'_born_1m>`threshold')
		gen excessiverain`months'_`threshold_str'_30d3m	 = (spi`months'_born_2to3m>`threshold')
		gen excessiverain`months'_`threshold_str'_3m6m	 = (spi`months'_born_3to6m>`threshold')
		gen excessiverain`months'_`threshold_str'_6m12m	 = (spi`months'_born_6to12m>`threshold')

		label var excessiverain`months'_`threshold_str'_q1 "Affected by Ex. Rain 1stQ in Utero (rain >`threshold'std)"
		label var excessiverain`months'_`threshold_str'_q2 "Affected by Ex. Rain 2ndQ in Utero (rain >`threshold'std)"
		label var excessiverain`months'_`threshold_str'_q3 "Affected by Ex. Rain 3rdQ in Utero (rain >`threshold'std)"
		label var excessiverain`months'_`threshold_str'_30d "Affected by Ex. Rain 0-30 days (rain >`threshold'std)"
		label var excessiverain`months'_`threshold_str'_30d3m "Affected by Ex. Rain 1-3 months (rain >`threshold'std)"		
		label var excessiverain`months'_`threshold_str'_3m6m "Affected by Ex. Rain 3-6 months (rain >`threshold'std)"
		label var excessiverain`months'_`threshold_str'_6m12m "Affected by Ex. Rain 6-12 months (rain >`threshold'std)"		
		
	}

rename spi`months'_inutero_q1    spi`months'_q1 	
rename spi`months'_inutero_q2    spi`months'_q2 	
rename spi`months'_inutero_q3    spi`months'_q3 	
rename spi`months'_born_1m       spi`months'_30d 	
rename spi`months'_born_2to3m    spi`months'_30d3m
rename spi`months'_born_3to6m    spi`months'_3m6m 
rename spi`months'_born_6to12m   spi`months'_6m12m


label var spi`months'_q1 	 	"Standarized Precipitation Index 1stQ in Utero"
label var spi`months'_q2 	 	"Standarized Precipitation Index 2ndQ in Utero"
label var spi`months'_q3 	 	"Standarized Precipitation Index 3rdQ in Utero"
label var spi`months'_30d 	 	"Standarized Precipitation Index 0-30 days "
label var spi`months'_30d3m 	"Standarized Precipitation Index 1-3 months"		
label var spi`months'_3m6m  	"Standarized Precipitation Index 3-6 months"
label var spi`months'_6m12m 	"Standarized Precipitation Index 6-12 months"		

}

rename temp_inutero_q1    temp_q1 	
rename temp_inutero_q2    temp_q2 	
rename temp_inutero_q3    temp_q3 	
rename temp_born_1m       temp_30d 	
rename temp_born_2to3m    temp_30d3m
rename temp_born_3to6m    temp_3m6m 
rename temp_born_6to12m   temp_6m12m

label var temp_q1 	 	"Mean Temperature 1stQ in Utero"
label var temp_q2 	 	"Mean Temperature 2ndQ in Utero"
label var temp_q3 	 	"Mean Temperature 3rdQ in Utero"
label var temp_30d 	 	"Mean Temperature 0-30 days "
label var temp_30d3m 	"Mean Temperature 1-3 months"		
label var temp_3m6m  	"Mean Temperature 3-6 months"
label var temp_6m12m 	"Mean Temperature 6-12 months"		


drop index

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
//
// foreach cell_id in ID_cell ID_cell2 ID_cell3 ID_cell4 {
// 	levelsof `cell_id', local(celdas)
// 	foreach celda in `celdas' {
// 		gen `cell_id'_`celda' = 0
// 		replace `cell_id'_`celda' = 1 if `cell_id'==`celda'
// 	}
// 	tab `var', gen(`var'_)
// 	break
// 

save "$DATA_OUT/DHSBirthsGlobal&ClimateShocks.dta", replace
export delimited using "$DATA_OUT/DHSBirthsGlobal&ClimateShocks.csv", replace