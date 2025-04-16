clear all
set maxvar 120000

global PROJECT = "D:\World Bank\Paper - Child mortality and Climate Shocks"
global OUTPUTS = "${PROJECT}\Outputs"
global DATA = "${PROJECT}\Data"
global DATA_IN = "${DATA}\Data_in"
global DATA_PROC = "${DATA}\Data_proc"
global DATA_OUT = "${DATA}\Data_out"

// 1. Seguro ya están siguiendo las instrucciones de la DHS, pero si no es así, vale la pena revisarlas pues recomiendan algunas transformaciones dependiendo de cómo se estén usando los pesos (aquí).
// 2. En algún momento hablamos con Jed sobre excluir del análisis a aquellos niños que nacieron 12 meses alrededor de la fecha de la encuesta y no más allá de 10 y 15 años del momento de la encuesta. Esto para capturar algunas variaciones de la metodología y errores de medida. Para esto pueden usar las variables child_agem y child_agey, que fueron creadas usando la fecha de la entrevista (v008) y la fecha de nacimiento (b3) de cada individuo.
// 3. No estoy segura desde qué año es la muestra final que están usando, pero valdría la pena correr los resultados solo para aquellas observaciones donde la entrevista ocurrió desde el 2003. Excluir cerca del 11% que toma lugar antes del 2003. Esto teniendo en cuenta el siguiente statement:
// "Georeferenced surveys from 2003 onwards are displaced using the standard displacement procedure described in SAR7 (https://dhsprogram.com/pubs/pdf/SAR7/SAR7.pdf). Urban clusters are displaced up to 2km, while rural clusters are displaced up to 5km with a further 1% of rural clusters displaced up to 10km. Surveys conducted prior to 2003 were not displaced using the standard displacement procedure. Coordinates for the earliest surveys were obtained from paper maps, gazetteers of settlement names, or preexisting census data files, while GPS collection began in 1996. The method used to determine the lat/long coordinates for each cluster is listed under the SOURCE attribute in the GE datasets."



*############################################################*
*### 	 Read data and Merge with Climate Data
*############################################################*

import excel using "D:\World Bank\Data-Portal-Brief-Generator\Data\Data_Raw\Country codes & metadata\country_classification.xlsx", clear first
rename wbcode code_iso3
save "${DATA_IN}/Income level.dta", replace

use "${DATA_IN}/DHS/DHSBirthsGlobalAnalysis_11072024", clear
gen ID = _n - 1
merge 1:1 ID using "${DATA_PROC}/ClimateShocks_assigned_v9c" // Add climate variables
keep if _merge==3
drop _merge
merge m:1  code_iso3 using "${DATA_IN}/Income Level.dta" // Add income group
keep if _merge==3
drop _merge
merge m:1  ID_HH using "${DATA_PROC}/DHSBirthsGlobalAnalysis_11072024_climate_bands_assigned.dta" // Add climate bands and southern hemisphere dummy
keep if _merge==3
drop _merge

// merge m:1 v000 v001 v002 v008 using "${DATA_IN}/DHS/weights_DHS_by_hh.dta"
// keep if _merge==3
// drop _merge

*############################################################*
*# 	 Crate climate variables
*############################################################*

/* * Create min-max variables for linear and quadratic models without dummies. Only the biggest effect is the one considered (i.e. where the deviation is bigger)
*	For example, if there were a -1.5 shock and a +1.1 shock, we keep the -1.5 for the variable `var'_`time'_`stat'_minmax
foreach var in "t" "std_t" "stdm_t" "absdif_t" "absdifm_t" "spi1" "spi3" "spi6" "spi9" "spi12" "spi24" { // "spi48"{
	foreach time in "inutero_1m3m" "inutero_4m6m" "inutero_6m9m" "born_1m3m" "born_3m6m" "born_6m9m" "born_9m12m" {
		gen `var'_`time'_min_abs = sqrt(`var'_`time'_min*`var'_`time'_min)
		gen `var'_`time'_max_abs = sqrt(`var'_`time'_max*`var'_`time'_max)
		gen		 `var'_`time'_minmax = `var'_`time'_min if `var'_`time'_min_abs>=`var'_`time'_max_abs
		replace  `var'_`time'_minmax = `var'_`time'_max if `var'_`time'_min_abs<=`var'_`time'_max_abs
	}
} */

foreach var in  "std_t" "stdm_t" "spi1" "spi3" "spi6" "spi9" "spi12" "spi24" { // "t" "absdif_t" "absdifm_t" "spi48"{
	foreach time in "inutero_1m3m" "inutero_4m6m" "inutero_6m9m" "born_1m3m" "born_3m6m" "born_6m9m" "born_9m12m" "born_12m15m" "born_15m18m" "born_18m21m" "born_21m24m" {
		foreach stat in  "avg" { // "minmax" {
			
			* Quadratic term
			gen `var'_`time'_`stat'_sq = `var'_`time'_`stat' * `var'_`time'_`stat'
			
			* Positive and negative linear
			if "`stat'"=="minmax" {
				local posstat = "max"
				local negstat = "min"

				gen `var'_`time'_`stat'_pos = 0
				replace `var'_`time'_`stat'_pos = `var'_`time'_max if `var'_`time'_max>=0
				gen `var'_`time'_`stat'_neg = 0 
				replace `var'_`time'_`stat'_neg = `var'_`time'_min if `var'_`time'_min<=0	

				* Positive and negative dummy greater than threshold. 
				foreach sd in 1 2 {
					* This part is tricky. For minmax, we have a bimodal distribution for min and max values, therefore we
					*	treat them as two different distributions, computing their own sd and mean. For avg, we only compute their
					*	sd and mean of the pooled distribution!
					qui sum `var'_`time'_max if `var'_`time'_max>0
					local threshold_max = r(mean) + `sd'*r(sd)
					gen `var'_`time'_`stat'_gt`sd'   = (`var'_`time'_max>=`threshold_max') * `var'_`time'_max
					gen `var'_`time'_`stat'_bt0`sd'  = ((`var'_`time'_max>=0) & (`var'_`time'_max<`threshold_max')) * `var'_`time'_max
					
					qui sum `var'_`time'_min if `var'_`time'_min>0
					local threshold_min = r(mean) + `sd'*r(sd)
					gen `var'_`time'_`stat'_ltm`sd'  = (`var'_`time'_min<`threshold_min') * `var'_`time'_min
					gen `var'_`time'_`stat'_bt0m`sd' = ((`var'_`time'_min<0) & (`var'_`time'_min>=`threshold_min')) * `var'_`time'_min
				}
			}
			else {
				gen `var'_`time'_avg_pos = 0
				replace `var'_`time'_avg_pos = `var'_`time'_avg if `var'_`time'_avg>=0
				gen `var'_`time'_avg_neg = 0
				replace `var'_`time'_avg_neg = `var'_`time'_avg  if `var'_`time'_avg<=0	

				* Positive and negative dummy greater than threshold. 
				foreach sd in 1 2 {
					qui sum `var'_`time'_avg
					local threshold_avg_pos = r(mean) + `sd'*r(sd)
					local threshold_avg_neg = r(mean) - `sd'*r(sd)
					
					gen `var'_`time'_avg_gt`sd'   = (`var'_`time'_avg>=`threshold_avg_pos') * `var'_`time'_avg
					gen `var'_`time'_avg_bt0`sd'  = ((`var'_`time'_avg<`threshold_avg_pos') & (`var'_`time'_avg>=0)) * `var'_`time'_avg
					gen `var'_`time'_avg_bt0m`sd'  = ((`var'_`time'_avg>=`threshold_avg_neg') & (`var'_`time'_avg<0)) * `var'_`time'_avg
					gen `var'_`time'_avg_ltm`sd' = (`var'_`time'_avg<`threshold_avg_neg') * `var'_`time'_avg
				}

			}
		
	
		}
	}
}


drop index

*############################################################*
*# 	 Create child agedeath variables
*############################################################*

drop child_agedeath_3m6m
gen child_agedeath_1m3m = 0
gen child_agedeath_3m6m = 0
gen child_agedeath_6m9m = 0
gen child_agedeath_9m12m = 0
gen child_agedeath_12m15m = 0
gen child_agedeath_15m18m = 0
gen child_agedeath_18m21m = 0
gen child_agedeath_21m24m = 0

replace child_agedeath_1m3m= 1 if child_agedeath>=0 & child_agedeath<3
replace child_agedeath_3m6m = 1 if child_agedeath>=3 & child_agedeath<6
replace child_agedeath_6m9m = 1 if child_agedeath>=6 & child_agedeath<9
replace child_agedeath_9m12m = 1 if child_agedeath>=9 & child_agedeath<12
replace child_agedeath_12m15m = 1 if child_agedeath>=12 & child_agedeath<15
replace child_agedeath_15m18m = 1 if child_agedeath>=15 & child_agedeath<18
replace child_agedeath_18m21m = 1 if child_agedeath>=18 & child_agedeath<21
replace child_agedeath_21m24m = 1 if child_agedeath>=21 & child_agedeath<24

replace child_agedeath_1m3m = child_agedeath_1m3m * 1000
replace child_agedeath_3m6m = child_agedeath_3m6m * 1000
replace child_agedeath_6m9m = child_agedeath_6m9m * 1000
replace child_agedeath_9m12m = child_agedeath_9m12m * 1000
replace child_agedeath_12m15m = child_agedeath_12m15m * 1000
replace child_agedeath_15m18m = child_agedeath_15m18m * 1000
replace child_agedeath_18m21m = child_agedeath_18m21m * 1000
replace child_agedeath_21m24m = child_agedeath_21m24m * 1000

*############################################################*
*# 	 Create control variables for the regressions
*############################################################*

* Genero ID_cell con las celdas originales (0.1x0.1)
rename (lat lon) (lat_climate lon_climate)
egen ID_cell1 = group(lat_climate lon_climate)

* Celdas agrupadas de a 4 (0.25x0.25)
gen lat_climate_2 = round(LATNUM*4, 1)/4
gen lon_climate_2 = round(LONGNUM*4, 1)/4
egen ID_cell2 = group(lat_climate_2 lon_climate_2)

* Celdas agrupadas de a 4 (0.5x0.5)
gen lat_climate_3 = round(LATNUM*2, 1)/2
gen lon_climate_3 = round(LONGNUM*2, 1)/2
egen ID_cell3 = group(lat_climate_3 lon_climate_3)

* Celdas agrupadas de a 4 (1x1)
gen lat_climate_4 = round(LATNUM, 1)
gen lon_climate_4 = round(LONGNUM, 1)
egen ID_cell4 = group(lat_climate_4 lon_climate_4)

* Celdas agrupadas de a 8 (2x2)
gen lat_climate_5 = lat_climate_4 - mod(lat_climate_4, 2) // Substract one if value is odd
gen lon_climate_5 = lat_climate_4 - mod(lat_climate_4, 2)
egen ID_cell5 = group(lat_climate_5 lon_climate_5)


encode code_iso3, generate(ID_country)

foreach var in mother_ageb mother_eduy {

	gen `var'_squ = `var'^2
	gen `var'_cub = `var'^3

}

sort ID_R chb_year chb_month
by ID_R: gen birth_order = _n 

gen chb_year_sq = chb_year*chb_year

*############################################################*
*# 	 Keep from_2003 and born in last_10_years
*############################################################*

// keep if since_2003==1 & last_10_years==1

encode v000, gen(IDsurvey_country)

gen time = chb_year - 1989
gen time_sq = time*time


*############################################################*
*# 	 Keep only relevant vars
*############################################################*

// drop *_min *_max *_min_* *_max_*

keep  ID ID_R ID_CB ID_HH t_* std_t_* stdm_t_* absdif_t_* absdifm_t_* spi* child_fem child_mulbirth birth_order rural d_weatlh_ind_2 d_weatlh_ind_3 d_weatlh_ind_4 d_weatlh_ind_5 mother_age mother_ageb_squ mother_ageb_cub mother_eduy mother_eduy_squ mother_eduy_cub chb_month chb_year chb_year_sq child_agedeath_* ID_cell* pipedw helec href hhaircon hhfan hhelectemp wbincomegroup climate_band_3 climate_band_2 climate_band_1 southern

drop child_agedeath_30d child_agedeath_30d3m child_agedeath_6m12m child_agedeath_12m
foreach var of varlist _all {
    // Check if the variable is of type double
    if "`: type `var''" == "double" {
		// Change the variable type to float
		recast float `var', force
    }
}

save "$DATA_OUT/DHSBirthsGlobal&ClimateShocks_v9c.dta", replace
export delimited using "$DATA_OUT/DHSBirthsGlobal&ClimateShocks_v9c.csv", replace quote





stop
foreach j in 3 4 5 {
	preserve
	collapse (count) spi1_inutero_avg, by(ID_cell`j' chb_month)
	foreach i in 5 10 20 30 40 50 100 {
		qui gen less_than_`i' = (spi1_inutero_avg < `i')
		tab less_than_`i'
	}
	restore
}

* Verificamos que esté todo ok
sum t_* std_t_* spei12_* spei6_* spei3_* spei1_*


