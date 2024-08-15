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

use "${DATA_IN}/DHS/DHSBirthsGlobalAnalysis_05142024", clear
gen ID = _n - 1
merge 1:1 ID using "${DATA_PROC}/ClimateShocks_assigned_v4"
keep if _merge==3
drop _merge

merge m:1  code_iso3 using "${DATA_IN}/Income Level.dta"
keep if _merge==3
drop _merge

// merge m:1 v000 v001 v002 v008 using "${DATA_IN}/DHS/weights_DHS_by_hh.dta"
// keep if _merge==3
// drop _merge

foreach months in "1" "3" "6" "9" "12" {
	foreach threshold in 1.5 2.0 2.5 {
		local threshold_str = subinstr("`threshold'",".","_",.)

		*############################################################*
		*# 	 Crate dummy variables
		*############################################################*
		
		* Drought
		count if spi`months'_inutero_min<-`threshold'
		if r(n)<2000 {
			display in red "Less than 2000 treated droughts for SPI`months'<`threshold'"
			continue
		}
		gen drought`months'_`threshold_str'_inutero_avg	= (spi`months'_inutero_avg<-`threshold')
		gen drought`months'_`threshold_str'_inutero_min = (spi`months'_inutero_min<-`threshold')
		gen drought`months'_`threshold_str'_inutero_max = (spi`months'_inutero_max<-`threshold')
		gen drought`months'_`threshold_str'_30d_avg	   	= (spi`months'_30d_mean<-`threshold')
		gen drought`months'_`threshold_str'_30d_min	   	= (spi`months'_30d_min<-`threshold')
		gen drought`months'_`threshold_str'_30d_max	   	= (spi`months'_30d_max<-`threshold')
		gen drought`months'_`threshold_str'_2m12m_avg  	= (spi`months'_2m12m_mean<-`threshold')
		gen drought`months'_`threshold_str'_2m12m_min  	= (spi`months'_2m12m_min <-`threshold')
		gen drought`months'_`threshold_str'_2m12m_max  	= (spi`months'_2m12m_max <-`threshold')
			
		label var drought`months'_`threshold_str'_inutero_avg	"Affected by Drought in Utero (avg. SPI`months'<-`threshold'std)"
		label var drought`months'_`threshold_str'_inutero_min 	"Affected by Drought in Utero (min. SPI`months'<-`threshold'std)"
		label var drought`months'_`threshold_str'_inutero_max 	"Affected by Drought in Utero (max. SPI`months'<-`threshold'std)"
		label var drought`months'_`threshold_str'_30d_avg		"Affected by Drought 0-30 days (avg. SPI`months'<-`threshold'std)"   
		label var drought`months'_`threshold_str'_30d_min		"Affected by Drought 0-30 days (min. SPI`months'<-`threshold'std)"   
		label var drought`months'_`threshold_str'_30d_max		"Affected by Drought 0-30 days (max. SPI`months'<-`threshold'std)"   
		label var drought`months'_`threshold_str'_2m12m_avg 	"Affected by Drought 6-12 months (avg. SPI`months'<-`threshold'std)" 
		label var drought`months'_`threshold_str'_2m12m_min 	"Affected by Drought 6-12 months (min. SPI`months'<-`threshold'std)" 
		label var drought`months'_`threshold_str'_2m12m_max 	"Affected by Drought 6-12 months (max. SPI`months'<-`threshold'std)" 
		
		* Excessive Rain
		count if spi`months'_inutero_avg>`threshold'
		if r(n)<2000 {
			display in red "Less than 2000 treated droughts for SPI`months'>`threshold'"	
			continue
		}
		gen excessiverain`months'_`threshold_str'_inutero_avg	= (spi`months'_inutero_avg>`threshold')
		gen excessiverain`months'_`threshold_str'_inutero_min 	= (spi`months'_inutero_min>`threshold')
		gen excessiverain`months'_`threshold_str'_inutero_max 	= (spi`months'_inutero_max>`threshold')
		gen excessiverain`months'_`threshold_str'_30d_avg		= (spi`months'_30d_mean>`threshold')
		gen excessiverain`months'_`threshold_str'_30d_min		= (spi`months'_30d_min>`threshold')
		gen excessiverain`months'_`threshold_str'_30d_max		= (spi`months'_30d_max>`threshold')
		gen excessiverain`months'_`threshold_str'_2m12m_avg 	= (spi`months'_2m12m_mean>`threshold')
		gen excessiverain`months'_`threshold_str'_2m12m_min 	= (spi`months'_2m12m_min >`threshold')
		gen excessiverain`months'_`threshold_str'_2m12m_max 	= (spi`months'_2m12m_max >`threshold')
			
		label var excessiverain`months'_`threshold_str'_inutero_avg		"Affected by Ex. Rain in Utero (avg. SPI`months'>`threshold'std)"
		label var excessiverain`months'_`threshold_str'_inutero_min     "Affected by Ex. Rain in Utero (min. SPI`months'>`threshold'std)"
		label var excessiverain`months'_`threshold_str'_inutero_max     "Affected by Ex. Rain in Utero (max. SPI`months'>`threshold'std)"
		label var excessiverain`months'_`threshold_str'_30d_avg			"Affected by Ex. Rain 0-30 days (avg. SPI`months'>`threshold'std)"   
		label var excessiverain`months'_`threshold_str'_30d_min			"Affected by Ex. Rain 0-30 days (min. SPI`months'>`threshold'std)"   
		label var excessiverain`months'_`threshold_str'_30d_max			"Affected by Ex. Rain 0-30 days (max. SPI`months'>`threshold'std)"   
		label var excessiverain`months'_`threshold_str'_2m12m_avg 		"Affected by Ex. Rain 6-12 months (avg. SPI`months'>`threshold'std)" 
		label var excessiverain`months'_`threshold_str'_2m12m_min 		"Affected by Ex. Rain 6-12 months (min. SPI`months'>`threshold'std)" 
		label var excessiverain`months'_`threshold_str'_2m12m_max 		"Affected by Ex. Rain 6-12 months (max. SPI`months'>`threshold'std)" 	
		
	}

rename spi`months'_30d_mean     spi`months'_30d_avg
rename spi`months'_2m12m_mean  spi`months'_2m12m_avg


label var spi`months'_inutero_avg	 	"Avg. Standarized Precipitation Index in Utero"
label var spi`months'_inutero_min	 	"Min. Standarized Precipitation Index in Utero"
label var spi`months'_inutero_max	 	"Max. Standarized Precipitation Index in Utero"
label var spi`months'_30d_avg       	"Avg. Standarized Precipitation Index 0-30 days"
label var spi`months'_30d_min       	"Min. Standarized Precipitation Index 0-30 days"
label var spi`months'_30d_max       	"Max. Standarized Precipitation Index 0-30 days"
label var spi`months'_2m12m_avg   		"Avg. Standarized Precipitation Index 2-12 months"
label var spi`months'_2m12m_min   		"Min. Standarized Precipitation Index 2-12 months"
label var spi`months'_2m12m_max   		"Max. Standarized Precipitation Index 2-12 months"

}

rename t_30d_mean   	t_30d_avg 	
rename t_2m12m_mean   	t_2m12m_avg 	

label var t_inutero_avg		"Avg. Temperature in Utero"	
label var t_inutero_min		"Min. Temperature in Utero"	
label var t_inutero_max		"Max. Temperature in Utero"	
label var t_30d_avg 		"Avg. Temperature 0-30 days"	
label var t_30d_min 	 	"Min. Temperature 0-30 days"	
label var t_30d_max 		"Max. Temperature 0-30 days"	
label var t_2m12m_avg 		"Avg. Temperature 2-12 months"	
label var t_2m12m_min  		"Min. Temperature 6-12 months"	
label var t_2m12m_max  		"Max. Temperature 6-12 months"	

rename std_t_30d_mean   	 	std_t_30d_avg 		
rename std_t_2m12m_mean   		std_t_2m12m_avg

label var std_t_inutero_avg		"Avg. Standardized Temperature in Utero"	
label var std_t_inutero_min		"Min. Standardized Temperature in Utero"	
label var std_t_inutero_max		"Max. Standardized Temperature in Utero"	
label var std_t_30d_avg 		"Avg. Standardized Temperature 0-30 days"	
label var std_t_30d_min 	 	"Min. Standardized Temperature 0-30 days"	
label var std_t_30d_max 		"Max. Standardized Temperature 0-30 days"	
label var std_t_2m12m_avg 		"Avg. Standardized Temperature 2-12 months"	
label var std_t_2m12m_min  		"Min. Standardized Temperature 6-12 months"	
label var std_t_2m12m_max  		"Max. Standardized Temperature 6-12 months"	

rename stdm_t_30d_mean   	 	stdm_t_30d_avg 		
rename stdm_t_2m12m_mean   		stdm_t_2m12m_avg

label var std_t_inutero_avg		"Avg. Standardized Temperature in Utero"	
label var std_t_inutero_min		"Min. Standardized Temperature in Utero"	
label var std_t_inutero_max		"Max. Standardized Temperature in Utero"	
label var std_t_30d_avg 		"Avg. Standardized Temperature 0-30 days"	
label var std_t_30d_min 	 	"Min. Standardized Temperature 0-30 days"	
label var std_t_30d_max 		"Max. Standardized Temperature 0-30 days"	
label var std_t_2m12m_avg 		"Avg. Standardized Temperature 2-12 months"	
label var std_t_2m12m_min  		"Min. Standardized Temperature 6-12 months"	
label var stdm_t_2m12m_max  		"Max. Standardized Temperature 6-12 months"	

*############################################################*
*# 	 Crate squared variables
*############################################################*

foreach var in "t" "std_t" "stdm_t" "spi12" "spi6" "spi3" "spi1" {
	foreach time in "inutero_avg" "inutero_min" "inutero_max" "30d_avg" "30d_min" "30d_max" "2m12m_avg" "2m12m_min" "2m12m_max" {
		gen `var'_`time'_sq = `var'_`time' * `var'_`time'
	}
}


drop index

*############################################################*
*# 	 Create child agedeath variables
*############################################################*

egen child_agedeath_2m12m = rowmax(child_agedeath_30d3m child_agedeath_3m6m child_agedeath_6m12m)
replace child_agedeath_30d = child_agedeath_30d * 1000
replace child_agedeath_2m12m = child_agedeath_2m12m * 1000

*############################################################*
*# 	 Create control variables for the regressions
*############################################################*

* Genero ID_cell con las celdas originales
tostring lon_climate lat_climate , generate(lon_climate_str lat_climate_str )
gen ID_cell_str = lat_climate_str + "-" + lon_climate_str
encode ID_cell_str, gen(ID_cell1)
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
*# 	 Keep from_2003 and born in last_10_years
*############################################################*

// use "$DATA_IN/DHS/DHSBirthsGlobalAnalysis_05142024.dta", replace

keep if since_2003==1 & last_10_years==1

encode v000, gen(IDsurvey_country)

gen time = chb_year - 1989
gen time_sq = time*time

save "$DATA_OUT/DHSBirthsGlobal&ClimateShocks.dta", replace
export delimited using "$DATA_OUT/DHSBirthsGlobal&ClimateShocks.csv", replace

* Verificamos que esté todo ok
sum t_* std_t_* stdm_t_* spi12_* spi6_* spi3_* spi1_* drought* excessiverain*