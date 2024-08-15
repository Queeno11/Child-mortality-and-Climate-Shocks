module CustomModels

# Cosas:
# Trend en decades en vez de año ----> CustonModelsDecade
# Effectos fijos por país
# Efecto fijo por edad

    export spi_temp_linear_regression

    using CSV, DataFrames, RDatasets, RegressionTables, FixedEffectModels, CUDA, ProgressMeter

    function spi_temp_linear_regression(df, months, controls, times, folder, extra)
        #################################################################
        ### VERSION 1: spi + temp models (linear)
        #################################################################
        println("\rRunning Model VERSION 1: spi + temp models (linear) - SPI$(months)\r")

        for temp in ["std_t"]
            for ind in [("avg", "avg", "avg")]
                nameind = ind[1]
                spiind = ind[2]
                tind = ind[3]

                spi_previous = [] 
                temp_previous = []
                order_spi = []
                order_temp = []
                regs = []

                for time in 1:(length(times)-1)
                    time1 = times[time]
                    time2 = times[time + 1]

                    spi_actual = [Symbol("spi$(months)_$(time2)_$(spiind)")]
                    temp_actual = [Symbol("$(temp)_$(time2)_$(spiind)")]
                    if time==1
                        spi_start = [Symbol("spi$(months)_$(time1)_$(tind)")]
                        temp_start = [Symbol("$(temp)_$(time1)_$(tind)")]
                        append!(order_spi, spi_start)
                        append!(order_temp, temp_start)
                        append!(spi_previous, spi_start)
                        append!(temp_previous, temp_start)
                    else
                        # only children that survived
                        df_temp = filter(row -> row[Symbol("child_agedeath_$(time1)")] == 0, df)
                    end
                    
                    for i in 1:4
                        fixed_effects = fe(Symbol("ID_cell$i"))&term(:chb_year) + fe(Symbol("ID_cell$i"))&fe(:chb_month)
                        reg_model = reg(
                            df, 
                            term(Symbol("child_agedeath_$(time2)")) ~ sum(term.(spi_previous)) + sum(term.(spi_actual))  + sum(term.(temp_previous)) + sum(term.(temp_actual)) + sum(controls) + fixed_effects, 
                            Vcov.cluster(Symbol("ID_cell$i")), 
                            weights = :hv005,
                            method=:CUDA
                        )
                        push!(regs, reg_model)
                    end
                    append!(spi_previous, spi_actual)
                    append!(temp_previous, temp_actual)
                    append!(order_spi, spi_actual)
                    append!(order_temp, temp_actual)
                end
                println(regs)

                # Generate regression table
                order = vcat(order_spi, order_temp)
                order = [string(sym) for sym in order]
                regtable(
                    regs...; 
                    render = AsciiTable(), 
                    file="D:\\World Bank\\Paper - Child mortality and Climate Shocks\\Outputs\\$(folder)version 1 SPI$months $(nameind) $(temp) $(extra).txt",
                    order=order,
                )
                regtable(
                    regs...; 
                    render = LatexTable(), 
                    file="D:\\World Bank\\Paper - Child mortality and Climate Shocks\\Outputs\\$(folder)version 1 SPI$months $(nameind) $(temp) $(extra).tex", 
                    order=order,
                )
            end
        end
    end

    function spi_temp_quadratic_regression(df, months, controls, times, folder, extra)

        #################################################################
        ### VERSION 2: spi + temp models (quadratic) ###
        #################################################################
        println("\rRunning Model VERSION 2: spi + temp models (quadratic) - SPI$(months)\r")

        for temp in ["std_t"]
            for ind in [("avg", "avg", "avg")]
                nameind = ind[1]
                spiind = ind[2]
                tind = ind[3]

                spi_previous = [] 
                temp_previous = []
                order_spi = []
                order_temp = []
                regs = []

                for time in 1:(length(times)-1)
                    time1 = times[time]
                    time2 = times[time + 1]

                    spi_actual = [Symbol("spi$(months)_$(time2)_$(spiind)"), Symbol("spi$(months)_$(time2)_$(spiind)_sq")]
                    temp_actual = [Symbol("$(temp)_$(time2)_$(spiind)"), Symbol("$(temp)_$(time2)_$(spiind)_sq")]
                    
                    if time==1
                        spi_start = [Symbol("spi$(months)_$(time1)_$(tind)"), Symbol("spi$(months)_$(time1)_$(tind)_sq")]
                        temp_start = [Symbol("$(temp)_$(time1)_$(tind)"), Symbol("$(temp)_$(time1)_$(tind)_sq")]
                        append!(order_spi, spi_start)
                        append!(order_temp, temp_start)
                        append!(spi_previous, spi_start)
                        append!(temp_previous, temp_start)
                    else
                        # only children that survived
                        df_temp = filter(row -> row[Symbol("child_agedeath_$(time1)")] == 0, df)
                    end

                    for i in 1:4
                        fixed_effects = fe(Symbol("ID_cell$i"))&term(:chb_year) + fe(Symbol("ID_cell$i"))&fe(:chb_month)
                        reg_model = reg(
                            df, 
                            term(Symbol("child_agedeath_$(time2)")) ~ sum(term.(spi_previous)) + sum(term.(spi_actual))  + sum(term.(temp_previous)) + sum(term.(temp_actual)) + sum(controls) + fixed_effects, 
                            Vcov.cluster(Symbol("ID_cell$i")), 
                            weights = :hv005,
                            method=:CUDA
                        )
                        push!(regs, reg_model)
                    end
                    append!(spi_previous, spi_actual)
                    append!(temp_previous, temp_actual)

                    append!(order_spi, spi_actual)
                    append!(order_temp, temp_actual)
                end
                println(regs)
                # Generate regression table
                order = vcat(order_spi, order_temp)
                order = [string(sym) for sym in order]
                regtable(
                    regs...; 
                    render = AsciiTable(), 
                    file="D:\\World Bank\\Paper - Child mortality and Climate Shocks\\Outputs\\$(folder)version 2 SPI$(months) $(nameind) $(temp) $(extra).txt", 
                    order=order,
                )
                regtable(
                    regs...; 
                    render = LatexTable(), 
                    file="D:\\World Bank\\Paper - Child mortality and Climate Shocks\\Outputs\\$(folder)version 2 SPI$(months) $(nameind) $(temp) $(extra).tex", 
                    order=order,
                )

            end
        end
    end


    function spi_temp_quadratic_dummy_regression(df, months, controls, times, folder, extra)
        #################################################################
        ### VERSION 3: spi + temp models (quadratic dummy) ###
        #################################################################
        println("\rRunning Model VERSION 3: spi + temp models (quadratic different for pos and neg) - SPI$(months)\r")

        for temp in ["std_t"]
            for ind in [("avg", "avg", "avg")]
                nameind = ind[1]
                spiind = ind[2]
                tind = ind[3]

                spi_previous = [] 
                temp_previous = []
                order_spi = []
                order_temp = []
                regs = []

                for time in 1:(length(times)-1)
                    time1 = times[time]
                    time2 = times[time + 1]

                    spi_actual = [Symbol("spi$(months)_$(time2)_$(spiind)"), Symbol("spi$(months)_$(time2)_$(spiind)_sq_neg"), Symbol("spi$(months)_$(time2)_$(spiind)_sq_pos")]
                    temp_actual = [Symbol("$(temp)_$(time2)_$(spiind)"), Symbol("$(temp)_$(time2)_$(spiind)_sq_neg"), Symbol("$(temp)_$(time2)_$(spiind)_sq_pos")]
                    
                    if time==1
                        spi_start = [Symbol("spi$(months)_$(time1)_$(tind)"), Symbol("spi$(months)_$(time1)_$(tind)_sq_neg"), Symbol("spi$(months)_$(time1)_$(tind)_sq_pos")]
                        temp_start = [Symbol("$(temp)_$(time1)_$(tind)"), Symbol("$(temp)_$(time1)_$(tind)_sq_neg"), Symbol("$(temp)_$(time1)_$(tind)_sq_pos")]
                        append!(order_spi, spi_start)
                        append!(order_temp, temp_start)
                        append!(spi_previous, spi_start)
                        append!(temp_previous, temp_start)
                    else
                        # only children that survived
                        df_temp = filter(row -> row[Symbol("child_agedeath_$(time1)")] == 0, df)
                    end

                    for i in 1:4
                        fixed_effects = fe(Symbol("ID_cell$i"))&term(:chb_year) + fe(Symbol("ID_cell$i"))&fe(:chb_month)
                        reg_model = reg(
                            df, 
                            term(Symbol("child_agedeath_$(time2)")) ~ sum(term.(spi_previous)) + sum(term.(spi_actual))  + sum(term.(temp_previous)) + sum(term.(temp_actual)) + sum(controls) + fixed_effects, 
                            Vcov.cluster(Symbol("ID_cell$i")), 
                            weights = :hv005,
                            method=:CUDA
                        )
                        push!(regs, reg_model)
                    end
                    append!(spi_previous, spi_actual)
                    append!(temp_previous, temp_actual)
                    append!(order_spi, spi_actual)
                    append!(order_temp, temp_actual)
                end
                                    
                # Generate regression table
                order = vcat(order_spi, order_temp)
                order = [string(sym) for sym in order]
                regtable(
                    regs...; 
                    render = AsciiTable(), 
                    file="D:\\World Bank\\Paper - Child mortality and Climate Shocks\\Outputs\\$(folder)version 3 SPI$(months) $(nameind) $(temp) $(extra).txt", 
                    order=order,
                )
                regtable(
                    regs...; 
                    render = LatexTable(), 
                    file="D:\\World Bank\\Paper - Child mortality and Climate Shocks\\Outputs\\$(folder)version 3 SPI$(months) $(nameind) $(temp) $(extra).tex", 
                    order=order,
                )


            end
        end
    end

    
    function run_models(df, controls, folder, extra)
        println("\rRunning Standard Models for $(folder)\r")
        for months in ["1", "3", "6", "9", "12"]
            i = 1
            extra_original = extra
            for times in (["inutero", "30d", "2m12m"], ["inutero", "1m3m", "4m12m"], ["inutero", "1m12m"])
                extra_with_time = extra_original * " - times$(i)"
                CustomModels.spi_temp_linear_regression(df, months, controls, times, folder, extra_with_time)
                CustomModels.spi_temp_quadratic_regression(df, months, controls, times, folder, extra_with_time)
                CustomModels.spi_temp_quadratic_dummy_regression(df, months, controls, times, folder, extra_with_time)
                i += 1
            end
        end
    end
    
end