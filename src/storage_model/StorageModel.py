import os
import pandas as pd
import pyomo.environ as pyo
from math import ceil
from functools import reduce

class StorageModel():
    def __init__(
        self,
        capacity_mw: float,
        duration_hours: float,
        round_trip_efficiency: float,
        cycles_per_day_target: float=1.0,
        initial_storage_fraction: float=0.0,
        final_storage_fraction: float=0.0
    ):
        self.capacity_mw = capacity_mw
        self.duration_hours = duration_hours
        self.round_trip_efficiency = round_trip_efficiency
        self.cycles_per_day_target = cycles_per_day_target

        self.initial_storage_fraction = initial_storage_fraction
        self.final_storage_fraction = final_storage_fraction

    def solve(
        self,
        spot_price_trace: pd.DataFrame,
        interval_length_hours: float=1/12
    ):
        """
        Args:
            spot_price_trace: pd.DataFrame
                interval_date_time: datetime
                rrp: float
        Returns: pd.DataFrame
            interval_date_time: datetime
            rrp: float
            discharge_mw: float
            charge_mw: float
            effective_storage_level_mwh: float
        Note:
            Should add linear ramp constraint? Would also reduce erratic cycling?
            energy_change_mwh = interval_length_hours * (charge_mw[i] + charge_mw[i-1]) / 2

            Need to add degradation rate and would need ramp constraints for storage other than BESS.
        """

        if os.system("which cbc > /dev/null") != 0:
            cbc_bin_location = "/Volumes/exploration/earnings_forecast/bin/cbc"
            exit_code = os.system(f"cp {cbc_bin_location} /usr/bin/cbc")
            if exit_code != 0:
                raise Exception("Could not find 'cbc' binary and failed to copy binary from {cbc_bin_location}")

        spot_price_trace = (
            spot_price_trace
            .sort_values("interval_date_time")
            .reset_index(drop=True)
        )

        model = pyo.ConcreteModel()

        model.intervals = pyo.RangeSet(0, len(spot_price_trace) - 1)

        # Parameters
        model.capacity_mw = pyo.Param(initialize=self.capacity_mw)
        model.duration_hours = pyo.Param(initialize=self.duration_hours)
        model.effective_storage_mwh = pyo.Param(initialize=self.capacity_mw * self.duration_hours) # Dispatchable storage capacity
        model.round_trip_efficiency = pyo.Param(initialize=self.round_trip_efficiency)

        model.initial_storage_mwh = pyo.Param(initialize=self.capacity_mw * self.duration_hours * self.initial_storage_fraction)
        model.final_storage_mwh = pyo.Param(initialize=self.capacity_mw * self.duration_hours * self.final_storage_fraction)
        model.cycles_per_day_target = pyo.Param(initialize=self.cycles_per_day_target)

        model.interval_length_hours = pyo.Param(initialize=interval_length_hours)
        model.spot_price = pyo.Param(model.intervals, initialize=spot_price_trace["rrp"].to_dict())
    
        # Variables
        model.discharge_mw = pyo.Var(
            model.intervals,
            domain=pyo.NonNegativeReals,
            bounds=(0, self.capacity_mw)
        )

        model.charge_mw = pyo.Var(
            model.intervals,
            domain=pyo.NonNegativeReals,
            bounds=(0, self.capacity_mw)
        )

        # This represents the storage level at the END of the interval
        model.effective_storage_level_mwh = pyo.Var(
            model.intervals,
            domain=pyo.NonNegativeReals,
            bounds=(0, self.capacity_mw * self.duration_hours)
        )

        # Constraints
        model.storage_tracking_constraint = pyo.Constraint(model.intervals, rule=storage_tracking_constraint)
        model.final_storage_level_constraint = pyo.Constraint(model.intervals, rule=final_storage_level_constraint)
        model.cycles_per_day_constraint = pyo.Constraint(rule=cycles_per_day_constraint)
        model.reduce_double_booking_constraint = pyo.Constraint(model.intervals, rule=reduce_double_booking_constraint)

        # Objective
        model.objective = pyo.Objective(rule=arbitrage_profit, sense=pyo.maximize)

        solver = pyo.SolverFactory("cbc")
        solution = solver.solve(model)

        variables_result = [
            pd.DataFrame.from_dict(
                var.extract_values(),
                orient='index',
                columns=[str(var)]
            )
            for var in model.component_objects(pyo.Var, active=True)
        ]
        variables_result.append(spot_price_trace)
        result = reduce(lambda x, y: pd.merge(x, y, left_index=True,right_index=True), variables_result)
        
        return result

def arbitrage_profit(model):
    profit = (
        pyo.summation(model.spot_price, model.discharge_mw) 
        - pyo.summation(model.spot_price, model.charge_mw)
    ) * model.interval_length_hours
    return profit

def storage_tracking_constraint(model, interval):
    energy_change_mwh = (
        model.interval_length_hours * model.round_trip_efficiency * model.charge_mw[interval] - 
        model.interval_length_hours * model.discharge_mw[interval]
    )
    
    if interval == 0:
        # Enforces starting initial level constraint
        return model.effective_storage_level_mwh[interval] == model.initial_storage_mwh + energy_change_mwh
    else:
        return model.effective_storage_level_mwh[interval] == model.effective_storage_level_mwh[interval-1] + energy_change_mwh

def final_storage_level_constraint(model, interval):
    if interval == model.intervals.at(-1):
        return model.effective_storage_level_mwh[interval] == model.final_storage_mwh
    else:
        return pyo.Constraint.Feasible 
    
def cycles_per_day_constraint(model):
    return sum(
        (model.interval_length_hours * model.discharge_mw[i] / model.effective_storage_mwh)
        for i in model.intervals
    ) <= ceil(model.cycles_per_day_target * len(model.intervals) * model.interval_length_hours / 24)

def reduce_double_booking_constraint(model, interval):
    return model.discharge_mw[interval] + model.charge_mw[interval] <= model.capacity_mw


