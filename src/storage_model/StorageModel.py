import os
import pandas as pd
import polars as pl
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
        spot_price_trace: pl.DataFrame,
        interval_length_hours: float=1/12
    ):
        """
        Args:
            spot_price_trace: pl.DataFrame
                interval_date: date
                period_id: short
                rrp: float
        Returns: pl.DataFrame
            interavl_date: date
            period_id: short
            rrp: float
            discharge_mw: float
            charge_mw: float
            effective_storage_level_mwh: float
        Note:
            Need to add degradation rate and would need ramp constraints for storage other than BESS.
        """

        if os.system("which cbc > /dev/null") != 0:
            cbc_bin_location = "/Volumes/exploration/earnings_forecast/bin/cbc"
            exit_code = os.system(f"cp {cbc_bin_location} /usr/bin/cbc")
            if exit_code != 0:
                raise Exception("Could not find 'cbc' binary and failed to copy binary from {cbc_bin_location}")

        spot_price_trace = (
            spot_price_trace
            .with_columns(
                index=pl.struct("interval_date", "period_id").rank(method="ordinal")
            )
            .sort(["interval_date", "period_id"])
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
        model.spot_price = pyo.Param(model.intervals, initialize=spot_price_trace["rrp"].to_pandas().to_dict())
    
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

        # Positive is for discharged energy
        model.metered_energy_mwh = pyo.Var(
            model.intervals,
            domain=pyo.Reals,
            bounds=(-self.capacity_mw * interval_length_hours, self.capacity_mw * interval_length_hours)
        )

        # This represents the storage level at the END of the interval
        model.effective_storage_level_mwh = pyo.Var(
            model.intervals,
            domain=pyo.NonNegativeReals,
            bounds=(0, self.capacity_mw * self.duration_hours)
        )

        # Constraints
        model.storage_tracking_constraint = pyo.Constraint(model.intervals, rule=storage_tracking_constraint)
        model.linear_ramp_constraint = pyo.Constraint(model.intervals, rule=linear_ramp_constraint)
        model.final_storage_level_constraint = pyo.Constraint(model.intervals, rule=final_storage_level_constraint)
        model.cycles_per_day_constraint = pyo.Constraint(rule=cycles_per_day_constraint)
        model.reduce_double_booking_constraint = pyo.Constraint(model.intervals, rule=reduce_double_booking_constraint)

        # Objective
        model.objective = pyo.Objective(rule=arbitrage_profit, sense=pyo.maximize)

        solver = pyo.SolverFactory("cbc")
        solution = solver.solve(model)

        result = (
            pl.concat(
                [
                    pl.DataFrame({
                        "index": var.extract_values().keys(),
                        str(var): var.extract_values().values()
                    }, schema={"index": pl.Int32(), str(var): pl.Float32()})
                    for var in model.component_objects(pyo.Var, active=True)
                ],
                how="align"
            )
            .join(
                spot_price_trace,
                "index"
            )
            .select(
                "interval_date",
                "period_id",
                "rrp",
                "charge_mw",
                "discharge_mw",
                "metered_energy_mwh",
                "effective_storage_level_mwh"
            )
        )

        return result

def arbitrage_profit(model):
    profit = (
        pyo.summation(model.spot_price, model.metered_energy_mwh) 
    ) * model.interval_length_hours
    return profit

def storage_tracking_constraint(model, interval):
    
    if interval == 0:
        # Enforces initial storage level constraint

        stored_energy_change_mwh = (
            model.charge_mw[interval] * model.round_trip_efficiency - 
            model.discharge_mw[interval]
        ) * model.interval_length_hours / 2

        return model.effective_storage_level_mwh[interval] == model.initial_storage_mwh + stored_energy_change_mwh
    else:
        stored_energy_change_mwh = (
            (model.charge_mw[interval] + model.charge_mw[interval-1]) * model.round_trip_efficiency - 
            (model.discharge_mw[interval] + model.discharge_mw[interval-1])
        ) * model.interval_length_hours / 2

        return model.effective_storage_level_mwh[interval] == model.effective_storage_level_mwh[interval-1] + stored_energy_change_mwh

def linear_ramp_constraint(model, interval):
    if interval == 0:
        # Assumes at rest at interval -1
        return model.metered_energy_mwh[interval] == (model.discharge_mw[interval] - model.charge_mw[interval]) * model.interval_length_hours / 2
    else:
        return model.metered_energy_mwh[interval] == (
            model.discharge_mw[interval] + model.discharge_mw[interval-1] - model.charge_mw[interval] - model.charge_mw[interval-1]
        ) * model.interval_length_hours / 2

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


