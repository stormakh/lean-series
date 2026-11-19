//! Port of `number_data_points.go`: gauge and sum data points as float samples.

use model::exemplar;
use model::labels::METRIC_NAME;
use model::value::stale_nan;
use storage::{Context, Error};

use super::super::pdata::{NumberDataPoint, NumberValue};
use super::helper::{RESERVED_LABEL_NAMES, convert_time_stamp};
use super::metrics_to_prw::{AppOpts, PrometheusConverter, Settings};

fn number_value(pt: &NumberDataPoint) -> f64 {
    if pt.flags.no_recorded_value() {
        return stale_nan();
    }
    match pt.value {
        NumberValue::Int(v) => v as f64,
        NumberValue::Double(v) => v,
        NumberValue::Empty => 0.0,
    }
}

impl PrometheusConverter {
    /// Go `addGaugeNumberDataPoints`.
    pub fn add_gauge_number_data_points(
        &mut self,
        ctx: &Context,
        data_points: &[NumberDataPoint],
        settings: &Settings,
        app_opts: &AppOpts,
    ) -> Result<(), Error> {
        for pt in data_points {
            self.every_n.check_context(ctx)?;
            let labels = self.create_attributes(
                &pt.attributes,
                settings,
                RESERVED_LABEL_NAMES,
                true,
                &app_opts.metadata,
                &[METRIC_NAME, &app_opts.metric_family_name],
            )?;
            let val = number_value(pt);
            let ts = convert_time_stamp(pt.timestamp);
            let st = convert_time_stamp(pt.start_timestamp);
            self.append(&labels, st, ts, val, None, app_opts, &[])?;
        }
        Ok(())
    }

    /// Go `addSumNumberDataPoints`.
    pub fn add_sum_number_data_points(
        &mut self,
        ctx: &Context,
        data_points: &[NumberDataPoint],
        settings: &Settings,
        app_opts: &AppOpts,
    ) -> Result<(), Error> {
        for pt in data_points {
            self.every_n.check_context(ctx)?;
            let lbls = self.create_attributes(
                &pt.attributes,
                settings,
                RESERVED_LABEL_NAMES,
                true,
                &app_opts.metadata,
                &[METRIC_NAME, &app_opts.metric_family_name],
            )?;
            let val = number_value(pt);
            let ts = convert_time_stamp(pt.timestamp);
            let st = convert_time_stamp(pt.start_timestamp);
            let mut exemplars = self.get_prom_exemplars(ctx, &pt.exemplars)?;
            // Appenders need exemplars sorted by timestamp.
            exemplars.sort_by(|a, b| exemplar::compare(a, b).cmp(&0));
            self.append(&lbls, st, ts, val, None, app_opts, &exemplars)?;
        }
        Ok(())
    }
}
