use std::sync::Arc;
use std::collections::HashSet;

use arrow::array::{Array, BooleanArray, Int64Array, Float64Array,
    Date32Array, TimestampNanosecondArray};
use arrow::compute::filter;
use arrow::compute::kernels::cmp::{gt, eq, lt, gt_eq, lt_eq, neq};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use rayon::iter::{
    IntoParallelRefIterator,
    IndexedParallelIterator,
    IntoParallelIterator,
    ParallelIterator,
};

use crate::handler::handler::{ColumnFilter};

pub fn process_filter(
    record_batch: &RecordBatch,
    cols: &Vec<String>,
    filterlogic: &str,
    columns_filters: &Vec<ColumnFilter>
) -> RecordBatch {
    let filtering_mask: BooleanArray;
    let schema = record_batch.schema();

    if columns_filters.len() > 0 {
        let data_type_options = vec!["IN", "FL", "DA", "DT"];
        filtering_mask = columns_filters
            .iter()
            .filter(|item| {
                data_type_options.contains(&item.data_type.as_str()) &&
                    schema.field_with_name(&item.col).is_ok()
            })
            .map(|item| {
                let value_array = record_batch.column_by_name(&item.col)
                    .expect("Can not access to a col of the record bacth.");
                let bool_arr = match item.data_type.as_str() {
                    "IN" => filter_int(&value_array, &item.value, &item.filter_type),
                    "FL" => filter_float(&value_array, &item.value, &item.filter_type),
                    "DA" => filter_date(&value_array, &item.value, &item.filter_type),
                    "DT" => filter_datetime(&value_array, &item.value, &item.filter_type),
                    _ => {
                        if filterlogic == "AND" {
                            BooleanArray::from(vec![true; record_batch.num_rows()])
                        } else {
                            BooleanArray::from(vec![false; record_batch.num_rows()])
                        }
                    }
                };
                return bool_arr;
            })
            .reduce(|m1, m2| {
                if filterlogic == "AND" {
                    return (m1.values() & m2.values()).into();
                } else {
                    return (m1.values() | m2.values()).into();
                }
            }).unwrap();
    } else {
        filtering_mask = BooleanArray::from(vec![true; record_batch.num_rows()]);
    }

    let new_record_batch: RecordBatch;
    let new_schema: Schema;
    if cols.len() > 0 {
        let cols_set: HashSet<String> = cols.clone().into_iter().collect();
        let filtered_columns = record_batch
            .columns()
            .par_iter()
            .zip(schema.fields().par_iter())
            .into_par_iter()
            .filter(|(_, field)| {
                cols_set.contains(field.name())
            })
            .map(|(column, _)| {
                filter(column.as_ref(), &filtering_mask).unwrap()
            })
            .collect::<Vec<_>>();

        let fields: Vec<Field> = schema
            .fields()
            .par_iter()
            .filter(|field| {
                cols_set.contains(field.name())
            })
            .map(|field| {
                schema.field_with_name(field.name()).expect("Can not access to a col of the schema").clone()
            })
            .collect::<Vec<_>>();

        new_schema = Schema::new_with_metadata(fields, schema.metadata().clone());
        new_record_batch = RecordBatch::try_new(Arc::new(new_schema), filtered_columns).unwrap();
    } else {
        let filtered_columns = record_batch
            .columns()
            .par_iter()
            .map(|column| {
                filter(column.as_ref(), &filtering_mask).unwrap()
            })
            .collect::<Vec<_>>();

        new_record_batch = RecordBatch::try_new(schema, filtered_columns).unwrap();
    }
    return new_record_batch;
}

fn filter_int(value_array: &Arc<dyn Array>, value: &f64, filter_type: &String) -> BooleanArray {
    let filter_arr = Int64Array::from(vec![*value as i64; value_array.len()]);
    let mask: BooleanArray;
    if filter_type == "gt" {
        mask = gt(value_array, &filter_arr).expect("Can not compare the arrays.");
    } else if filter_type == "eq" {
        mask = eq(value_array, &filter_arr).expect("Can not compare the arrays.");
    } else if filter_type == "lt" {
        mask = lt(value_array, &filter_arr).expect("Can not compare the arrays.");
    } else if filter_type == "gte" {
        mask = gt_eq(value_array, &filter_arr).expect("Can not compare the arrays.");
    } else if filter_type == "lte" {
        mask = lt_eq(value_array, &filter_arr).expect("Can not compare the arrays.");
    } else {
        mask = neq(value_array, &filter_arr).expect("Can not compare the arrays.");
    }
    return mask;
}

fn filter_float(value_array: &Arc<dyn Array>, value: &f64, filter_type: &String) -> BooleanArray {
    let filter_arr = Float64Array::from(vec![*value; value_array.len()]);
    let mask: BooleanArray;
    if filter_type == "gt" {
        mask = gt(value_array, &filter_arr).expect("Can not compare the arrays.");
    } else if filter_type == "eq" {
        mask = eq(value_array, &filter_arr).expect("Can not compare the arrays.");
    } else if filter_type == "lt" {
        mask = lt(value_array, &filter_arr).expect("Can not compare the arrays.");
    } else if filter_type == "gte" {
        mask = gt_eq(value_array, &filter_arr).expect("Can not compare the arrays.");
    } else if filter_type == "lte" {
        mask = lt_eq(value_array, &filter_arr).expect("Can not compare the arrays.");
    } else {
        mask = neq(value_array, &filter_arr).expect("Can not compare the arrays.");
    }
    return mask;
}

fn filter_date(value_array: &Arc<dyn Array>, value: &f64, filter_type: &String) -> BooleanArray {
    let filter_arr = Date32Array::from(vec![*value as i32; value_array.len()]);
    let mask: BooleanArray;
    if filter_type == "gt" {
        mask = gt(value_array, &filter_arr).expect("Can not compare the arrays.");
    } else if filter_type == "eq" {
        mask = eq(value_array, &filter_arr).expect("Can not compare the arrays.");
    } else if filter_type == "lt" {
        mask = lt(value_array, &filter_arr).expect("Can not compare the arrays.");
    } else if filter_type == "gte" {
        mask = gt_eq(value_array, &filter_arr).expect("Can not compare the arrays.");
    } else if filter_type == "lte" {
        mask = lt_eq(value_array, &filter_arr).expect("Can not compare the arrays.");
    } else {
        mask = neq(value_array, &filter_arr).expect("Can not compare the arrays.");
    }
    return mask;
}

fn filter_datetime(value_array: &Arc<dyn Array>, value: &f64, filter_type: &String) -> BooleanArray {
    let filter_arr = TimestampNanosecondArray::from(vec![*value as i64; value_array.len()]);
    let mask: BooleanArray;
    if filter_type == "gt" {
        mask = gt(value_array, &filter_arr).expect("Can not compare the arrays.");
    } else if filter_type == "eq" {
        mask = eq(value_array, &filter_arr).expect("Can not compare the arrays.");
    } else if filter_type == "lt" {
        mask = lt(value_array, &filter_arr).expect("Can not compare the arrays.");
    } else if filter_type == "gte" {
        mask = gt_eq(value_array, &filter_arr).expect("Can not compare the arrays.");
    } else if filter_type == "lte" {
        mask = lt_eq(value_array, &filter_arr).expect("Can not compare the arrays.");
    } else {
        mask = neq(value_array, &filter_arr).expect("Can not compare the arrays.");
    }
    return mask;
}
