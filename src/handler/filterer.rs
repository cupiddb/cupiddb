use std::sync::Arc;
use std::collections::HashSet;

use arrow::array::{self, Array, BooleanArray};
use arrow::compute::filter;
use arrow::compute::kernels::cmp::{gt, eq, lt, gt_eq, lt_eq, neq};
use arrow::datatypes::{Field, Schema, DataType, TimeUnit};
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
        let data_type_options = vec!["IN", "FL", "DA", "DT", "ST", "BL"];
        filtering_mask = columns_filters
            .iter()
            .filter(|item| {
                data_type_options.contains(&item.data_type.as_str()) &&
                    schema.field_with_name(&item.col).is_ok()
            })
            .map(|item| {
                let data_array = record_batch.column_by_name(&item.col)
                    .expect("Can not access to a col of the record bacth.");
                let data_len = data_array.len();

                let bool_arr = match data_array.data_type() {
                    DataType::Int64 => {
                        let value = item.value_int.unwrap() as i64;
                        let filter_arr = array::Int64Array::from(vec![value; data_len]);
                        filter_array(&data_array, &filter_arr, &item.filter_type)
                    },
                    DataType::Int32 => {
                        let value = item.value_int.unwrap() as i32;
                        let filter_arr = array::Int32Array::from(vec![value; data_len]);
                        filter_array(&data_array, &filter_arr, &item.filter_type)
                    },
                    DataType::Int16 => {
                        let value = item.value_int.unwrap() as i16;
                        let filter_arr = array::Int16Array::from(vec![value; data_len]);
                        filter_array(&data_array, &filter_arr, &item.filter_type)
                    },
                    DataType::Int8 => {
                        let value = item.value_int.unwrap() as i8;
                        let filter_arr = array::Int8Array::from(vec![value; data_len]);
                        filter_array(&data_array, &filter_arr, &item.filter_type)
                    },
                    DataType::UInt64 => {
                        let value = item.value_int.unwrap() as u64;
                        let filter_arr = array::UInt64Array::from(vec![value; data_len]);
                        filter_array(&data_array, &filter_arr, &item.filter_type)
                    },
                    DataType::UInt32 => {
                        let value = item.value_int.unwrap() as u32;
                        let filter_arr = array::UInt32Array::from(vec![value; data_len]);
                        filter_array(&data_array, &filter_arr, &item.filter_type)
                    },
                    DataType::UInt16 => {
                        let value = item.value_int.unwrap() as u16;
                        let filter_arr = array::UInt16Array::from(vec![value; data_len]);
                        filter_array(&data_array, &filter_arr, &item.filter_type)
                    },
                    DataType::UInt8 => {
                        let value = item.value_int.unwrap() as u8;
                        let filter_arr = array::UInt8Array::from(vec![value; data_len]);
                        filter_array(&data_array, &filter_arr, &item.filter_type)
                    },
                    DataType::Float64 => {
                        let value = item.value_flt.unwrap();
                        let filter_arr = array::Float64Array::from(vec![value; data_len]);
                        filter_array(&data_array, &filter_arr, &item.filter_type)
                    },
                    DataType::Float32 => {
                        let value = item.value_flt.unwrap() as f32;
                        let filter_arr = array::Float32Array::from(vec![value; data_len]);
                        filter_array(&data_array, &filter_arr, &item.filter_type)
                    },
                    DataType::Boolean => {
                        let value = item.value_bol.unwrap();
                        let filter_arr = array::BooleanArray::from(vec![value; data_len]);
                        filter_array(&data_array, &filter_arr, &item.filter_type)
                    }
                    DataType::Utf8 => {
                        let value = item.value_str.clone().unwrap();
                        let filter_arr = array::StringArray::from(vec![value; data_len]);
                        filter_array(&data_array, &filter_arr, &item.filter_type)
                    },
                    DataType::Date32 => {
                        let value = item.value_int.unwrap() as i32;
                        let filter_arr = array::Date32Array::from(vec![value; data_len]);
                        filter_array(&data_array, &filter_arr, &item.filter_type)
                    },
                    DataType::Timestamp(TimeUnit::Nanosecond, ..) => {
                        let value = item.value_int.unwrap() as i64;
                        let filter_arr = array::TimestampNanosecondArray::from(vec![value; data_len]);
                        filter_array(&data_array, &filter_arr, &item.filter_type)
                    },
                    _ => { panic!("Not implemented for data type") }
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

fn filter_array(data_array: &Arc<dyn Array>, filter_value: &dyn Array, filter_type: &String) -> BooleanArray {
    let mask: BooleanArray;
    if filter_type == "gt" {
        mask = gt(data_array, &filter_value).unwrap();
    } else if filter_type == "eq" {
        mask = eq(data_array, &filter_value).unwrap();
    } else if filter_type == "lt" {
        mask = lt(data_array, &filter_value).unwrap();
    } else if filter_type == "gte" {
        mask = gt_eq(data_array, &filter_value).unwrap();
    } else if filter_type == "lte" {
        mask = lt_eq(data_array, &filter_value).unwrap();
    } else {
        mask = neq(data_array, &filter_value).unwrap();
    }
    return mask;
}
