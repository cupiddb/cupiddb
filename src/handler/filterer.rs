use polars::frame::DataFrame;
use polars_lazy::prelude::{col, Expr};
use polars_lazy::frame::IntoLazy;

use crate::handler::handler::{ColumnFilter};


fn produce_primative_expr(column_filter: &ColumnFilter) -> Expr{
    let mut expr: Expr = col(&column_filter.col);

    if column_filter.filter_type == "gte" {
        expr = expr.gt_eq(column_filter.value);
    } else if column_filter.filter_type == "gt" {
        expr = expr.gt(column_filter.value);
    } else if column_filter.filter_type == "lte" {
        expr = expr.lt_eq(column_filter.value);
    } else if column_filter.filter_type == "lt" {
        expr = expr.lt(column_filter.value);
    } else if column_filter.filter_type == "eq" {
        expr = expr.eq(column_filter.value);
    } else {
        expr = expr.neq(column_filter.value);
    }
    return expr;
}


fn produce_expr(column_filters: &Vec<ColumnFilter>, filterlogic: &str) -> Expr{
    let mut expr: Expr = produce_primative_expr(&column_filters[0]);

    for i in 1..column_filters.len(){
        if filterlogic == "AND" {
            expr = expr.and(produce_primative_expr(&column_filters[i]));
        } else {
            expr = expr.or(produce_primative_expr(&column_filters[i]));
        }
    }

    return expr;
}


pub fn process_filter(
    dataframe: &DataFrame,
    filterlogic: &str,
    column_filters: &Vec<ColumnFilter>
) -> DataFrame {
    if column_filters.len() == 0 {
        return dataframe.clone();
    }
    let expr: Expr = produce_expr(&column_filters, &filterlogic);
    let filtering = &dataframe.clone().lazy().filter(expr).collect();

    let new_dataframe: &DataFrame = match filtering{
        Ok(content) => { content },
        Err(e) => { eprintln!("{}", e); panic!("Can not filter") },
    };

    return new_dataframe.clone();
}
