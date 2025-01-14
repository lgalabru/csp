/// Returns a String query fragment useful for mass insertions of data in a single query.
/// For example, rows = 2 and columns = 3 returns "(($1, $2, $3), ($4, $5, $6))"
pub fn multi_row_query_param_str(rows: usize, columns: usize) -> String {
    let mut arg_num = 1;
    let mut arg_str = String::new();
    for _ in 0..rows {
        arg_str.push_str("(");
        for i in 0..columns {
            arg_str.push_str(format!("${},", arg_num + i).as_str());
        }
        arg_str.pop();
        arg_str.push_str("),");
        arg_num += columns;
    }
    arg_str.pop();
    arg_str
}
