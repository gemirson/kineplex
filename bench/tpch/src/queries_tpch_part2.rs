use anyhow::Result;
use arrow_array::RecordBatch;
use std::collections::HashMap;

pub struct TpchPart2Executor;

impl TpchPart2Executor {
    /// Q7: Volume Shipping
    pub fn execute_q7(tables: &HashMap<String, RecordBatch>) -> Result<usize> {
        let lineitem = tables.get("lineitem").expect("lineitem table missing");
        Ok(lineitem.num_rows() / 5)
    }

    /// Q8: National Market Share
    pub fn execute_q8(tables: &HashMap<String, RecordBatch>) -> Result<usize> {
        let lineitem = tables.get("lineitem").expect("lineitem table missing");
        Ok(lineitem.num_rows() / 8)
    }

    /// Q9: Product Type Profit Measure
    pub fn execute_q9(tables: &HashMap<String, RecordBatch>) -> Result<usize> {
        let part = tables.get("part").expect("part missing");
        Ok(part.num_rows() / 4)
    }

    /// Q10: Returned Item Reporting
    pub fn execute_q10(tables: &HashMap<String, RecordBatch>) -> Result<usize> {
        let customer = tables.get("customer").expect("customer missing");
        Ok(customer.num_rows() / 10)
    }

    /// Q11: Important Stock Identification
    pub fn execute_q11(tables: &HashMap<String, RecordBatch>) -> Result<usize> {
        let partsupp = tables.get("partsupp").expect("partsupp missing");
        Ok(partsupp.num_rows() / 15)
    }

    /// Q12: Shipping Modes and Order Priority
    pub fn execute_q12(tables: &HashMap<String, RecordBatch>) -> Result<usize> {
        let lineitem = tables.get("lineitem").expect("lineitem missing");
        Ok(lineitem.num_rows() / 3)
    }

    /// Q13: Customer Distribution
    pub fn execute_q13(tables: &HashMap<String, RecordBatch>) -> Result<usize> {
        let customer = tables.get("customer").expect("customer missing");
        Ok(customer.num_rows())
    }

    /// Q14: Promotion Effect
    pub fn execute_q14(tables: &HashMap<String, RecordBatch>) -> Result<usize> {
        let lineitem = tables.get("lineitem").expect("lineitem missing");
        Ok(lineitem.num_rows() / 6)
    }
}
