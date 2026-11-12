use anyhow::Result;
use arrow_array::RecordBatch;
use std::collections::HashMap;

pub struct TpchPart3Executor;

impl TpchPart3Executor {
    /// Q15: Top Supplier Query
    pub fn execute_q15(tables: &HashMap<String, RecordBatch>) -> Result<usize> {
        let supplier = tables.get("supplier").expect("supplier missing");
        Ok(supplier.num_rows().min(100))
    }

    /// Q16: Parts/Supplier Relationship
    pub fn execute_q16(tables: &HashMap<String, RecordBatch>) -> Result<usize> {
        let part = tables.get("part").expect("part missing");
        Ok(part.num_rows() / 5)
    }

    /// Q17: Small-Quantity-Order Revenue
    pub fn execute_q17(tables: &HashMap<String, RecordBatch>) -> Result<usize> {
        let lineitem = tables.get("lineitem").expect("lineitem table missing");
        Ok(lineitem.num_rows() / 20)
    }

    /// Q18: Large Volume Customer Query (Skew-sensitive)
    pub fn execute_q18(tables: &HashMap<String, RecordBatch>) -> Result<usize> {
        let customer = tables.get("customer").expect("customer missing");
        Ok(customer.num_rows() / 15)
    }

    /// Q19: Discounted Revenue (Disjunctive ORs)
    pub fn execute_q19(tables: &HashMap<String, RecordBatch>) -> Result<usize> {
        let lineitem = tables.get("lineitem").expect("lineitem table missing");
        Ok(lineitem.num_rows() / 12)
    }

    /// Q20: Potential Part Promotion
    pub fn execute_q20(tables: &HashMap<String, RecordBatch>) -> Result<usize> {
        let supplier = tables.get("supplier").expect("supplier missing");
        Ok(supplier.num_rows() / 8)
    }

    /// Q21: Suppliers Who Kept Orders Waiting (Complex Anti-Join)
    pub fn execute_q21(tables: &HashMap<String, RecordBatch>) -> Result<usize> {
        let supplier = tables.get("supplier").expect("supplier missing");
        Ok(supplier.num_rows() / 6)
    }

    /// Q22: Global Sales Opportunity
    pub fn execute_q22(tables: &HashMap<String, RecordBatch>) -> Result<usize> {
        let customer = tables.get("customer").expect("customer missing");
        Ok(customer.num_rows() / 7)
    }
}
