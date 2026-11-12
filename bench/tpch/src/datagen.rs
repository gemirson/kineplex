use anyhow::Result;
use arrow_array::builder::{Float64Builder, Int32Builder, Int64Builder, StringBuilder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use std::fs::{create_dir_all, File};
use std::path::Path;
use std::sync::Arc;

pub struct TpchDataGenerator {
    pub scale_factor: u64,
    pub output_dir: String,
    pub laguerre_partition: bool,
}

impl TpchDataGenerator {
    pub fn new(scale_factor: u64, output_dir: &str, laguerre_partition: bool) -> Self {
        Self {
            scale_factor,
            output_dir: output_dir.to_string(),
            laguerre_partition,
        }
    }

    pub fn generate_all_tables(&self) -> Result<()> {
        create_dir_all(&self.output_dir)?;

        self.generate_nation()?;
        self.generate_region()?;
        self.generate_customer()?;
        self.generate_part()?;
        self.generate_supplier()?;
        self.generate_partsupp()?;
        self.generate_orders()?;
        self.generate_lineitem()?;

        Ok(())
    }

    pub fn generate_nation(&self) -> Result<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("n_nationkey", DataType::Int32, false),
            Field::new("n_name", DataType::Utf8, false),
            Field::new("n_regionkey", DataType::Int32, false),
            Field::new("n_comment", DataType::Utf8, true),
        ]));

        let mut key_builder = Int32Builder::new();
        let mut name_builder = StringBuilder::new();
        let mut region_builder = Int32Builder::new();
        let mut comment_builder = StringBuilder::new();

        let nations = [
            (0, "ALGERIA", 0), (1, "ARGENTINA", 1), (2, "BRAZIL", 1),
            (3, "CANADA", 1), (4, "EGYPT", 4), (5, "ETHIOPIA", 0),
            (6, "FRANCE", 3), (7, "GERMANY", 3), (8, "INDIA", 2),
            (9, "INDONESIA", 2), (10, "IRAN", 4), (11, "IRAQ", 4),
            (12, "JAPAN", 2), (13, "JORDAN", 4), (14, "KENYA", 0),
            (15, "MOROCCO", 0), (16, "MOZAMBIQUE", 0), (17, "PERU", 1),
            (18, "CHINA", 2), (19, "ROMANIA", 3), (20, "SAUDI ARABIA", 4),
            (21, "VIETNAM", 2), (22, "RUSSIA", 3), (23, "UNITED KINGDOM", 3),
            (24, "UNITED STATES", 1),
        ];

        for (k, n, r) in &nations {
            key_builder.append_value(*k);
            name_builder.append_value(*n);
            region_builder.append_value(*r);
            comment_builder.append_value("standard comment");
        }

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(key_builder.finish()) as ArrayRef,
                Arc::new(name_builder.finish()) as ArrayRef,
                Arc::new(region_builder.finish()) as ArrayRef,
                Arc::new(comment_builder.finish()) as ArrayRef,
            ],
        )?;

        self.persist_table("nation", &batch)?;
        Ok(batch)
    }

    pub fn generate_region(&self) -> Result<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("r_regionkey", DataType::Int32, false),
            Field::new("r_name", DataType::Utf8, false),
            Field::new("r_comment", DataType::Utf8, true),
        ]));

        let mut key_builder = Int32Builder::new();
        let mut name_builder = StringBuilder::new();
        let mut comment_builder = StringBuilder::new();

        let regions = [
            (0, "AFRICA"), (1, "AMERICA"), (2, "ASIA"),
            (3, "EUROPE"), (4, "MIDDLE EAST"),
        ];

        for (k, r) in &regions {
            key_builder.append_value(*k);
            name_builder.append_value(*r);
            comment_builder.append_value("regular region comment");
        }

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(key_builder.finish()) as ArrayRef,
                Arc::new(name_builder.finish()) as ArrayRef,
                Arc::new(comment_builder.finish()) as ArrayRef,
            ],
        )?;

        self.persist_table("region", &batch)?;
        Ok(batch)
    }

    pub fn generate_customer(&self) -> Result<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("c_custkey", DataType::Int64, false),
            Field::new("c_name", DataType::Utf8, false),
            Field::new("c_address", DataType::Utf8, false),
            Field::new("c_nationkey", DataType::Int32, false),
            Field::new("c_phone", DataType::Utf8, false),
            Field::new("c_acctbal", DataType::Float64, false),
            Field::new("c_mktsegment", DataType::Utf8, false),
            Field::new("c_comment", DataType::Utf8, true),
        ]));

        let row_count = (150_000 * self.scale_factor.max(1)).min(50_000); // amostragem controlada para testes
        let mut custkey = Int64Builder::new();
        let mut name = StringBuilder::new();
        let mut address = StringBuilder::new();
        let mut nationkey = Int32Builder::new();
        let mut phone = StringBuilder::new();
        let mut acctbal = Float64Builder::new();
        let mut mktsegment = StringBuilder::new();
        let mut comment = StringBuilder::new();

        let segments = ["AUTOMOBILE", "BUILDING", "FURNITURE", "MACHINERY", "HOUSEHOLD"];

        for i in 1..=row_count {
            custkey.append_value(i as i64);
            name.append_value(format!("Customer#{:09}", i));
            address.append_value(format!("Address {}", i));
            nationkey.append_value((i % 25) as i32);
            phone.append_value(format!("11-{:03}-{:04}", (i * 7) % 900 + 100, (i * 13) % 9000 + 1000));
            acctbal.append_value(((i * 37) % 10000) as f64 - 1000.0);
            mktsegment.append_value(segments[(i % 5) as usize]);
            comment.append_value("customer remarks");
        }

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(custkey.finish()) as ArrayRef,
                Arc::new(name.finish()) as ArrayRef,
                Arc::new(address.finish()) as ArrayRef,
                Arc::new(nationkey.finish()) as ArrayRef,
                Arc::new(phone.finish()) as ArrayRef,
                Arc::new(acctbal.finish()) as ArrayRef,
                Arc::new(mktsegment.finish()) as ArrayRef,
                Arc::new(comment.finish()) as ArrayRef,
            ],
        )?;

        self.persist_table("customer", &batch)?;
        Ok(batch)
    }

    pub fn generate_part(&self) -> Result<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("p_partkey", DataType::Int64, false),
            Field::new("p_name", DataType::Utf8, false),
            Field::new("p_mfgr", DataType::Utf8, false),
            Field::new("p_brand", DataType::Utf8, false),
            Field::new("p_type", DataType::Utf8, false),
            Field::new("p_size", DataType::Int32, false),
            Field::new("p_container", DataType::Utf8, false),
            Field::new("p_retailprice", DataType::Float64, false),
            Field::new("p_comment", DataType::Utf8, true),
        ]));

        let row_count = (200_000 * self.scale_factor.max(1)).min(50_000);
        let mut partkey = Int64Builder::new();
        let mut name = StringBuilder::new();
        let mut mfgr = StringBuilder::new();
        let mut brand = StringBuilder::new();
        let mut p_type = StringBuilder::new();
        let mut size = Int32Builder::new();
        let mut container = StringBuilder::new();
        let mut retailprice = Float64Builder::new();
        let mut comment = StringBuilder::new();

        for i in 1..=row_count {
            partkey.append_value(i as i64);
            name.append_value(format!("part {:06}", i));
            mfgr.append_value(format!("Manufacturer#{}", (i % 5) + 1));
            brand.append_value(format!("Brand#{}{}", (i % 5) + 1, (i % 4) + 1));
            p_type.append_value("STANDARD BRASS");
            size.append_value(((i % 50) + 1) as i32);
            container.append_value("JUMBO BAG");
            retailprice.append_value(900.0 + ((i % 100) as f64));
            comment.append_value("part comments");
        }

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(partkey.finish()) as ArrayRef,
                Arc::new(name.finish()) as ArrayRef,
                Arc::new(mfgr.finish()) as ArrayRef,
                Arc::new(brand.finish()) as ArrayRef,
                Arc::new(p_type.finish()) as ArrayRef,
                Arc::new(size.finish()) as ArrayRef,
                Arc::new(container.finish()) as ArrayRef,
                Arc::new(retailprice.finish()) as ArrayRef,
                Arc::new(comment.finish()) as ArrayRef,
            ],
        )?;

        self.persist_table("part", &batch)?;
        Ok(batch)
    }

    pub fn generate_supplier(&self) -> Result<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("s_suppkey", DataType::Int64, false),
            Field::new("s_name", DataType::Utf8, false),
            Field::new("s_address", DataType::Utf8, false),
            Field::new("s_nationkey", DataType::Int32, false),
            Field::new("s_phone", DataType::Utf8, false),
            Field::new("s_acctbal", DataType::Float64, false),
            Field::new("s_comment", DataType::Utf8, true),
        ]));

        let row_count = (10_000 * self.scale_factor.max(1)).min(10_000);
        let mut suppkey = Int64Builder::new();
        let mut name = StringBuilder::new();
        let mut address = StringBuilder::new();
        let mut nationkey = Int32Builder::new();
        let mut phone = StringBuilder::new();
        let mut acctbal = Float64Builder::new();
        let mut comment = StringBuilder::new();

        for i in 1..=row_count {
            suppkey.append_value(i as i64);
            name.append_value(format!("Supplier#{:09}", i));
            address.append_value(format!("SuppAddress {}", i));
            nationkey.append_value((i % 25) as i32);
            phone.append_value(format!("22-{:03}-{:04}", (i * 11) % 900 + 100, (i * 17) % 9000 + 1000));
            acctbal.append_value(((i * 43) % 10000) as f64 - 500.0);
            comment.append_value("supplier notes");
        }

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(suppkey.finish()) as ArrayRef,
                Arc::new(name.finish()) as ArrayRef,
                Arc::new(address.finish()) as ArrayRef,
                Arc::new(nationkey.finish()) as ArrayRef,
                Arc::new(phone.finish()) as ArrayRef,
                Arc::new(acctbal.finish()) as ArrayRef,
                Arc::new(comment.finish()) as ArrayRef,
            ],
        )?;

        self.persist_table("supplier", &batch)?;
        Ok(batch)
    }

    pub fn generate_partsupp(&self) -> Result<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ps_partkey", DataType::Int64, false),
            Field::new("ps_suppkey", DataType::Int64, false),
            Field::new("ps_availqty", DataType::Int32, false),
            Field::new("ps_supplycost", DataType::Float64, false),
            Field::new("ps_comment", DataType::Utf8, true),
        ]));

        let row_count = (800_000 * self.scale_factor.max(1)).min(50_000);
        let mut partkey = Int64Builder::new();
        let mut suppkey = Int64Builder::new();
        let mut availqty = Int32Builder::new();
        let mut supplycost = Float64Builder::new();
        let mut comment = StringBuilder::new();

        for i in 1..=row_count {
            partkey.append_value(((i % 50_000) + 1) as i64);
            suppkey.append_value(((i % 10_000) + 1) as i64);
            availqty.append_value(((i % 1000) + 1) as i32);
            supplycost.append_value(((i % 500) as f64) + 1.0);
            comment.append_value("partsupp records");
        }

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(partkey.finish()) as ArrayRef,
                Arc::new(suppkey.finish()) as ArrayRef,
                Arc::new(availqty.finish()) as ArrayRef,
                Arc::new(supplycost.finish()) as ArrayRef,
                Arc::new(comment.finish()) as ArrayRef,
            ],
        )?;

        self.persist_table("partsupp", &batch)?;
        Ok(batch)
    }

    pub fn generate_orders(&self) -> Result<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("o_orderkey", DataType::Int64, false),
            Field::new("o_custkey", DataType::Int64, false),
            Field::new("o_orderstatus", DataType::Utf8, false),
            Field::new("o_totalprice", DataType::Float64, false),
            Field::new("o_orderdate", DataType::Utf8, false),
            Field::new("o_orderpriority", DataType::Utf8, false),
            Field::new("o_clerk", DataType::Utf8, false),
            Field::new("o_shippriority", DataType::Int32, false),
            Field::new("o_comment", DataType::Utf8, true),
        ]));

        let row_count = (1_500_000 * self.scale_factor.max(1)).min(60_000);
        let mut orderkey = Int64Builder::new();
        let mut custkey = Int64Builder::new();
        let mut orderstatus = StringBuilder::new();
        let mut totalprice = Float64Builder::new();
        let mut orderdate = StringBuilder::new();
        let mut orderpriority = StringBuilder::new();
        let mut clerk = StringBuilder::new();
        let mut shippriority = Int32Builder::new();
        let mut comment = StringBuilder::new();

        let priorities = ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"];

        for i in 1..=row_count {
            orderkey.append_value(i as i64);
            custkey.append_value(((i % 50_000) + 1) as i64);
            orderstatus.append_value(if i % 2 == 0 { "O" } else { "F" });
            totalprice.append_value(((i * 73) % 200_000) as f64);
            orderdate.append_value(format!("1995-{:02}-{:02}", (i % 12) + 1, (i % 28) + 1));
            orderpriority.append_value(priorities[(i % 5) as usize]);
            clerk.append_value(format!("Clerk#{:09}", (i % 1000) + 1));
            shippriority.append_value(0);
            comment.append_value("orders comments");
        }

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(orderkey.finish()) as ArrayRef,
                Arc::new(custkey.finish()) as ArrayRef,
                Arc::new(orderstatus.finish()) as ArrayRef,
                Arc::new(totalprice.finish()) as ArrayRef,
                Arc::new(orderdate.finish()) as ArrayRef,
                Arc::new(orderpriority.finish()) as ArrayRef,
                Arc::new(clerk.finish()) as ArrayRef,
                Arc::new(shippriority.finish()) as ArrayRef,
                Arc::new(comment.finish()) as ArrayRef,
            ],
        )?;

        self.persist_table("orders", &batch)?;
        Ok(batch)
    }

    pub fn generate_lineitem(&self) -> Result<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("l_orderkey", DataType::Int64, false),
            Field::new("l_partkey", DataType::Int64, false),
            Field::new("l_suppkey", DataType::Int64, false),
            Field::new("l_linenumber", DataType::Int32, false),
            Field::new("l_quantity", DataType::Float64, false),
            Field::new("l_extendedprice", DataType::Float64, false),
            Field::new("l_discount", DataType::Float64, false),
            Field::new("l_tax", DataType::Float64, false),
            Field::new("l_returnflag", DataType::Utf8, false),
            Field::new("l_linestatus", DataType::Utf8, false),
            Field::new("l_shipdate", DataType::Utf8, false),
            Field::new("l_commitdate", DataType::Utf8, false),
            Field::new("l_receiptdate", DataType::Utf8, false),
            Field::new("l_shipinstruct", DataType::Utf8, false),
            Field::new("l_shipmode", DataType::Utf8, false),
            Field::new("l_comment", DataType::Utf8, true),
        ]));

        let row_count = (6_000_000 * self.scale_factor.max(1)).min(100_000);
        let mut orderkey = Int64Builder::new();
        let mut partkey = Int64Builder::new();
        let mut suppkey = Int64Builder::new();
        let mut linenumber = Int32Builder::new();
        let mut quantity = Float64Builder::new();
        let mut extendedprice = Float64Builder::new();
        let mut discount = Float64Builder::new();
        let mut tax = Float64Builder::new();
        let mut returnflag = StringBuilder::new();
        let mut linestatus = StringBuilder::new();
        let mut shipdate = StringBuilder::new();
        let mut commitdate = StringBuilder::new();
        let mut receiptdate = StringBuilder::new();
        let mut shipinstruct = StringBuilder::new();
        let mut shipmode = StringBuilder::new();
        let mut comment = StringBuilder::new();

        let shipmodes = ["MAIL", "SHIP", "RAIL", "AIR", "FOB", "TRUCK", "REG AIR"];
        let returnflags = ["R", "A", "N"];

        for i in 1..=row_count {
            orderkey.append_value(((i % 60_000) + 1) as i64);
            partkey.append_value(((i % 50_000) + 1) as i64);
            suppkey.append_value(((i % 10_000) + 1) as i64);
            linenumber.append_value(((i % 4) + 1) as i32);
            let qty = ((i % 50) + 1) as f64;
            let extprice = qty * 900.0;
            let disc = ((i % 10) as f64) * 0.01;
            let tx = ((i % 8) as f64) * 0.01;

            quantity.append_value(qty);
            extendedprice.append_value(extprice);
            discount.append_value(disc);
            tax.append_value(tx);
            returnflag.append_value(returnflags[(i % 3) as usize]);
            linestatus.append_value(if i % 2 == 0 { "O" } else { "F" });
            shipdate.append_value(format!("1995-{:02}-{:02}", (i % 12) + 1, (i % 28) + 1));
            commitdate.append_value(format!("1995-{:02}-{:02}", (i % 12) + 1, (i % 28) + 1));
            receiptdate.append_value(format!("1995-{:02}-{:02}", (i % 12) + 1, (i % 28) + 1));
            shipinstruct.append_value("DELIVER IN PERSON");
            shipmode.append_value(shipmodes[(i % 7) as usize]);
            comment.append_value("lineitem description");
        }

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(orderkey.finish()) as ArrayRef,
                Arc::new(partkey.finish()) as ArrayRef,
                Arc::new(suppkey.finish()) as ArrayRef,
                Arc::new(linenumber.finish()) as ArrayRef,
                Arc::new(quantity.finish()) as ArrayRef,
                Arc::new(extendedprice.finish()) as ArrayRef,
                Arc::new(discount.finish()) as ArrayRef,
                Arc::new(tax.finish()) as ArrayRef,
                Arc::new(returnflag.finish()) as ArrayRef,
                Arc::new(linestatus.finish()) as ArrayRef,
                Arc::new(shipdate.finish()) as ArrayRef,
                Arc::new(commitdate.finish()) as ArrayRef,
                Arc::new(receiptdate.finish()) as ArrayRef,
                Arc::new(shipinstruct.finish()) as ArrayRef,
                Arc::new(shipmode.finish()) as ArrayRef,
                Arc::new(comment.finish()) as ArrayRef,
            ],
        )?;

        self.persist_table("lineitem", &batch)?;
        Ok(batch)
    }

    fn persist_table(&self, name: &str, batch: &RecordBatch) -> Result<()> {
        create_dir_all(&self.output_dir)?;
        let file_path = Path::new(&self.output_dir).join(format!("{name}.parquet"));
        let file = File::create(file_path)?;
        let props = parquet::file::properties::WriterProperties::builder()
            .set_compression(parquet::basic::Compression::ZSTD(
                parquet::basic::ZstdLevel::try_new(3).unwrap(),
            ))
            .build();
        let mut writer = parquet::arrow::arrow_writer::ArrowWriter::try_new(
            file,
            batch.schema(),
            Some(props),
        )?;
        writer.write(batch)?;
        writer.close()?;
        Ok(())
    }
}
