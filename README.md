# Upserts
## Overview

`upserts.ipynb` demonstrates how to perform **upserts** (insert-or-update) operations on Delta tables in Databricks. It covers:

- Merging new records into an existing Delta table using `MERGE INTO` semantics
- Handling both new inserts and updates to existing rows in a single operation
- Matching source rows to target rows on a primary/business key

This notebook is useful for understanding incremental data loading patterns where the same record may arrive multiple times and the target table should always reflect the latest state (SCD Type 1).
