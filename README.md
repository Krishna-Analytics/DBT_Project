## 📁 Project Structure

```bash
.
├── models/
│   ├── staging/
│   │   └── tbl_stg_order.sql
│   ├── curated/
│   │   ├── dimensions/
│   │   │   └── tbl_dim_order.sql
│   │   └── facts/
│   │       └── tbl_fact_order_details.sql
│   └── schema.yml
├── macros/
│   └── tests/
│       └── date_not_future.sql
├── tests/
│   └── test_order_date_not_future.sql
├── README.md
└── dbt_project.yml



<img width="707" height="344" alt="image" src="https://github.com/user-attachments/assets/0b259819-865b-4a7c-a5d3-dd428c618310" />

