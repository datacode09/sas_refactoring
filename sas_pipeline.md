Here’s a **SAS template** for a **composable pipeline** that implements modular, reusable components and is driven by configurations. The template supports **data extraction**, **transformation**, **validation**, and **loading**.

---

### **Composable Pipeline Template**

#### **1. Directory Structure**
```
composable_pipeline/
├── configs/
│   ├── table1_config.json
│   └── table2_config.json
├── macros/
│   ├── extract.sas
│   ├── transform.sas
│   ├── validate.sas
│   ├── load.sas
│   └── utils.sas
├── logs/
│   └── pipeline_log.txt
└── main.sas
```

---

#### **2. Components (SAS Macros)**

##### **`macros/extract.sas`**
```sas
%macro extract_data(source, target, format=csv);
    %if "&format" = "csv" %then %do;
        proc import datafile="&source" out=&target dbms=csv replace;
            getnames=yes;
        run;
    %end;
    %else %do;
        /* Add support for other formats like Excel, JSON, etc. */
        %put ERROR: Unsupported format "&format".;
    %end;
%mend;
```

---

##### **`macros/transform.sas`**
```sas
%macro join_data(left, right, join_col, target, join_type=inner);
    proc sql;
        create table &target as
        select a.*, b.*
        from &left as a
        &join_type join &right as b
        on a.&join_col = b.&join_col;
    quit;
%mend;

%macro filter_data(source, condition, target);
    data &target;
        set &source;
        if &condition;
    run;
%mend;
```

---

##### **`macros/validate.sas`**
```sas
%macro validate_schema(source, expected_columns);
    proc contents data=&source out=work._schema noprint; run;
    data _null_;
        set work._schema;
        array expected[&expected_columns];
        /* Compare schema columns with expected_columns */
        /* Log mismatches or errors */
    run;
%mend;
```

---

##### **`macros/load.sas`**
```sas
%macro save_data(source, target, format=parquet, mode=overwrite);
    %if "&format" = "parquet" %then %do;
        proc export data=&source outfile="&target" dbms=parquet replace;
        run;
    %end;
    %else %do;
        %put ERROR: Unsupported format "&format".;
    %end;
%mend;
```

---

##### **`macros/utils.sas`**
```sas
%macro log_message(message);
    data _null_;
        file "logs/pipeline_log.txt" mod;
        put datetime() ": " "&message";
    run;
%mend;
```

---

#### **3. Configuration File Example**

##### **`configs/table1_config.json`**
```json
{
    "extract": [
        {"source": "data/schema1.csv", "target": "work.schema1"},
        {"source": "data/schema2.csv", "target": "work.schema2"}
    ],
    "transform": [
        {"type": "join", "params": {"left": "work.schema1", "right": "work.schema2", "join_col": "id", "target": "work.joined_data"}},
        {"type": "filter", "params": {"source": "work.joined_data", "condition": "amount > 1000", "target": "work.filtered_data"}}
    ],
    "validate": {
        "source": "work.filtered_data",
        "expected_columns": ["id", "amount", "date"]
    },
    "load": {
        "source": "work.filtered_data",
        "target": "output/table1.parquet",
        "format": "parquet"
    }
}
```

---

#### **4. Main Pipeline Script**

##### **`main.sas`**
```sas
%include "macros/extract.sas";
%include "macros/transform.sas";
%include "macros/validate.sas";
%include "macros/load.sas";
%include "macros/utils.sas";

%macro run_pipeline(config_file);

    /* Step 1: Read configuration */
    filename config "&config_file";
    proc json out=work.config json=config;
    run;

    /* Step 2: Extract Data */
    data _null_;
        set work.config (where=(path="extract"));
        call execute(cats('%extract_data(source=', value.source, 
                          ', target=', value.target, ');'));
    run;

    /* Step 3: Transform Data */
    data _null_;
        set work.config (where=(path="transform"));
        if value.type = "join" then
            call execute(cats('%join_data(left=', value.params.left,
                              ', right=', value.params.right,
                              ', join_col=', value.params.join_col,
                              ', target=', value.params.target, ');'));
        else if value.type = "filter" then
            call execute(cats('%filter_data(source=', value.params.source,
                              ', condition=', value.params.condition,
                              ', target=', value.params.target, ');'));
    run;

    /* Step 4: Validate Data */
    data _null_;
        set work.config (where=(path="validate"));
        call execute(cats('%validate_schema(source=', value.source,
                          ', expected_columns=', value.expected_columns, ');'));
    run;

    /* Step 5: Load Data */
    data _null_;
        set work.config (where=(path="load"));
        call execute(cats('%save_data(source=', value.source,
                          ', target=', value.target,
                          ', format=', value.format, ');'));
    run;

    %log_message(Pipeline execution completed successfully);

%mend;

/* Example Execution */
%run_pipeline("configs/table1_config.json");
```

---

### **Features of This Template**
1. **Modularity**:
   - Each pipeline step is implemented as a macro (e.g., `extract_data`, `join_data`).
   - Steps can be reused across multiple pipelines.

2. **Configuration-Driven**:
   - JSON config files define pipeline behavior dynamically.
   - New pipelines can be created by editing configurations without modifying code.

3. **Observability**:
   - Logs messages to a centralized file (`pipeline_log.txt`).
   - Can be extended to include lineage tracking.

4. **Extensibility**:
   - Additional steps (e.g., feature engineering, ML training) can be added by defining new macros.

5. **Scalability**:
   - Suitable for small to medium data volumes in SAS environments.


