# **ELT pipeline using descriptive and incremental data from the NASA NeoWs API**

## Abstract

The following program extracts data from the NASA NeoWs API collected by the [CNEOS](http://neo.jpl.nasa.gov) on asteroids and near-Earth objects (NEOs), such as their orbital and descriptive characteristics and their proximity to other stellar objects. In this case, the NeoWs API includes two endpoints from which related data can be extracted but with two different approaches: 'feed' gathers updated data about the approach of multiple NEOs over a specific period of time (with a limit up to 7 days), while in 'neo' descriptive data is collected given a single asteroid ID. Both extractions are tabulated into DataFrames, cleaned and transformed accordingly to be ultimately joined into a single Delta Table with relevant data on approaches and the approaching asteroids.

Aditionally, another endpoint from the API called 'browse' offers descriptive data about all the asteroids registered without requesting for them individually, but it was not implemented due to its scarce limit of entries per page, complicating pagination functions or request optimization tools to avoid exceptions such as HTTP 429 error (Too Many Requests).

## Architecture

### Extraction

The program is modularized because the set of all functions and aditional tools implemented specifically for the API's behaviour makes the main file excessively complex in terms of reading, storage, and testing capacity. Moreover, it is designed to make periodic requests and collect data predicted for the next 7 days inclusively, taking as the 'start_date' parameter the last date written in the stateful file or, in the lack thereof, the execution date, while taking the date after 7 days as the 'end_date' parameter. The application then loads the data of Earth-approaching asteroids in the selected date range along with their measurable attributes (which are updated but unfrequently) in Delta Lake format for further processing. These steps imply that the pipeline follows an ELT structure and that the full extraction depends on the incremental fashion extraction. Given this, in the `get_data()` extraction function an asynchronous request method is used for the 'feed' endpoint that requires individual IDs, so that multiple and continuous requests can be made effectively. In case some specific request fails, it is skipped to continue with the next one.

### Loading

The data is loaded into a Datalake folder containing subdirectories such as the data stage, the API from which the data was extracted, and each table corresponding to each endpoint. The incremental table (**close_approach_data**) is partitioned by year, month, and week of month (computed with a dedicated function) to optimize the data partition size, its organization, and reading. Each partition column is extracted from the 'approach_datetime' column, created in the table's extraction from casting 'epoch_date_close_approach', in Unix Time format, to datetime format ('YYYY-mm-dd HH:MM:SS').

For the full extraction (**asteroid_data**), data is added using an UPSERT operation that updates a register if an ID asocciated to it already exists in the table or inserts a new register otherwise, since, with each new batch registered, it is likely that for some NEOs already added there will be updates in some of their attributes (covered by the `when_matched_update_all` method) but it can also happen that new NEOs need to be inserted according to the results of the incremental extraction (covered by the `when_not_matched_insert_all` method). On the other hand, when loading incremental data, rows are compared by 'approach_datetime' and 'neo_reference_id' columns to avoid duplicates; an INSERT operation is performed with this aditional functionality.

### Transformation

For the silver and gold transformations of **close_approach_data** only the last batch extracted is read from the Delta Table; that is, the set of registers which have the present execution date as the 'extraction_date' value. To optimize the reading operation, in each data writing a z-order function is implemented having the 'extraction_date' column as parameter. However, when dealing with transformations related to **asteroid_data** all registers from each stage are read from this table to have the total index of detected asteroids and thus take everything into account without filtering.

- Here, the standard measurement units are **kilometers** for distances and **kilometers per hour** for velocity.

For the silver stage, the function `clean_table()` reads parameters from a schemas file, stored inside the 'metadata' folder, and applies to each table the modularized functions in `transform.py` for column deletion, renaming (using _regex_) and casting. Neither imputation for null values nor data deduplication is applied since the API does not allow null fields and, as mentioned above, the extraction settings avoid the storage of duplicate registers.

For the gold stage, specific transformation operations are applied in both tables inside the `app.py` file which compute scientifically analytical values:

**asteroid_data**
1. The 'estimated_diameter_avg' column is added with the average estimated diameter of each asteroid, useful for representing an approximate measurement of their spherical diameter being an irregularly shaped object.

2. The 'albedo' column is added with each asteroid's albedo; the measure of their reflectivity. This value, which ranges from 0 to 1, is used to describe their composition according to the fraction of sunlight they reflect, with 0 representing total absorption of radiation (a perfect black body) and 1 representing total reflection of radiation (a perfect mirror). The albedo $A$ is computed using the asteroid's diameter $D$, the average in this case, and their absolute magnitude or luminosity measure $H$ in the following equation rearranged from the one presented in [Asteroid Size Estimator (CNEOS)](https://cneos.jpl.nasa.gov/tools/ast_size_est.html):

$$A=10^{-2\log(D) + 6.2472 - 0.4H}$$

**close_approach_data**
1. The 'approach_datetime' column is separated into the 'approach_date' and 'approach_time' columns, both casted to _datetime64[ns]_.

2. The 'count_per_date' column is added as the NEO count grouped by approach date ('approach_date' column).

3. An INNER JOIN is performed between the full table and the incremental table to merge approach data with relevant descriptive data from the approaching asteroids. The function defined as `inner_join()` also takes a selection of columns in a list as a parameter, which is retrieved in `app.py` from the schemas file.

The tables in both transformation stages are stored in their respective directories with a logic similar to the one described in the 'Loading' section above. For the gold stage, only the joined table is loaded (**near_earth_approaches**).

### Optimization

Additionally, the `optimize.py` file is implemented to define functions for z-ordering and table compacting. The compacting and vacuum operations are executed in case this file is ran as a main program, optimizing each table in each stage only if the retention threshold for the Delta and log files, greater than 7 days, is surpassed. This threshold, which is also the default one set in Delta Lake, is made explicit in the `save_data()` loading function. This allows the scheduling of the tables' optimization in terms of storage and querying, for example, every 30 days.