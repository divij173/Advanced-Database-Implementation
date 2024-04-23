# Advanced Database Implementation

## Overview

This repository contains projects related to the Advanced Database Implementation course. The course delves into the internal workings of database management systems (DBMSs) such as Oracle or SQL Server, exploring data structures, algorithms, storage management, relational operations, query optimization, transactions, concurrency, recovery, security, and emerging trends in large data repositories.

The primary focus is on relational DBMSs, particularly PostgreSQL, offering insights into real-world DBMS internals. While the content covers a wide range of topics, some areas are explored in greater detail than others.

## Projects

### Project 1: `gcoord.c`

This project file contains routines that can be bound to a PostgreSQL backend and are called during query processing. The file includes functions for parsing geographical coordinates, performing validations, and converting formats. Key functions include:

- `check_input`: Validates input strings representing geographical coordinates.
- `connocal_form`: Converts input strings into canonical format.
- `get_loc_name`, `get_latit`, `get_longi`: Extracts location name, latitude, and longitude from input strings.
- `gcoord_in`, `gcoord_out`: Input and output functions for PostgreSQL custom data type.
- Comparison and support functions for B-tree indexing (`gcoord_abs_cmp_internal`, `gcoord_abs_gcmp_internal`, etc.).
- Conversion functions (`convert_string`, `convert2dms`) for formatting geographical coordinates.


# Project 2

## Introduction

Project 2 is a buffer pool management system designed to efficiently manage buffers in memory. It includes functionalities such as initializing the buffer pool, requesting and releasing pages, and joining tables from a database.

## Implementation Details

### Buffer Pool Initialization

The buffer pool is initialized using the `initBufPool` function, which takes the number of buffers (`nbufs`) and a strategy (`strategy`) as parameters.

### Page Management

The `request_page` function is used to request a page from the buffer pool. If the page is already in the pool, it returns the slot index. Otherwise, it reads the page from disk and adds it to the pool.

The `release_page` function releases a page from the buffer pool, making it available for other pages to use.

### Table Operations

The `sel` function performs a selection operation on a table based on a condition value (`cond_val`) and returns a `_Table` struct pointer containing the selected tuples.

The `join` function joins two tables based on specified attributes (`idx1` and `idx2`) and returns a `_Table` struct pointer containing the joined tuples.

## Usage

To use Project 2, include the necessary header files (`stdio.h`, `stdlib.h`, `ro.h`, `db.h`, `assert.h`) and implement the functions according to your requirements.

Example usage:

```c
#include <stdio.h>
#include <stdlib.h>
#include "ro.h"
#include "db.h"
#include <assert.h>

// Function prototypes and implementation here...

int main() {
    // Initialize buffer pool and perform operations
    BufPool pool = initBufPool(10, 'LRU');
    // Perform page requests and releases
    int page = request_page(pool, 123);
    // Perform table operations
    _Table* result = sel(0, 10, "table_name");
    // Join tables
    _Table* joined = join(0, "table1_name", 1, "table2_name");
    return 0;
}
```

## Conclusion

Project 2 provides a foundation for efficient buffer pool management and table operations in a database system. By implementing the provided functions, developers can customize and extend the functionality as needed.
