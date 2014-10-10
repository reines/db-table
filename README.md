# DB Table

[![Build Status](https://api.travis-ci.org/reines/db-table.png)](https://travis-ci.org/reines/db-table)
[![Coverage Status](https://coveralls.io/repos/reines/db-table/badge.png?branch=master)](https://coveralls.io/r/reines/db-table?branch=master)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.jamierf.db-table/db-table-core/badge.png)](https://maven-badges.herokuapp.com/maven-central/com.jamierf.db-table/db-table-core)

DB Table is an implementation of the [Guava Table](https://code.google.com/p/guava-libraries/wiki/NewCollectionTypesExplained#Table) interface, backed by [JDBI](http://jdbi.org). It is designed and tested against the Java SQL database [H2](http://www.h2database.com/html/main.html), though with some minor dialect tweaks should work on any supported by JDBI.

DB Table can be found in maven central.

## Installation

```xml
<dependency>
    <groupId>com.jamierf.db-table</groupId>
    <artifactId>db-table-jackson</artifactId>
    <version>...</version>
</dependency>
```

## Usage

```java
final Handle handle = DBI.open("jdbc:h2:example.db");
final Table<String, String, String> table = new JacksonDbTableBuilder(handle)
    .build("table_name", String.class, String.class, String.class);
table.put("row", "column", "value");
```

## License

Released under the [Apache 2.0 License](LICENSE).
