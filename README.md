# DB Table

[![Build Status](https://api.travis-ci.org/reines/db-table.png)](https://travis-ci.org/reines/db-table)
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

## License

Released under the [Apache 2.0 License](LICENSE).
