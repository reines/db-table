# DB Table

[![Build Status](https://api.travis-ci.org/reines/db-table.png)](https://travis-ci.org/reines/db-table)

DB Table is an implementation of the [Guava Table](https://code.google.com/p/guava-libraries/wiki/NewCollectionTypesExplained#Table) interface, backed by [JDBI](http://jdbi.org). It is designed and tested against the Java SQL database [H2](http://www.h2database.com/html/main.html), though with some minor dialect tweaks should work on any supported by JDBI.

DB Table can be found in maven central.

    <dependency>
        <groupId>com.jamierf</groupId>
        <artifactId>db-table</artifactId>
        <version>...</version>
    </dependency>

## License
The DB Table library is released under the BSD 3-clause license.
