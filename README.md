# clojure.jdbc

A low level JDBC library for Clojure, jdbc-based database access..

![Test Badge](https://github.com/yogthos/clojure.jdbc/actions/workflows/main.yml/badge.svg)

## Installation

Add the following dependency to your project:

```clojure
{:deps {io.github.yogthos/clojure.jdbc  {:git/tag "v0.1.0" 
                                         :git/sha "3d5edd4"}}}
```

You also need to add an entry for the java driver that you need. For instance, for `Postgres`:

```
org.postgresql/postgresql {:mvn/version "42.6.0"}
```

## Connecting to database

This section intends to explain everything that you should to know about how
connect to the database.

### Connection parameters

JDBC is the default Java abstraction/interface for SQL databases. Clojure, as a guest language on the JVM, benefits from having a good, well-tested abstraction.

Connection parameters are exposed in a simple hash-map and called `dbspec`. 

This is the simplest and most idiomatic way in Clojure to define configuration parameters.

```clojure
(def dbspec {:subprotocol "postgresql"
             :subname "//localhost:5432/dbname"
             :user "username"         ;; Optional
             :password "password"}    ;; Optional
```

Also, `clojure.jdbc` comes with alternative humanized format.

```clojure
(def dbspec {:vendor "postgresql"
             :name "dbname"
             :host "localhost"      ;; Optional
             :port 5432             ;; Optional
             :user "username"       ;; Optional
             :password "password"}) ;; Optional
```

Finally, `dbspec` can be represented as a URI.

```clojure
(def dbspec "postgresql://user:password@localhost:5432/dbname")
```

### Creating a connection

With `clojure.jdbc` every function that interacts with a database explicitly requires a connection instance as parameter.

Creating a connection using `connection` function:

```clojure
(require '[jdbc.core :as jdbc])

(let [conn (jdbc/connection dbspec)]
  (do-something-with conn)
  (.close conn))
```

As you can see in previous example, you should explicltly close connection for proper resource management. A more idiomatic approach is to use the `with-open` clojure macro that will automatically close the connection.

```clojure
(with-open [conn (jdbc/connection dbspec)]
  (do-something-with conn))
```

## Execute Database Commands

#### Execute Raw SQL Statements

The simplest way to execute raw SQL is using the `execute!` function. It requires
an active connection followed by the SQL string:

```clojure
(with-open [conn (jdbc/connection dbspec)]
  (jdbc/execute! conn "CREATE TABLE foo (id serial, name text);"))
```

#### Query options

Query functions can take an additional map of options as the last parameter. For example,
if we wanted the query to timeout after 5 seconds, we could pass the `:timeout` flag to
the query.

```clojure
(with-open [conn (jdbc/connection dbspec)]
  (jdbc/execute! conn "CREATE TABLE foo (id serial, name text);" {:timeout 5}))
```

The options include

* `:returning` - return the id of the inserted row
* `:timeout` - timeout for query in seconds
* `:max-rows` - max rows to be returned
* `:fetch-size` - fetch size for the results

#### Execute Parametrized SQL Statements

Raw SQL statements work well for creating tables and similar operations. However,
when you need to insert some data, especially if the data comes from untrusted
sources, using plain string concatenation is not safe.

Parameters should be passed using **sqlvec** format that consists of a
vector with the SQL string followed by the dynamic parameters:

```clojure
["select * from foo where age < ?" 20]
```

The parameters will be pssed to a prepared statement ensuring that they're sanitized and escaped.

Most `clojure.jdbc` functions accept sqlvec as query parameter.
Let see an example using it in `execute!` function:

```clojure
(jdbc/execute! conn ["insert into foo (name) values (?);" "Foo"]))
```

#### Returning Inserted Keys

In some circumstances, you want to use the "RETURNING id" or similar functionality on
your queries for getting the primary keys of newly inserted records.

```clojure
(let [sql "insert into foo (name) values (?) returning id;"
      res (jdbc/fetch conn [sql "Foo"])]
  (println res))

;; This should print something like this to standard output:
[{:id 3}]
```

### Make Queries

Executing queries and fetch results is done using the `fetch` function:

```clojure
(let [sql    ["SELECT id, name FROM people WHERE age > ?", 2]
      result (jdbc/fetch conn sql)]
  (doseq [row result]
    (println row))))

;; It should print somthing like this:
;; => {:id 1 :name "Foo"}
;; => {:id 2 :name "Bar"}
```

While functions accept a plain sql and sqlvec as query parameters,
it's also possible to pass an instance of a custom `PreparedStatement`
or anythig that implements the `ISQLStatement` protocol as a parameter as well.

```clojure
(let [stmt (jdbc/prepared-statement ["SELECT id, name FROM people WHERE age > ?", 2])
      result (jdbc/fetch conn stmt)]
  (println "Result: " result))
```

#### **note**

The `fetch` method is useful in most cases but may not work well with
queries that return large results. For this purpose, cursor type queries exist
that are explained in the [Advanced usage](#server-side-cursors) section.

### Query helpers

The library provides serveral query helper functions for common operations.

#### insert!

The `insert!` helper can be used to make `insert` statements by providing a connection, followed by the table name and a map where keys represent column names for the value to be inserted.

```clojure
(jdbc/insert! conn :foo {:id 1 :value "bar"})
=> (1)

;; we can pass in :returning flag to get the value back
(jdbc/insert! conn :foo {:id 1 :value "bar"} {:returning true})
=> ({:id 1, :value "bar"})
```

Multiple rows can be inserted using `insert-multi!` which accepts a table name followed by a vector of rows to insert.

```clojure
(jdbc/insert-multi! conn :foo [{:id 1 :value "bar"} {:id 2 :value "baz"}])
```

The rows will be inserted using `executeBatch` method on the `PreparedStatement`. Note that some database drivers need to be told to rewrite the SQL for this to be performed as a single, batched operation.

In particular, PostgreSQL requires `:reWriteBatchedInserts` to be set to `true` and MySQL requires
  `:rewriteBatchedStatement` set to `true` (both non-standard JDBC options, of course!). These options should be passed into the driver when the connection is created in your program.

#### update!

The `update!` function accepts a connection, followed by the table name, a map of values to set, and a vector with the parametarized `WHERE` clause for the SQL statement.

```clojure
(update! conn :person {:zip 94540} [\"zip = ?\" 94546])
```

#### delete!

The `delete!` function accepts a connection followed by the table name, and a vector with the parametarized `WHERE` clause for the SQL statement.

```clojure
(jdbc/delete! conn :foo ["id = ?" 1])
```

### Transactions

#### Getting Started with Transactions

The most idiomatic way to wrap some code in a transaction, is by using the `atomic`
macro:

```clojure
(jdbc/atomic conn
  (do-thing-first conn)
  (do-thing-second conn))
```

Note that `clojure.jdbc` overwrites the lexical scope value of `conn` with a new
connection that has transactional state.

#### Low-level Transaction Primitives

Behind the scenes of the `atomic` macro, `clojure.jdbc` uses the `atomic-apply` function.

Given an active connection as the first parameter and a function that you want execute in a
transaction as the second parameter, it executes the function inside a database transaction.
The callback function should accept a connection as its first parameter.

```clojure
(jdbc/atomic-apply conn
  (fn [conn]
    (do-something-with conn)))
```

Note that `clojure.jdbc` handles nested transactions gracefully making all
code wrapped in transaction blocks truly atomic independently of transaction nesting.

If you want extend or change a default transaction strategy, see
[Transaction Strategy section](#transaction-strategy).

#### Isolation Level

By default, `clojure.jdbc` does nothing with the isolation level and keeps default values.

**You can set the isolation level when creating a connection by specifying it in your dbspec.**

```clojure
(def dbspec {:subprotocol "h2"
             :subname "mem:"
             :isolation-level :serializable})

(with-open [conn (jdbc/connection dbspec)]
  ;; The just created connection has the isolation
  ;; level set to :serializable
  (do-things conn))
```

Another way to set the isolation level is at the moment of declaring a transaction, using
the `atomic-apply` function or `atomic` macro:

```clojure
(jdbc/atomic-apply conn do-something {:isolation-level :serializable})

(jdbc/atomic conn {:isolation-level :serializable}
  (do-something conn))
```

This is a list of supported options:

* `:read-uncommitted` - Set read uncommitted isolation level
* `:read-committed` - Set read committed isolation level
* `:repeatable-read` - Set repeatable reads isolation level
* `:serializable` - Set serializable isolation level
* `:none` - Use this option to indicate to `clojure.jdbc` to do nothing and keep default behavior.

You can read more about it on [wikipedia](http://en.wikipedia.org/wiki/Isolation_(database_systems)).

**⚠️ WARNING ⚠️**

Not all JDBC providers support the above isolation levels.

#### Read-Only Transactions

In some circumstances, mainly when you are using the strictest isolation-level, you may want
to indicate to database that a query is actually read-only, allowing the database server to make some
optimizations for this operation.

```clojure
(jdbc/atomic conn {:isolation-level :serializable
                   :read-only true}
  (query-something conn))
```

## Advanced usage

### Server Side Cursors

By default, most JDBC drivers prefetch all results into memory make the use of lazy structures
totally useless for fetching data. Luckily, some databases implement server-side cursors that avoid
this behavior.

If you have an extremely large resultset and you want retrieve it and process each item, this is
exactly what you need.

For this purpose, `clojure.jdbc` exposes the `fetch-lazy` function, that returns some kind of
cursor instance. At the moment of creating cursor, no query is executed.

The cursor can be used by converting it into clojure lazyseq using `cursor->lazyseq` function:

```clojure
(jdbc/atomic conn
  (with-open [cursor (jdbc/fetch-lazy conn "SELECT id, name FROM people;")]
    (doseq [row (jdbc/cursor->lazyseq cursor)]
      (println row)))
```

In some databases, it requires that cursor should be evaluated in a context of one
transaction.

### Transaction Strategy

Transaction strategies in `clojure.jdbc`` are implemented using protocols having default
implementation explained in the previous sections. This approach allows an easy way to extend,
customize or completely change a transaction strategy for your application.

If you want another strategy, you should create a new type and implement the
`ITransactionStrategy` protocol as follows.

```clojure
(require '[jdbc.proto :as proto])

(def dummy-tx-strategy
  (reify
    proto/ITransactionStrategy
    (begin! [_ conn opts] conn)
    (rollback! [_ conn opts] conn)
    (commit! [_ conn opts] conn)))
```

`clojure.jdbc` has different ways to specify that transaction strategy shouldbe used. The most
common is setting it in your dbspec:

```clojure
(def dbspec {:subprotocol "postgresql"
             :subname "//localhost:5432/dbname"
             :tx-strategy dummy-tx-strategy})
(with-open [conn (jdbc/connection dbspec)]
  (jdbc/atomic conn
    ;; In this transaction block, the dummy transaction
    ;; strategy will be used.
    (do-somthing conn)))
```

Internally, `clojure.jdbc` maintains an instance of default transaction strategy stored
in a dynamic var. You can use the clojure facilities for alter that var for set
an other default transaction stragegy:

**Overwritting with `alter-var-root`**

```clojure
(alter-var-root #'jdbc/*default-tx-strategy* (fn [_] dummy-tx-strategy))
```

**Overwritting transaction strategy with dynamic scope**

```clojure
(binding [jdbc/*default-tx-strategy* dummy-tx-strategy]
  (some-func-that-uses-transactions))
```

## Extend SQL Types

If you want to extend some type/class to use it as JDBC parameter without explicit conversion
to an SQL-compatible type, you should extend your type with the `jdbc.proto/ISQLType` protocol.

Here is an example which extends Java’s String[] (string array) in order to pass it as
a query parameter that corresponds to PostgreSQL text array in the database:

```clojure
(require '[jdbc.proto :as proto])

(extend-protocol ISQLType
  ;; Obtain a class for string array
  (class (into-array String []))

  (set-stmt-parameter! [this conn stmt index]
    (let [value (proto/as-sql-type this conn)
          array (.createArrayOf conn "text" value)]
      (.setArray stmt index array)))

  (as-sql-type [this conn] this))
```

In this way you can pass a string array as a JDBC parameter that is automatically converted
to an SQL array and assigned properly in a prepared statement:

```clojure
(with-open [conn (jdbc/connection pg-dbspec)]
  (jdbc/execute! conn "CREATE TABLE arrayfoo (id integer, data text[]);")
  (let [mystringarray (into-array String ["foo" "bar"])]
    (jdbc/insert! conn :arrayfoo {:id 1 :data mystringarray})))
```

`clojure.jdbc` also exposes the `jdbc.proto/ISQLResultSetReadColumn` protocol that encapsulates
reverse conversions from SQL types to user-defined types.

Here is an example of roundtripping EDN using Postgres JSONB type:

```clojure
(ns jdbc.pg
  (:require [jdbc.time]
            [jdbc.core :as jdbc]
            [jdbc.proto :as proto]
            [clojure.data.json :as json])
  (:import 
   clojure.lang.IPersistentMap
   java.sql.PreparedStatement 
   org.postgresql.util.PGobject))

(extend-protocol proto/ISQLType
  IPersistentMap
  (as-sql-type [self _conn]
    (doto (PGobject.)
      (.setType "jsonb")
      (.setValue (json/write-str self))))
  (set-stmt-parameter! [self conn ^PreparedStatement stmt index]
    (.setObject stmt index (proto/as-sql-type self conn))))  

(extend-protocol proto/ISQLResultSetReadColumn
  PGobject
  (from-sql-type [self _ _ _]
    (let [value (.getValue self)]
      (case (.getType self)
        "jsonb" (json/read-str value :key-fn keyword)
        value))))
```

You can read more about that in this blog post: http://www.niwi.be/2014/04/13/postgresql-json-field-with-clojure-and-jdbc/

## Connection pooling

DataSource is the preferd way to connect to the database in production enviroments, and
is usually used to implement connection pools.

To make good use of resourses it is recommended to use some sort of a connection pool
implementation. This helps avoiding continuosly creating and destroying connections,
that in the majority of time is a slow operation.

`clojure.jdbc` is compatible with any DataSource based connection pool implemenetation, simply
pass a `javax.sql.DataSource` instance to `jdbc/connection` function.

[HikariCP](https://github.com/brettwooldridge/HikariCP) is a popular connection
pooling library for JDBC that has a [hikari-cp](https://github.com/tomekw/hikari-cp)
Clojure wrapper available.

In order to use this connection pool, you should first create a DataSource instance. Here
an little example:

```clojure
(require '[hikari-cp.core :as hikari])

(def ds (hikari/make-datasource
         {:connection-timeout 30000
          :idle-timeout 600000
          :max-lifetime 1800000
          :minimum-idle 10
          :maximum-pool-size  10
          :adapter "postgresql"
          :username "username"
          :password "password"
          :database-name "database"
          :server-name "localhost"
          :port-number 5432}))
```

HikariCP, unlike other datasource implementations, requires to setup explicitly that adapter should
be used. This is a list of supported adapters:


| Adapter           | Datasource class name
|-------------------|---------------------------------------
| `:derby`          | `org.apache.derby.jdbc.ClientDataSource`
| `:firebird`       | `org.firebirdsql.pool.FBSimpleDataSource`
| `:db2`            | `com.ibm.db2.jcc.DB2SimpleDataSource`
| `:h2`             | `org.h2.jdbcx.JdbcDataSource`
| `:hsqldb`         | `org.hsqldb.jdbc.JDBCDataSource`
| `:mariadb`        | `org.mariadb.jdbc.MySQLDataSource`
| `:mysql`          | `com.mysql.jdbc.jdbc2.optional.MysqlDataSource`
| `:sqlserver-jtds` | `net.sourceforge.jtds.jdbcx.JtdsDataSource`
| `:sqlserver`      | `com.microsoft.sqlserver.jdbc.SQLServerDataSource`
| `:oracle`         | `oracle.jdbc.pool.OracleDataSource`
| `:pgjdbc-ng`      | `com.impossibl.postgres.jdbc.PGDataSource`
| `:postgresql`     | `org.postgresql.ds.PGSimpleDataSource`
| `:sybase`         | `com.sybase.jdbcx.SybDataSource`


Now, the new created datasource should be used like a plain dbspec for creating connections:

```clojure
(with-open [conn (jdbc/connection ds)]
  (do-stuff conn))
```

## FAQ

### Why another JDBC wrapper?

This is an incomplete list of reasons:

* `clojure.jdbc` is a small and simple library.
* Connection management in `clojure.jdbc` is simple and explicit.
* `clojure.jdbc` comes with proper transaction management with full support for nested transactions,
  and plugable transaction strategies.
* `clojure.jdbc` has native support for connection pools.

### Performance

`clojure.jdbc` has good overall performance that should be sufficient for most situations. However, simplicity is the primary goal of the library.

You can
run the micro benchmark code in your environment with: `lein with-profile bench run`

In my environments, the results are:

```text
[3/5.0.5]niwi@niwi:~/clojure.jdbc> lein with-profile bench run
Simple query without connection overhead.
java.jdbc:
"Elapsed time: 673.890131 msecs"
clojure.jdbc:
"Elapsed time: 450.329706 msecs"
Simple query with connection overhead.
java.jdbc:
"Elapsed time: 2490.233925 msecs"
clojure.jdbc:
"Elapsed time: 2239.524395 msecs"
Simple query with transaction.
java.jdbc:
"Elapsed time: 532.151667 msecs"
clojure.jdbc:
"Elapsed time: 602.482932 msecs"
```

## Contributing

Contributions to the project are welcome. Simply clone the project and make a pull request for new features and bug fixes.

### Run tests

For running tests just execute this:

```text
clj -M:test
```
You should have postgresql up and running with a current user created with trust access mode
activated for this user and test db already created. The project also comes with a `docker-compose.yml` for spinning up a test Postgres.


### License

`clojure.jdbc` is licensed under [Apache 2.0 license]http://www.apache.org/licenses/LICENSE-2.0).


## Attribution

Many thanks to the original authors of `clojure.jdbc` and `clojure.java.jdbc`

* Andrey Antukh
* Sean Corfield
