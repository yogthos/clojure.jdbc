;; Copyright 2014-2016 Andrey Antukh <niwi@niwi.nz>
;;
;; Licensed under the Apache License, Version 2.0 (the "License")
;; you may not use this file except in compliance with the License.
;; You may obtain a copy of the License at
;;
;;     http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.

(ns jdbc.core
  "Alternative implementation of jdbc wrapper for clojure."
  (:require 
   [jdbc.insert :as insert]
   [jdbc.util :as util]
   [jdbc.types :as types]
   [jdbc.impl :as impl]
   [jdbc.proto :as proto]
   [jdbc.constants :as constants])
  (:import
   java.sql.PreparedStatement 
   java.sql.Connection))

(def ^{:doc "Default transaction strategy implementation."
       :no-doc true
       :dynamic true}
  *default-tx-strategy* (impl/transaction-strategy))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Main public api.
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn connection
  "Creates a connection to a database. As parameter accepts:

  - dbspec map containing connection parameters
  - dbspec map containing a datasource (deprecated)
  - URI or string (interpreted as uri)
  - DataSource instance

  The dbspec map has this possible variants:

  Classic approach:

  - `:subprotocol` -> (required) string that represents a vendor name (ex: postgresql)
  - `:subname` -> (required) string that represents a database name (ex: test)
    (many others options that are pased directly as driver parameters)

  Pretty format:

  - `:vendor` -> (required) string that represents a vendor name (ex: postgresql)
  - `:name` -> (required) string that represents a database name (ex: test)
  - `:host` -> (optional) string that represents a database hostname (default: 127.0.0.1)
  - `:port` -> (optional) long number that represents a database port (default: driver default)
    (many others options that are pased directly as driver parameters)

  URI or String format: `vendor://user:password@host:post/dbname?param1=value`

  Additional options:

  - `:schema` -> string that represents a schema name (default: nil)
  - `:read-only` -> boolean for mark entire connection read only.
  - `:isolation-level` -> keyword that represents a isolation level (`:none`, `:read-committed`,
                        `:read-uncommitted`, `:repeatable-read`, `:serializable`)

  Opions can be passed as part of dbspec map, or as optional second argument.
  For more details, see documentation."
  ([dbspec] (connection dbspec {}))
  ([dbspec options]
   (let [^Connection conn (proto/connection dbspec)
         options (merge (when (map? dbspec) dbspec) options)]

     ;; Set readonly flag if it found on the options map
     (some->> (:read-only options)
              (.setReadOnly conn))

     ;; Set the concrete isolation level if it found
     ;; on the options map
     (some->> (:isolation-level options)
              (get constants/isolation-levels)
              (.setTransactionIsolation conn))

     ;; Set the schema if it found on the options map
     (some->> (:schema options)
              (.setSchema conn))

     (let [tx-strategy (:tx-strategy options  *default-tx-strategy*)
           metadata {:tx-strategy tx-strategy}]
       (with-meta (types/->connection conn) metadata)))))

(defn prepared-statement?
  "Check if specified object is prepared statement."
  [obj]
  (instance? PreparedStatement obj))

(defn prepared-statement
  "Given a string or parametrized sql in sqlvec format
  return an instance of prepared statement.
  `options` is an optional map with these keys (all optional):
  - :result-type - A keyword indicating the java.sql.ResultSet type. Any of :forward-only, :scroll-insensitive, :scroll-sensitive.
  - :result-concurrency - A keyword indicating the concurrency mode of the java.sql.ResultSet object. Either :read-only or :updatable.
  - :fetch-size - An integer indicating the number of rows to fetch at once.
  - :max-rows - An integer indicating the maximumal number of rows that may be fetched.
  - :holdability - A keyword indicating whether cursors should be held or closed on commit. Either :hold or :close.
  - :returning - Either true or :all, to indicate that the generated keys for new rows should be returned, or a sequence of keywords indicating the names of rows to return.

  More information about these options can be found in the [javadoc](https://docs.oracle.com/javase/8/docs/api/java/sql/ResultSet.html)."
  ([conn sqlvec] (prepared-statement conn sqlvec {}))
  ([conn sqlvec options]
   (let [conn (proto/connection conn)]
     (proto/prepared-statement sqlvec conn options))))

(defn execute!
  "Execute a query and return a number of rows affected.

      (with-open [conn (jdbc/connection dbspec)]
        (jdbc/execute conn \"create table foo (id integer);\"))

  This function also accepts sqlvec format."
  ([conn q] (execute! conn q {}))
  ([conn q opts]
   (let [rconn (proto/connection conn)]
     (proto/execute q rconn opts))))

(defn fetch
  "Fetch eagerly results executing a query.

  This function returns a vector of records (default) or
  rows (depending on specified opts). Resources are relased
  inmediatelly without specific explicit action for it.

  It accepts a sqlvec, plain sql or prepared statement
  as query parameter."
  ([conn q] (fetch conn q {}))
  ([conn q opts]
   (let [rconn (proto/connection conn)]
     (proto/fetch q rconn opts))))

(defn fetch-one
  "Fetch eagerly one result executing a query."
  ([conn q] (fetch-one conn q {}))
  ([conn q opts]
   (first (fetch conn q opts))))

(defn fetch-lazy
  "Fetch lazily results executing a query.

      (with-open [cursor (jdbc/fetch-lazy conn sql)]
        (doseq [item (jdbc/cursor->lazyseq cursor)]
          (do-something-with item)))

  This function returns a cursor instead of result.
  You should explicitly close the cursor at the end of
  iteration for release resources.

  `options` is an optional map with these keys (all optional):
  - :result-type - A keyword indicating the java.sql.ResultSet type. Any of :forward-only, :scroll-insensitive, :scroll-sensitive.
  - :result-concurrency - A keyword indicating the concurrency mode of the java.sql.ResultSet object. Either :read-only or :updatable.
  - :fetch-size - An integer indicating the number of rows to fetch at once.
  - :max-rows - An integer indicating the maximumal number of rows that may be fetched.
  - :holdability - A keyword indicating whether cursors should be held or closed on commit. Either :hold or :close.

  More information about these options can be found in the [javadoc](https://docs.oracle.com/javase/8/docs/api/java/sql/ResultSet.html)."
  ([conn q] (fetch-lazy conn q {}))
  ([conn q options]
   (let [^Connection conn (proto/connection conn)
         ^PreparedStatement stmt (proto/prepared-statement q conn options)]
     (types/->cursor stmt))))

(def ^{:doc "Deprecated alias for backward compatibility."
       :deprecated true}
  lazy-query fetch-lazy)

(defn cursor->lazyseq
  "Transform a cursor in a lazyseq.

  The returned lazyseq will return values until a cursor
  is closed or all values are fetched."
  ([cursor] (impl/cursor->lazyseq cursor {}))
  ([cursor opts] (impl/cursor->lazyseq cursor opts)))

(defn insert!
  "Given a database connection, a table name and a map representing a rows,
  perform an insert.
  
  The result is the database-specific form of the generated keys, if available
  (note: PostgreSQL returns the whole row).
  
  The row map or column value vector may be followed by a map of options:
  The :entities option specifies how to convert the table name and column names
  to SQL entities."
  ([conn table row] (insert! conn table row {}))
  ([conn table row opts]
   (insert/insert-rows! (proto/connection conn) table [row] opts)))

(defn insert-multi!
  "Given a database connection, a table name and a sequence of maps for
  rows, and possibly a map of options, insert that data into
  the database.

  The result is a sequence of the generated keys, if available (note: PostgreSQL
  returns the whole rows). A single database operation should be used to insert
  all the rows at once using executeBatch.
  
  Note: some database drivers need to be told to rewrite the SQL for this to
  be performed as a single, batched operation. In particular, PostgreSQL
  requires :reWriteBatchedInserts true and My SQL requires
  :rewriteBatchedStatement true (both non-standard JDBC options, of course!).
  These options should be passed into the driver when the connection is
  created (however that is done in your program).

  The :entities option specifies how to convert the table name and column
  names to SQL entities."
  ([conn table rows] (insert/insert-rows! (proto/connection conn) table rows {}))
  ([conn table rows opts]
   (insert/insert-rows! (proto/connection conn) table rows opts)))

(defn update!
  "Given a database connection, a table name, a map of column values to set and a
  where clause of columns to match, perform an update. The options may specify
  how column names (in the set / match maps) should be transformed (default
  'as-is') and whether to run the update in a transaction (default true).
  Example:
    (update! db :person {:zip 94540} [\"zip = ?\" 94546])
  is equivalent to:
    (execute! db [\"UPDATE person SET zip = ? WHERE zip = ?\" 94540 94546])"
  ([conn table set-map where-clause] (update! conn table set-map where-clause {}))
  ([conn table set-map where-clause opts]
   (let [{:keys [entities] :as opts}
         (merge {:entities identity} opts)]
     (prn (util/update-sql table set-map where-clause entities))
     (execute! (proto/connection conn) (util/update-sql table set-map where-clause entities) opts))))

(defn delete!
  "Given a database connection, a table name and a where clause of columns to match,
  perform a delete. The options may specify how to transform column names in the
  map (default 'as-is') and whether to run the delete in a transaction (default true).
  Example:
    (delete! db :person [\"zip = ?\" 94546])
  is equivalent to:
    (execute! db [\"DELETE FROM person WHERE zip = ?\" 94546])"
  ([conn table where-clause] (delete! conn table where-clause {}))
  ([conn table where-clause opts]
   (let [{:keys [entities] :as opts}
         (merge {:entities identity} opts)
         delete-sql (fn delete-sql 
                      [table [where & params] entities]
                      (into [(str "DELETE FROM " (util/table-str table entities)
                                  (when where " WHERE ") where)]
                            params))]
     (execute! (proto/connection conn) (delete-sql table where-clause entities) opts))))
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Transactions
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn atomic-apply
  "Wrap function in one transaction.
  This function accepts as a parameter a transaction strategy. If no one
  is specified, `DefaultTransactionStrategy` is used.

  With `DefaultTransactionStrategy`, if current connection is already in
  transaction, it uses truly nested transactions for properly handle it.
  The availability of this feature depends on database support for it.

      (with-open [conn (jdbc/connection)]
        (atomic-apply conn (fn [conn] (execute! conn 'DROP TABLE foo;'))))

  For more idiomatic code, you should use `atomic` macro.

  Depending on transaction strategy you are using, this function can accept
  additional parameters. The default transaction strategy exposes two additional
  parameters:

  - `:isolation-level` - set isolation level for this transaction
  - `:read-only` - set current transaction to read only"
  [conn func & [{:keys [savepoints strategy] :or {savepoints true} :as opts}]]
  (let [metadata (meta conn)
        tx-strategy (or strategy
                        (:tx-strategy metadata)
                        *default-tx-strategy*)]
    (when (and (:transaction metadata) (not savepoints))
      (throw (RuntimeException. "Savepoints explicitly disabled.")))

    (let [conn (proto/begin! tx-strategy conn opts)
          metadata (meta conn)]
      (try
        (let [returnvalue (func conn)]
          (proto/commit! tx-strategy conn opts)
          returnvalue)
        (catch Throwable t
          (proto/rollback! tx-strategy conn opts)
          (throw t))))))

(defmacro atomic
  "Creates a context that evaluates in transaction (or nested transaction).
  This is a more idiomatic way to execute some database operations in
  atomic way.

      (jdbc/atomic conn
        (jdbc/execute conn \"DROP TABLE foo;\")
        (jdbc/execute conn \"DROP TABLE bar;\"))

  Also, you can pass additional options to transaction:

      (jdbc/atomic conn {:read-only true}
        (jdbc/execute conn \"DROP TABLE foo;\")
        (jdbc/execute conn \"DROP TABLE bar;\"))
  "
  [conn & body]
  (if (map? (first body))
    `(let [func# (fn [c#] (let [~conn c#] ~@(next body)))]
       (atomic-apply ~conn func# ~(first body)))
    `(let [func# (fn [c#] (let [~conn c#] ~@body))]
       (atomic-apply ~conn func#))))

(defn set-rollback!
  "Mark a current connection for rollback.

  It ensures that on the end of the current transaction
  instead of commit changes, rollback them.

  This function should be used inside of a transaction
  block, otherwise this function does nothing.

      (jdbc/atomic conn
        (make-some-queries-without-changes conn)
        (jdbc/set-rollback! conn))
  "
  [conn]
  (let [metadata (meta conn)]
    (when-let [rollback-flag (:rollback metadata)]
      (reset! rollback-flag true))))
