(ns jdbc.core-test
  (:require [jdbc.core :as jdbc]
            [jdbc.proto :as proto]
            [jdbc.insert :as jdbc.insert]
            [clojure.test :refer :all]
            [clojure.string :as str])
  (:import java.sql.BatchUpdateException))

(def sqlite-dbspec1 {:classname "org.sqlite.JDBC"
                    :subprotocol "sqlite"
                    :subname "/tmp/jdbctest.db"})

(def sqlite-dbspec2 {:subprotocol "sqlite"
                     :subname "/tmp/jdbctest.db"})

;; each connection to :memory: gets its own private database, so tests that
;; create the same table name in turn do not collide
(def sqlite-dbspec3 {:subprotocol "sqlite"
                     :subname ":memory:"})

(def sqlite-dbspec4 {:subprotocol "sqlite"
                     :subname ":memory:"
                     :isolation-level :serializable})

(deftest db-specs
  (let [c1 (jdbc/connection sqlite-dbspec1)
        c2 (jdbc/connection sqlite-dbspec2)
        c3 (jdbc/connection sqlite-dbspec3)]
    (is (satisfies? proto/IConnection c1))
    (is (satisfies? proto/IConnection c2))
    (is (satisfies? proto/IConnection c3))))

(deftest db-isolation-level-1
  (let [c1 (-> (jdbc/connection sqlite-dbspec4)
               (proto/connection))
        c2 (-> (jdbc/connection sqlite-dbspec3)
               (proto/connection))]
    ;; this spec asked for :serializable
    (is (= (.getTransactionIsolation c1) 8))
    ;; and this one asked for nothing, so it reports the driver's default. SQLite
    ;; serializes by nature, so that default is SERIALIZABLE rather than the
    ;; READ_COMMITTED an MVCC engine starts at.
    (is (= (.getTransactionIsolation c2) 8))))

(deftest db-invalid-isolation-level
  (is (thrown? IllegalArgumentException
              (jdbc/connection sqlite-dbspec3 {:isolation-level :bogus}))))

(deftest connection-closed-on-setup-failure
  ;; When connection setup fails (e.g. bad isolation level), the raw JDBC
  ;; connection should be closed, not leaked.
  (let [closed? (atom false)
        orig-conn (proto/connection sqlite-dbspec3)
        spy-conn (proxy [java.sql.Connection] []
                   (close [] (reset! closed? true))
                   (setReadOnly [_] nil)
                   (setAutoCommit [_] nil)
                   (setTransactionIsolation [_]
                     (throw (IllegalArgumentException. "boom")))
                   (getMetaData [] (.getMetaData orig-conn)))]
    (try
      ;; Bypass proto/connection by calling the 2-arity directly
      ;; with a datasource that returns our spy connection
      (let [ds (reify javax.sql.DataSource
                 (getConnection [_] spy-conn))]
        (jdbc/connection ds {:isolation-level :serializable}))
      (catch Exception _))
    (.close orig-conn)
    (is (true? @closed?) "Raw connection should be closed when setup throws")))

(deftest db-isolation-level-2
  (let [func1 (fn [conn]
                (let [conn (proto/connection conn)
                      isolation (.getTransactionIsolation conn)]
                  (is (= isolation 8))))]
    (with-open [conn (jdbc/connection sqlite-dbspec3)]
      (jdbc/atomic-apply conn func1 {:isolation-level :serializable}))))

(deftest execute-return-type-consistency
  ;; String execute should return a single count, same as vector execute
  (with-open [conn (jdbc/connection sqlite-dbspec3)]
    (jdbc/execute! conn "CREATE TABLE exec_test (id integer, value varchar(255));")
    (let [r1 (jdbc/execute! conn "INSERT INTO exec_test VALUES (1, 'foo');")
          r2 (jdbc/execute! conn ["INSERT INTO exec_test VALUES (?, ?);" 2 "bar"])]
      (is (= 1 r1) "String execute should return a single count")
      (is (= 1 r2) "Vector execute should return a single count"))))

(deftest db-commands
  ;; Simple statement
  (with-open [conn (jdbc/connection sqlite-dbspec3)]
    (let [sql "CREATE TABLE foo (name varchar(255), age integer);"
          r   (jdbc/execute! conn sql)]
      (is (= 0 r))))

  ;; Statement with exception
  (with-open [conn (jdbc/connection sqlite-dbspec3)]
    (let [sql "CREATE TABLE foo (name varchar(255), age integer);"]
      (jdbc/execute! conn sql)
      (is (thrown? BatchUpdateException (jdbc/execute! conn sql)))))

  ;; Fetch from simple query
  (with-open [conn (jdbc/connection sqlite-dbspec3)]
    (let [result (jdbc/fetch conn "SELECT 1 + 1 as foo;")]
      (is (= [{:foo 2}] result))))


  ;; Fetch with sqlvec format and overwriting identifiers parameter. The alias is
  ;; quoted so the driver reports it uppercase: SQLite preserves the case it was
  ;; given, where H2 uppercased an unquoted one. Without the quotes the label
  ;; would arrive lowercase and identity would agree with the default
  ;; lower-casing, leaving the test unable to tell the two apart.
  (with-open [conn (jdbc/connection sqlite-dbspec3)]
    (let [result (jdbc/fetch conn ["SELECT 1 + 1 as \"FOO\";"] {:identifiers identity})]
      (is (= [{:FOO 2}] result)))
    (let [result (jdbc/fetch conn ["SELECT 1 + 1 as \"FOO\";"])]
      (is (= [{:foo 2}] result))))

  ;; Fetch returning rows
  (with-open [conn (jdbc/connection sqlite-dbspec3)]
    (let [result (jdbc/fetch conn ["SELECT 1 + 1 as foo;"] {:as-rows? true})]
      (is (= [2] (first result)))))

  ;; Fetch returning rows with header
  (with-open [conn (jdbc/connection sqlite-dbspec3)]
    (let [result (jdbc/fetch conn ["SELECT 1 + 1 as foo, 2 + 2 as bar;"] {:as-rows? true :header? true})]
      (is (= [["foo", "bar"] [2, 4]] result))))

  ;; Fetch from prepared statement
  (with-open [conn (jdbc/connection sqlite-dbspec3)]
    (let [stmt (jdbc/prepared-statement conn ["select ? as foo;" 2])
          result (jdbc/fetch conn stmt)]
      (is (= [{:foo 2}] result)))))


(deftest lazy-queries
  (with-open [conn (jdbc/connection sqlite-dbspec3)]
    (jdbc/atomic conn
      (with-open [cursor (jdbc/fetch-lazy conn "SELECT 1 + 1 as foo;")]
        (let [result (vec (jdbc/cursor->lazyseq cursor))]
          (is (= [{:foo 2}] result)))
        (let [result (vec (jdbc/cursor->lazyseq cursor))]
          (is (= [{:foo 2}] result)))))))

(deftest fetch-one-limits-results
  (with-open [conn (jdbc/connection sqlite-dbspec3)]
    (jdbc/execute! conn "CREATE TABLE fetch_one_test (id integer);")
    (jdbc/execute! conn ["INSERT INTO fetch_one_test VALUES (1);"])
    (jdbc/execute! conn ["INSERT INTO fetch_one_test VALUES (2);"])
    (jdbc/execute! conn ["INSERT INTO fetch_one_test VALUES (3);"])
    (let [result (jdbc/fetch-one conn "SELECT * FROM fetch_one_test")]
      (is (= {:id 1} result)))))

(deftest insert-bytes
  ;; The parameter is the byte array itself rather than a stream over it: the
  ;; SQLite driver has no InputStream binding, and setObject on one stores the
  ;; stream's toString while setBinaryStream throws outright. A byte array is the
  ;; portable spelling and exercises the same path through set-stmt-parameter!.
  (let [buffer (byte-array (map byte (range 0 10)))
        sql    "CREATE TABLE foo (id integer, data blob);"]
    (with-open [conn (jdbc/connection sqlite-dbspec3)]
      (jdbc/execute! conn sql)
      (let [res (jdbc/execute! conn ["INSERT INTO foo (id, data) VALUES (?, ?);" 1 buffer])]
        (is (= res 1)))
      (let [res (jdbc/fetch-one conn "SELECT * FROM foo")]
        (is (instance? (Class/forName "[B") (:data res)))
        (is (= (get (:data res) 2) 2))))))


(deftest transactions-dummy-strategy
  (let [sql1 "CREATE TABLE foo (name varchar(255), age integer);"
        sql2 "INSERT INTO foo (name,age) VALUES (?, ?);"
        sql3 "SELECT age FROM foo;"
        strategy (reify proto/ITransactionStrategy
                   (begin! [_ conn _opts] conn)
                   (rollback! [_ _conn _opts] nil)
                   (commit! [_ _conn _opts] nil))
        dbspec (assoc sqlite-dbspec3 :tx-strategy strategy)]
    (with-open [conn (jdbc/connection dbspec)]
      (is (identical? (:tx-strategy (meta conn)) strategy))
      (jdbc/execute! conn sql1)
      (try
        (jdbc/atomic conn
          (jdbc/execute! conn [sql2 "foo" 1])
          (jdbc/execute! conn [sql2 "bar" 2])
          (let [results (jdbc/fetch conn sql3)]
            (is (= (count results) 2))
            (throw (RuntimeException. "Fooo"))))

        (catch Exception _e
          (let [results (jdbc/fetch conn sql3)]
            (is (= (count results) 2))))))))


(deftest transactions
  (let [sql1 "CREATE TABLE foo (name varchar(255), age integer);"
        sql2 "INSERT INTO foo (name,age) VALUES (?, ?);"
        sql3 "SELECT age FROM foo;"]

    ;; Basic transaction test with exception.
    (with-open [conn (jdbc/connection sqlite-dbspec3)]
      (jdbc/execute! conn sql1)

      (try
        (jdbc/atomic conn
          (jdbc/execute! conn [sql2 "foo" 1])
          (jdbc/execute! conn [sql2 "bar" 2])

          (let [results (jdbc/fetch conn sql3)]
              (is (= (count results) 2))
              (throw (RuntimeException. "Fooo"))))
          (catch Exception e
            (let [results (jdbc/fetch conn sql3)]
              (is (= (count results) 0))))))

    ;; Basic transaction test without exception.
    (with-open [conn (jdbc/connection sqlite-dbspec3)]
      (jdbc/execute! conn sql1)

      (jdbc/atomic conn
        (jdbc/execute! conn [sql2 "foo" 1])
        (jdbc/execute! conn [sql2 "bar" 2]))

        (jdbc/atomic conn
          (let [results (jdbc/fetch conn sql3)]
            (is (= (count results) 2)))))

    ;; Immutability
    (with-open [conn (jdbc/connection sqlite-dbspec3)]
      (jdbc/atomic conn
        (let [metadata (meta conn)]
          (is (:transaction metadata))
          (is (:rollback metadata))
          (is (false? @(:rollback metadata)))
          (is (nil? (:savepoint metadata)))))

      (let [metadata (meta conn)]
        (is (= (:transaction metadata) nil))
        (is (= (:rollback metadata) nil))))

    ;; Savepoints
    (with-open [conn (jdbc/connection sqlite-dbspec3)]
      (jdbc/atomic conn
        (is (:transaction (meta conn)))
        (jdbc/atomic conn
          (is (not (nil? (:savepoint (meta conn))))))))

    ;; Set rollback 01
    (with-open [conn (jdbc/connection sqlite-dbspec3)]
        (jdbc/execute! conn sql1)

        (jdbc/atomic conn
        (jdbc/execute! conn [sql2 "foo" 1])
        (jdbc/execute! conn [sql2 "bar" 2])
        (is (false? @(:rollback (meta conn))))

        (jdbc/atomic conn
          (jdbc/execute! conn [sql2 "foo" 1])
          (jdbc/execute! conn [sql2 "bar" 2])
          (jdbc/set-rollback! conn)
          (is (true? @(:rollback (meta conn))))
          (let [results (jdbc/fetch conn sql3)]
            (is (= (count results) 4))))

        (let [results (jdbc/fetch conn [sql3])]
          (is (= (count results) 2)))))

    ;; Set rollback 02
    (with-open [conn (jdbc/connection sqlite-dbspec3)]
      (jdbc/execute! conn sql1)

      (jdbc/atomic conn
        (jdbc/set-rollback! conn)
        (jdbc/execute! conn [sql2 "foo" 1])
        (jdbc/execute! conn [sql2 "bar" 2])

        (is (true? @(:rollback (meta conn))))

        (jdbc/atomic conn
          (is (false? @(:rollback (meta conn))))

          (jdbc/execute! conn [sql2 "foo" 1])
          (jdbc/execute! conn [sql2 "bar" 2])
          (let [results (jdbc/fetch conn sql3)]
            (is (= (count results) 4))))

        (let [results (jdbc/fetch conn [sql3])]
          (is (= (count results) 4))))

      (let [results (jdbc/fetch conn [sql3])]
        (is (= (count results) 0))))
  
    ;; Subtransactions
    (with-open [conn (jdbc/connection sqlite-dbspec3)]
      (jdbc/execute! conn sql1)

      (jdbc/atomic conn
        (jdbc/execute! conn [sql2 "foo" 1])
        (jdbc/execute! conn [sql2 "bar" 2])

        (try
          (jdbc/atomic conn
            (jdbc/execute! conn [sql2 "foo" 1])
            (jdbc/execute! conn [sql2 "bar" 2])
            (let [results (jdbc/fetch conn [sql3])]
              (is (= (count results) 4))
              (throw (RuntimeException. "Fooo"))))
          (catch Exception _e
            (let [results (jdbc/fetch conn [sql3])]
              (is (= (count results) 2)))))))))

(deftest insert-as-arrays-option
  (with-open [conn (jdbc/connection sqlite-dbspec3)]
    (jdbc/execute! conn "CREATE TABLE arr_test (id integer primary key autoincrement, value varchar(255));")
    (let [result (jdbc.insert/db-do-execute-prepared-return-keys
                  (proto/connection conn)
                  "INSERT INTO arr_test (value) VALUES (?)"
                  ["foo"]
                  {:returning true :as-arrays? true})]
      ;; as-arrays? should return [header row] where row is a vector, not a map
      (is (sequential? result))
      (is (sequential? (first result)))))) ;; header should be a vector of column names

(deftest db-do-prepared-return-keys-no-infinite-recursion
  (with-open [conn (jdbc/connection sqlite-dbspec3)]
    (jdbc/execute! conn "CREATE TABLE recurse_test (id integer, value varchar(255));")
    ;; Calling db-do-prepared-return-keys with a map as sql-params should throw
    ;; a meaningful error, not stack overflow from infinite recursion
    (is (thrown? Exception
                (jdbc.insert/db-do-prepared-return-keys
                 (proto/connection conn) {:not "a valid sql-params"})))))


(deftest update-no-debug-output
  (with-open [conn (jdbc/connection sqlite-dbspec3)]
    (jdbc/execute! conn "CREATE TABLE update_debug (id integer, value varchar(255));")
    (jdbc/execute! conn ["INSERT INTO update_debug (id, value) VALUES (?, ?);" 1 "foo"])
    (let [output (with-out-str
                   (jdbc/update! conn :update_debug {:value "bar"} ["id = ?" 1]))]
      (is (= "" output) "update! should not print debug output to stdout"))))


