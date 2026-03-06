(ns jdbc.core-test
  (:require [jdbc.core :as jdbc]
            [jdbc.proto :as proto]
            [jdbc.insert :as jdbc.insert]
            [hikari-cp.core :as hikari]
            [clojure.test :refer :all]
            [clojure.string :as str])
  (:import java.sql.BatchUpdateException
           org.postgresql.util.PSQLException))

(def h2-dbspec1 {:classname "org.h2.Driver"
                 :subprotocol "h2"
                 :subname "/tmp/jdbctest.db"})

(def h2-dbspec2 {:subprotocol "h2"
                 :subname "/tmp/jdbctest.db"})

(def h2-dbspec3 {:subprotocol "h2"
                 :subname "mem:"})

(def h2-dbspec4 {:subprotocol "h2"
                 :subname "mem:"
                 :isolation-level :serializable})

(def pg-dbspec {:subprotocol "postgresql"
                :subname "//localhost:5432/test"
                :user "postgres"
                :password "postgres"})

(def pg-dbspec-pretty {:vendor "postgresql"
                       :name "test"
                       :host "localhost"
                       :user "postgres"
                       :password "postgres"
                       :read-only true})

(def pg-dbspec-uri-1 "postgresql://localhost:5432/test?user=postgres&password=postgres")

(deftest datasource-spec
  (with-open [ds (hikari/make-datasource {:adapter "h2" :url "jdbc:h2:/tmp/test"})]
    (is (instance? javax.sql.DataSource ds))
    (with-open [conn (jdbc/connection ds)]
      (let [result (jdbc/fetch conn "SELECT 1 + 1 as foo;")]
        (is (= [{:foo 2}] result))))))

(deftest db-specs
  (let [c1 (jdbc/connection h2-dbspec1)
        c2 (jdbc/connection h2-dbspec2)
        c3 (jdbc/connection h2-dbspec3)
        c4 (jdbc/connection pg-dbspec-pretty)
        c5 (jdbc/connection pg-dbspec-uri-1)]
    (is (satisfies? proto/IConnection c1))
    (is (satisfies? proto/IConnection c2))
    (is (satisfies? proto/IConnection c3))
    (is (satisfies? proto/IConnection c4))
    (is (satisfies? proto/IConnection c5))))

(deftest db-isolation-level-1
  (let [c1 (-> (jdbc/connection h2-dbspec4)
               (proto/connection))
        c2 (-> (jdbc/connection h2-dbspec3)
               (proto/connection))]
    (is (= (.getTransactionIsolation c1) 8))
    (is (= (.getTransactionIsolation c2) 2))))

(deftest db-invalid-isolation-level
  (is (thrown? IllegalArgumentException
              (jdbc/connection h2-dbspec3 {:isolation-level :bogus}))))

(deftest connection-closed-on-setup-failure
  ;; When connection setup fails (e.g. bad isolation level), the raw JDBC
  ;; connection should be closed, not leaked.
  (let [closed? (atom false)
        orig-conn (proto/connection h2-dbspec3)
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
    (with-open [conn (jdbc/connection h2-dbspec3)]
      (jdbc/atomic-apply conn func1 {:isolation-level :serializable}))))

(deftest db-readonly-transactions
  (letfn [(func [conn]
            (let [raw (proto/connection conn)]
              (is (true? (.isReadOnly raw)))))]
    (with-open [conn (jdbc/connection pg-dbspec)]
      (jdbc/atomic-apply conn func {:read-only true})
      (is (false? (.isReadOnly (proto/connection conn))))))

  (with-open [conn (jdbc/connection pg-dbspec)]
    (jdbc/atomic conn {:read-only true}
      (is (true? (.isReadOnly (proto/connection conn)))))
    (is (false? (.isReadOnly (proto/connection conn))))))

(deftest query-timeout
  (with-open [conn (jdbc/connection pg-dbspec)]
    (try
      (jdbc/execute! conn "select pg_sleep(5);" {:timeout 1})
      (is (= 1 0) "failed timeout for execute")
      (catch BatchUpdateException e
        (is (= 0 (.getErrorCode e))))
      (catch PSQLException e
        (is (= 0 (.getErrorCode e)))))
    (try
      (jdbc/fetch conn ["select pg_sleep(5);"] {:timeout 1})
      (is (= 1 0) "failed timeout for fetch")
      (catch BatchUpdateException e
        (is (= 0 (.getErrorCode e))))
      (catch PSQLException e
        (is (= 0 (.getErrorCode e)))))
    (try
      (jdbc/fetch-one conn ["select pg_sleep(5);"] {:timeout 1})
      (is (= 1 0) "failed timeout for fetch-one")
      (catch BatchUpdateException e
        (is (= 0 (.getErrorCode e))))
      (catch PSQLException e
        (is (= 0 (.getErrorCode e)))))
    (try
      (with-open [cursor (jdbc/fetch-lazy conn ["select pg_sleep(5);"] {:timeout 1})]
        (vec (jdbc/cursor->lazyseq cursor))
        (is (= 1 0) "failed timeout for fetch-lazy"))
      (catch BatchUpdateException e
        (is (= 0 (.getErrorCode e))))
      (catch PSQLException e
        (is (= 0 (.getErrorCode e)))))))

(deftest db-commands-2
  (with-open [conn (jdbc/connection pg-dbspec)]
    (jdbc/atomic conn
      (jdbc/set-rollback! conn)
      (jdbc/execute! conn "create table foo2 (id serial, age integer);")
      (let [result (jdbc/fetch conn ["insert into foo2 (age) values (?) returning id" 1])]
        (is (= result [{:id 1}])))))

  (with-open [conn (jdbc/connection pg-dbspec)]
    (jdbc/atomic conn
      (jdbc/set-rollback! conn)
      (let [sql1 "CREATE TABLE foo (id integer primary key, age integer);"
            sql2 ["INSERT INTO foo (id, age) VALUES (?,?), (?,?);" 1 1 2 2]]
        (jdbc/execute! conn sql1)
        (let [result (jdbc/execute! conn sql2 {:returning true})]
          (is (= result [{:id 1, :age 1} {:id 2, :age 2}])))))))

(deftest execute-return-type-consistency
  ;; String execute should return a single count, same as vector execute
  (with-open [conn (jdbc/connection h2-dbspec3)]
    (jdbc/execute! conn "CREATE TABLE exec_test (id integer, value varchar(255));")
    (let [r1 (jdbc/execute! conn "INSERT INTO exec_test VALUES (1, 'foo');")
          r2 (jdbc/execute! conn ["INSERT INTO exec_test VALUES (?, ?);" 2 "bar"])]
      (is (= 1 r1) "String execute should return a single count")
      (is (= 1 r2) "Vector execute should return a single count"))))

(deftest db-commands
  ;; Simple statement
  (with-open [conn (jdbc/connection h2-dbspec3)]
    (let [sql "CREATE TABLE foo (name varchar(255), age integer);"
          r   (jdbc/execute! conn sql)]
      (is (= 0 r))))

  ;; Statement with exception
  (with-open [conn (jdbc/connection h2-dbspec3)]
    (let [sql "CREATE TABLE foo (name varchar(255), age integer);"]
      (jdbc/execute! conn sql)
      (is (thrown? org.h2.jdbc.JdbcBatchUpdateException (jdbc/execute! conn sql)))))

  ;; Fetch from simple query
  (with-open [conn (jdbc/connection h2-dbspec3)]
    (let [result (jdbc/fetch conn "SELECT 1 + 1 as foo;")]
      (is (= [{:foo 2}] result))))


  ;; Fetch from complex query in sqlvec format
  (with-open [conn (jdbc/connection pg-dbspec)]
    (let [result (jdbc/fetch conn ["SELECT * FROM generate_series(1, ?) LIMIT 1 OFFSET 3;" 10])]
      (is (= (count result) 1))))

  ;; Fetch with sqlvec format and overwriting identifiers parameter
  (with-open [conn (jdbc/connection h2-dbspec3)]
    (let [result (jdbc/fetch conn ["SELECT 1 + 1 as foo;"] {:identifiers identity})]
      (is (= [{:FOO 2}] result))))

  ;; Fetch returning rows
  (with-open [conn (jdbc/connection h2-dbspec3)]
    (let [result (jdbc/fetch conn ["SELECT 1 + 1 as foo;"] {:as-rows? true})]
      (is (= [2] (first result)))))

  ;; Fetch returning rows with header
  (with-open [conn (jdbc/connection h2-dbspec3)]
    (let [result (jdbc/fetch conn ["SELECT 1 + 1 as foo, 2 + 2 as bar;"] {:as-rows? true :header? true})]
      (is (= [["foo", "bar"] [2, 4]] result))))

  ;; Fetch from prepared statement
  (with-open [conn (jdbc/connection h2-dbspec3)]
    (let [stmt (jdbc/prepared-statement conn ["select ? as foo;" 2])
          result (jdbc/fetch conn stmt)]
      (is (= [{:foo 2}] result)))))


(deftest lazy-queries
  (with-open [conn (jdbc/connection h2-dbspec3)]
    (jdbc/atomic conn
      (with-open [cursor (jdbc/fetch-lazy conn "SELECT 1 + 1 as foo;")]
        (let [result (vec (jdbc/cursor->lazyseq cursor))]
          (is (= [{:foo 2}] result)))
        (let [result (vec (jdbc/cursor->lazyseq cursor))]
          (is (= [{:foo 2}] result)))))))

(deftest fetch-one-limits-results
  (with-open [conn (jdbc/connection h2-dbspec3)]
    (jdbc/execute! conn "CREATE TABLE fetch_one_test (id integer);")
    (jdbc/execute! conn ["INSERT INTO fetch_one_test VALUES (1);"])
    (jdbc/execute! conn ["INSERT INTO fetch_one_test VALUES (2);"])
    (jdbc/execute! conn ["INSERT INTO fetch_one_test VALUES (3);"])
    (let [result (jdbc/fetch-one conn "SELECT * FROM fetch_one_test")]
      (is (= {:id 1} result)))))

(deftest insert-bytes
  (let [buffer       (byte-array (map byte (range 0 10)))
        inputStream  (java.io.ByteArrayInputStream. buffer)
        sql          "CREATE TABLE foo (id integer, data bytea);"]
    (with-open [conn (jdbc/connection h2-dbspec3)]
      (jdbc/execute! conn sql)
      (let [res (jdbc/execute! conn ["INSERT INTO foo (id, data) VALUES (?, ?);" 1 inputStream])]
        (is (= res 1)))
      (let [res (jdbc/fetch-one conn "SELECT * FROM foo")]
        (is (instance? (Class/forName "[B") (:data res)))
        (is (= (get (:data res) 2) 2))))))


(extend-protocol proto/ISQLType
  (class (into-array String []))
  (as-sql-type [this conn] this)
  (set-stmt-parameter! [this conn stmt index]
    (let [prepared (proto/as-sql-type this conn)
          array (.createArrayOf conn "text" prepared)]
      (.setArray stmt index array))))

(deftest insert-arrays
  (with-open [conn (jdbc/connection pg-dbspec)]
    (jdbc/atomic conn
      (jdbc/set-rollback! conn)
      (let [sql "CREATE TABLE arrayfoo (id integer, data text[]);"
            dat (into-array String ["foo", "bar"])]
        (jdbc/execute! conn sql)
        (let [res (jdbc/execute! conn ["INSERT INTO arrayfoo (id, data) VALUES (?, ?);" 1, dat])]
          (is (= res 1)))

        (let [res (jdbc/fetch-one conn "SELECT * FROM arrayfoo")
              rr (.getArray (:data res))]
          (is (= (count rr) 2))
          (is (= (get rr 0) "foo"))
          (is (= (get rr 1) "bar")))))))

(deftest transactions-dummy-strategy
  (let [sql1 "CREATE TABLE foo (name varchar(255), age integer);"
        sql2 "INSERT INTO foo (name,age) VALUES (?, ?);"
        sql3 "SELECT age FROM foo;"
        strategy (reify proto/ITransactionStrategy
                   (begin! [_ conn _opts] conn)
                   (rollback! [_ _conn _opts] nil)
                   (commit! [_ _conn _opts] nil))
        dbspec (assoc h2-dbspec3 :tx-strategy strategy)]
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
    (with-open [conn (jdbc/connection h2-dbspec3)]
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
    (with-open [conn (jdbc/connection h2-dbspec3)]
      (jdbc/execute! conn sql1)

      (jdbc/atomic conn
        (jdbc/execute! conn [sql2 "foo" 1])
        (jdbc/execute! conn [sql2 "bar" 2]))

        (jdbc/atomic conn
          (let [results (jdbc/fetch conn sql3)]
            (is (= (count results) 2)))))

    ;; Immutability
    (with-open [conn (jdbc/connection h2-dbspec3)]
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
    (with-open [conn (jdbc/connection h2-dbspec3)]
      (jdbc/atomic conn
        (is (:transaction (meta conn)))
        (jdbc/atomic conn
          (is (not (nil? (:savepoint (meta conn))))))))

    ;; Set rollback 01
    (with-open [conn (jdbc/connection h2-dbspec3)]
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
    (with-open [conn (jdbc/connection h2-dbspec3)]
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
    (with-open [conn (jdbc/connection h2-dbspec3)]
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
  (with-open [conn (jdbc/connection h2-dbspec3)]
    (jdbc/execute! conn "CREATE TABLE arr_test (id integer auto_increment primary key, value varchar(255));")
    (let [result (jdbc.insert/db-do-execute-prepared-return-keys
                  (proto/connection conn)
                  "INSERT INTO arr_test (value) VALUES (?)"
                  ["foo"]
                  {:returning true :as-arrays? true})]
      ;; as-arrays? should return [header row] where row is a vector, not a map
      (is (sequential? result))
      (is (sequential? (first result)))))) ;; header should be a vector of column names

(deftest db-do-prepared-return-keys-no-infinite-recursion
  (with-open [conn (jdbc/connection h2-dbspec3)]
    (jdbc/execute! conn "CREATE TABLE recurse_test (id integer, value varchar(255));")
    ;; Calling db-do-prepared-return-keys with a map as sql-params should throw
    ;; a meaningful error, not stack overflow from infinite recursion
    (is (thrown? Exception
                (jdbc.insert/db-do-prepared-return-keys
                 (proto/connection conn) {:not "a valid sql-params"})))))

(deftest insert-test
  (with-open [conn (jdbc/connection pg-dbspec)]
    (jdbc/atomic conn
                 (jdbc/set-rollback! conn)
                 (jdbc/execute! conn "CREATE TABLE inserts (id integer, value text);")
                 (is (= [1] (jdbc/insert! conn :inserts {:id 1 :value "foo"})))
                 (is (= [{:id 1 :value "foo"}] (jdbc/fetch conn ["select * from inserts where id = ?" 1]))))
    (jdbc/atomic conn
                 (jdbc/set-rollback! conn)
                 (jdbc/execute! conn "CREATE TABLE inserts (id integer, value text);")
                 (is (= [{:id 1 :value "foo"}] (jdbc/insert! conn :inserts {:id 1 :value "foo"} {:returning true}))))))

(deftest insert-multi-test
  (with-open [conn (jdbc/connection pg-dbspec)]
    (jdbc/atomic conn
                 (jdbc/set-rollback! conn)
                 (jdbc/execute! conn "CREATE TABLE inserts (id integer, value text);")
                 (jdbc/insert-multi! conn :inserts [{:id 1 :value "foo"}
                                                    {:id 2 :value "foo"}])
                 (is (= [{:id 1, :value "foo"} {:id 2, :value "foo"}] 
                        (jdbc/fetch conn ["select * from inserts"])))
                 (is (= [{:id 3 :value "bar"}
                         {:id 4 :value "baz"}]
                        (jdbc/insert-multi! conn :inserts
                                            [{:id 3 :value "bar"}
                                             {:id 4 :value "baz"}]
                                            {:returning true})))
                 (is
                  (= [2 3 4]
                     (jdbc.core/insert-multi! conn :inserts
                                              [{:id 2}
                                               {:id 3}
                                               {:id 4}]
                                              {:returning true
                                               :row-fn :id}))))))

(deftest update-no-debug-output
  (with-open [conn (jdbc/connection h2-dbspec3)]
    (jdbc/execute! conn "CREATE TABLE update_debug (id integer, value varchar(255));")
    (jdbc/execute! conn ["INSERT INTO update_debug (id, value) VALUES (?, ?);" 1 "foo"])
    (let [output (with-out-str
                   (jdbc/update! conn :update_debug {:value "bar"} ["id = ?" 1]))]
      (is (= "" output) "update! should not print debug output to stdout"))))

(deftest update-test
  (with-open [conn (jdbc/connection pg-dbspec)]
    (jdbc/atomic conn
                 (jdbc/set-rollback! conn)
                 (jdbc/execute! conn "CREATE TABLE updates (id integer, value text);")
                 (jdbc/insert! conn :updates {:id 1 :value "foo"})
                 (jdbc/update! conn :updates {:value "bar"} ["id = ?" 1])
                 (is (= [{:id 1, :value "bar"}]
                        (jdbc/fetch conn ["select * from updates where id = ?" 1]))))))

(deftest delete-test
  (with-open [conn (jdbc/connection pg-dbspec)]
    (jdbc/atomic conn
                 (jdbc/set-rollback! conn)
                 (jdbc/execute! conn "CREATE TABLE deletes (id integer, value text);")
                 (jdbc/insert! conn :deletes {:id 1 :value "foo"})
                 (jdbc/delete! conn :deletes ["id = ?" 1])
                 (is (= []
                        (jdbc/fetch conn ["select * from deletes where id = ?" 1]))))))

