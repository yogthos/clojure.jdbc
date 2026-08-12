(ns jdbc.postgres-test
  (:require [jdbc.core :as jdbc]
            [jdbc.proto :as proto]
            [hikari-cp.core :as hikari]
            [clojure.test :refer :all])
  (:import java.sql.BatchUpdateException
           org.postgresql.util.PSQLException))

;; These tests need a running PostgreSQL (localhost:5432) and the JVM-only
;; hikari-cp / postgres driver, so they live outside the sqlite-only :jolt suite.

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
  (with-open [ds (hikari/make-datasource {:adapter "sqlite" :jdbc-url "jdbc:sqlite::memory:"})]
    (is (instance? javax.sql.DataSource ds))
    (with-open [conn (jdbc/connection ds)]
      (let [result (jdbc/fetch conn "SELECT 1 + 1 as foo;")]
        (is (= [{:foo 2}] result))))))

(deftest db-specs-pg
  (let [c4 (jdbc/connection pg-dbspec-pretty)
        c5 (jdbc/connection pg-dbspec-uri-1)]
    (is (satisfies? proto/IConnection c4))
    (is (satisfies? proto/IConnection c5))))

(deftest db-readonly-transactions
  (letfn [(func [conn]
            (let [raw (proto/connection conn)]
              (is (true? (.isReadOnly raw)))))]
    (with-open [conn (jdbc/connection pg-dbspec)]
      (jdbc/atomic-apply conn func {:read-only true})
      (is (false? (.isReadOnly (proto/connection conn)))))

    (with-open [conn (jdbc/connection pg-dbspec)]
      (jdbc/atomic conn {:read-only true}
        (is (true? (.isReadOnly (proto/connection conn)))))
      (is (false? (.isReadOnly (proto/connection conn)))))))

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

(deftest db-commands-postgres
  ;; Fetch from complex query in sqlvec format
  (with-open [conn (jdbc/connection pg-dbspec)]
    (let [result (jdbc/fetch conn ["SELECT * FROM generate_series(1, ?) LIMIT 1 OFFSET 3;" 10])]
      (is (= (count result) 1)))))

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
