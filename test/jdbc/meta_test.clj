(ns jdbc.meta-test
  (:require [jdbc.core :as jdbc]
            [jdbc.meta :as meta]
            [jdbc.proto :as proto]
            [clojure.test :refer :all])
  (:import java.sql.Connection))

(def sqlite-dbspec {:subprotocol "sqlite"
                    :subname ":memory:"})

(deftest isolation-level-mapping
  (with-open [conn (jdbc/connection sqlite-dbspec)]
    (let [raw ^Connection (proto/connection conn)]
      ;; SQLite reports back every level it is given, so each one round-trips
      (.setTransactionIsolation raw Connection/TRANSACTION_READ_COMMITTED)
      (is (= :read-committed (meta/isolation-level conn)))

      (.setTransactionIsolation raw Connection/TRANSACTION_READ_UNCOMMITTED)
      (is (= :read-uncommitted (meta/isolation-level conn)))

      (.setTransactionIsolation raw Connection/TRANSACTION_REPEATABLE_READ)
      (is (= :repeatable-read (meta/isolation-level conn)))

      (.setTransactionIsolation raw Connection/TRANSACTION_SERIALIZABLE)
      (is (= :serializable (meta/isolation-level conn))))))

(deftest vendor-name-test
  (with-open [conn (jdbc/connection sqlite-dbspec)]
    (is (string? (meta/vendor-name conn)))
    (is (= "SQLite" (meta/vendor-name conn)))))
