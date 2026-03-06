(ns jdbc.meta-test
  (:require [jdbc.core :as jdbc]
            [jdbc.meta :as meta]
            [jdbc.proto :as proto]
            [clojure.test :refer :all])
  (:import java.sql.Connection))

(def h2-dbspec {:subprotocol "h2"
                :subname "mem:"})

(deftest isolation-level-mapping
  (with-open [conn (jdbc/connection h2-dbspec)]
    (let [raw ^Connection (proto/connection conn)]
      ;; H2 defaults to read-committed
      (.setTransactionIsolation raw Connection/TRANSACTION_READ_COMMITTED)
      (is (= :read-committed (meta/isolation-level conn)))

      (.setTransactionIsolation raw Connection/TRANSACTION_READ_UNCOMMITTED)
      (is (= :read-uncommitted (meta/isolation-level conn)))

      ;; H2 upgrades REPEATABLE_READ to SERIALIZABLE, so we only test serializable
      (.setTransactionIsolation raw Connection/TRANSACTION_SERIALIZABLE)
      (is (= :serializable (meta/isolation-level conn))))))

(deftest vendor-name-test
  (with-open [conn (jdbc/connection h2-dbspec)]
    (is (string? (meta/vendor-name conn)))
    (is (= "H2" (meta/vendor-name conn)))))
