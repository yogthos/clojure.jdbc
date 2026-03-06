(ns jdbc.impl-test
  (:require [jdbc.impl :as impl]
            [jdbc.core :as jdbc]
            [jdbc.proto :as proto]
            [clojure.test :refer :all]))

(deftest uri-password-with-colons
  (let [uri (java.net.URI. "postgresql://user:pass:word@localhost:5432/mydb")
        dbspec (#'impl/uri->dbspec uri)]
    (is (= "user" (:user dbspec)))
    (is (= "pass:word" (:password dbspec)))))

(deftest uri-password-simple
  (let [uri (java.net.URI. "postgresql://user:secret@localhost:5432/mydb")
        dbspec (#'impl/uri->dbspec uri)]
    (is (= "user" (:user dbspec)))
    (is (= "secret" (:password dbspec)))))

(deftest querystring-with-equals-in-value
  (let [uri (java.net.URI. "postgresql://localhost/mydb?options=-c%20search_path=public")]
    (is (map? (#'impl/querystring->map uri)))))

(deftest querystring-empty-value
  (let [uri (java.net.URI. "postgresql://localhost/mydb?sslmode=require&debug=")]
    (let [result (#'impl/querystring->map uri)]
      (is (= "require" (:sslmode result))))))

(def h2-mem {:subprotocol "h2" :subname "mem:"})

(deftest internal-keys-not-passed-as-driver-properties
  ;; Keys like :isolation-level, :read-only, :schema, :tx-strategy, :classname
  ;; should not be passed as JDBC driver properties
  (let [props (atom nil)
        orig @#'impl/map->properties]
    (with-redefs [impl/map->properties
                  (fn [data]
                    (reset! props data)
                    (orig data))]
      (try
        (#'impl/dbspec->connection
         {:subprotocol "h2"
          :subname "mem:"
          :classname "org.h2.Driver"
          :isolation-level :serializable
          :read-only true
          :schema "public"
          :tx-strategy :something
          :user "sa"})
        (catch Exception _)))
    (is (some? @props))
    (is (not (contains? @props :isolation-level)) "isolation-level should be filtered")
    (is (not (contains? @props :read-only)) "read-only should be filtered")
    (is (not (contains? @props :schema)) "schema should be filtered")
    (is (not (contains? @props :tx-strategy)) "tx-strategy should be filtered")
    (is (not (contains? @props :classname)) "classname should be filtered")
    (is (contains? @props :user) "user should be kept")))

(deftest prepared-statement-closed-on-param-failure
  ;; If set-params throws, the PreparedStatement should still be closed
  (with-open [conn (jdbc/connection h2-mem)]
    (let [raw (proto/connection conn)
          closed? (atom false)
          bomb (reify proto/ISQLType
                 (as-sql-type [_ _] (throw (RuntimeException. "param bomb")))
                 (set-stmt-parameter! [_ _ _ _]
                   (throw (RuntimeException. "param bomb"))))]
      ;; prepared-statement* creates the stmt, then calls set-params which throws.
      ;; The stmt should be closed, not leaked.
      (let [stmt-ref (atom nil)]
        (try
          (with-redefs [impl/set-params
                        (fn [conn stmt params]
                          (reset! stmt-ref stmt)
                          (throw (RuntimeException. "param bomb")))]
            (impl/prepared-statement* raw ["SELECT ? as foo" 1] {}))
          (catch RuntimeException _))
        (is (some? @stmt-ref) "Statement should have been created")
        (is (.isClosed ^java.sql.PreparedStatement @stmt-ref)
            "Statement should be closed after set-params failure")))))
