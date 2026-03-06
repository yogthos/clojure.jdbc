(ns jdbc.util-test
  (:require [jdbc.util :as util]
            [clojure.test :refer :all]))

(deftest update-sql-blank-where
  ;; Empty string where clause should not produce invalid "WHERE " SQL
  (let [result (util/update-sql :person {:zip 94540} ["" 1] identity)]
    (is (not (re-find #"WHERE\s*$" (first result)))
        "Should not have a dangling WHERE clause"))
  ;; nil where clause should also omit WHERE
  (let [result (util/update-sql :person {:zip 94540} [nil] identity)]
    (is (not (re-find #"WHERE" (first result)))
        "nil where should omit WHERE entirely")))

(deftest update-sql-valid-where
  (let [result (util/update-sql :person {:zip 94540} ["zip = ?" 94546] identity)]
    (is (= "UPDATE person SET zip = ? WHERE zip = ?" (first result)))
    (is (= [94540 94546] (rest result)))))
