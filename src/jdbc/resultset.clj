;; Copyright 2014-2015 Andrey Antukh <niwi@niwi.be>
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

(ns jdbc.resultset
  "ResultSet conversion functions."
  (:require 
   [clojure.string :as str]
   [jdbc.proto :as proto])
  (:import
   java.sql.ResultSetMetaData
   java.sql.ResultSet))

(defn result-set->lazyseq
  "Function that wraps result in a lazy seq. This function
  is part of public api but can not be used directly (you should pass
  this function as parameter to `query` function).

  Required parameters:
    rs: ResultSet instance.

  Optional named parameters:
    :identifiers -> function that is applied for column name
                    when as-arrays? is false
    :as-rows?    -> by default this function return a lazy seq of
                    records (map), but in certain circumstances you
                    need results as a lazy-seq of vectors. With this keywork
                    parameter you can enable this behavior and return a lazy-seq
                    of vectors instead of records (maps).
  "
  [conn ^ResultSet rs {:keys [identifiers as-rows? header?]
                       :or {identifiers str/lower-case
                            as-rows? false
                            header? false}
                       :as options}]
  (let [^ResultSetMetaData metadata (.getMetaData rs)
        idseq (range 1 (inc (.getColumnCount metadata)))
        labels (mapv (fn [^long i] (.getColumnLabel metadata i)) idseq)
        keyseq (mapv (comp keyword identifiers) labels)
        values (fn []
                 (mapv (fn [^long i]
                         (-> (.getObject rs i)
                             (proto/from-sql-type conn metadata i)))
                       idseq))
        rows (fn thisfn []
               (when (.next rs)
                 (cons (values) (lazy-seq (thisfn)))))
        records (fn thisfn []
                  (when (.next rs)
                    (-> (zipmap keyseq (values))
                        (cons (lazy-seq (thisfn))))))
        header (mapv identifiers labels)]
    (if-not as-rows?
      (records)
      (if-not header?
        (rows)
        (cons header (lazy-seq (rows)))))))

(defn result-set->vector
  "Function that evaluates a result into one clojure persistent
  vector. Accept same parameters as `result-set->lazyseq`."
  [conn ^ResultSet rs options]
  (vec (result-set->lazyseq conn rs options)))
