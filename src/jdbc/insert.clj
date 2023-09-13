(ns jdbc.insert
  (:require 
   [clojure.string :as str]
   [jdbc.impl :as impl]
   [jdbc.util :refer [as-sql-name table-str lower-case]]
   [jdbc.resultset :as resultset])
  (:import
   [java.sql PreparedStatement]))

(defn sql-stmt?
  "Given an expression, return true if it is either a string (SQL) or a
  PreparedStatement."
  [expr]
  (or (string? expr) (instance? PreparedStatement expr)))

(defn db-do-execute-prepared-return-keys
  "Executes a PreparedStatement, and (attempts to) return any generated keys.
  
    Supports :multi? which causes a full result set sequence of keys to be
    returned, and assumes the param-group is a sequence of parameter lists,
    rather than a single sequence of parameters.
  
    Also supports :row-fn and, if :multi? is truthy, :result-set-fn"
  [conn sql param-group opts]
  (with-open [stmt (impl/prepared-statement* conn sql opts)]
    (let [{:keys [as-arrays? multi? row-fn] :as opts} (merge {:row-fn identity} opts)
         exec-and-return-keys
         (^{:once true} fn* []
                            (let [counts (if multi?
                                           (.executeBatch stmt)
                                           (vector (.executeUpdate stmt)))]
                              (try
                                (let [rs (.getGeneratedKeys stmt)
                                      result (cond multi?
                                                   (resultset/result-set->vector conn rs opts)
                                                   as-arrays?
                                                   ((^:once fn* [rs]
                                                                (list (first rs)
                                                                      (row-fn (second rs))))
                                                    (resultset/result-set->lazyseq conn rs opts))
                                                   :else
                                                   (row-fn (first (resultset/result-set->lazyseq conn rs opts))))]
                       ;; sqlite (and maybe others?) requires
                       ;; record set to be closed
                                  (.close rs)
                                  (or result (first counts)))
                                (catch Exception _
                       ;; assume generated keys is unsupported and return counts instead:
                                  (let [result-set-fn (or (:result-set-fn opts) doall)]
                                    (result-set-fn (map row-fn counts)))))))]
     
       (if multi?
         (doseq [params param-group]
           (impl/set-params conn stmt params)
           (.addBatch stmt))
         (impl/set-params conn stmt param-group))
       (exec-and-return-keys))))

(defn db-do-prepared-return-keys
  "Executes an (optionally parameterized) SQL prepared statement on the
  open database connection. The param-group is a seq of values for all of
  the parameters. Return the generated keys for the (single) update/insert."
  ([conn sql-params]
   (if (map? sql-params)
     (db-do-prepared-return-keys conn sql-params)
     (db-do-prepared-return-keys conn sql-params {})))
  ([conn sql-params opts]
   (let [[sql & params] (if (sql-stmt? sql-params) (vector sql-params) (vec sql-params))]
     (db-do-execute-prepared-return-keys conn sql params opts))))

(defn insert-helper
  "Given a (connected) database connection and some SQL statements (for multiple
   inserts), run a prepared statement on each and return any generated keys.
   Note: we are eager so an unrealized lazy-seq cannot escape from the connection."
  [conn stmts opts]
  (let [{:keys [result-set-fn]} opts
        per-statement (fn [stmt] (db-do-prepared-return-keys conn stmt opts))]
    (if result-set-fn
      (result-set-fn (map per-statement stmts))
      (seq (mapv per-statement stmts)))))

(defn col-str
  "Transform a column spec to an entity name for SQL. The column spec may be a
  string, a keyword or a map with a single pair - column name and alias."
  [col entities]
  (if (map? col)
    (let [[k v] (first col)]
      (str (as-sql-name entities k) " AS " (as-sql-name entities v)))
    (as-sql-name entities col)))

(defn insert-single-row-sql
  "Given a table and a map representing a row, return a vector of the SQL needed for
  the insert followed by the list of column values. The entities function specifies
  how column names are transformed."
  [table row entities]
  (let [ks (keys row)]
    (into [(str "INSERT INTO " (table-str table entities) " ( "
                (str/join ", " (map (fn [col] (col-str col entities)) ks))
                " ) VALUES ( "
                (str/join ", " (repeat (count ks) "?"))
                " )")]
          (vals row))))

(defn insert-rows!
  "Given a database connection, a table name, a sequence of rows, and an options
  map, insert the rows into the database."
  [conn table rows opts]
  (let [{:keys [entities] :as opts}
        (merge {:entities identity :identifiers lower-case :keywordize? true} opts)
        sql-params (map (fn [row]
                          (when-not (map? row)
                            (throw (IllegalArgumentException. "insert! / insert-multi! called with a non-map row")))
                          (insert-single-row-sql table row entities)) rows)]
    (insert-helper conn sql-params opts)))



