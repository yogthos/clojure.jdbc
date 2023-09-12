(ns jdbc.util
  (:require
   [clojure.string :as str])
  (:import java.util.Locale))

(defn lower-case
  "Converts a string to lower case in the US locale to avoid problems in
  locales where the lower case version of a character is not a valid SQL
  entity name (e.g., Turkish)."
  [^String s]
  (.toLowerCase s (Locale/US)))

(defn as-sql-name
  "Given a naming strategy function and a keyword or string, return
   a string per that naming strategy.
   A name of the form x.y is treated as multiple names, x, y, etc,
   and each are turned into strings via the naming strategy and then
   joined back together so x.y might become `x`.`y` if the naming
   strategy quotes identifiers with `."
  [f x]
  (let [n (name x)
        i (.indexOf n (int \.))]
    (if (= -1 i)
      (f n)
      (str/join "." (map f (.split n "\\."))))))

(defn table-str
  "Transform a table spec to an entity name for SQL. The table spec may be a
  string, a keyword or a map with a single pair - table name and alias."
  [table entities]
  (let [entities (or entities identity)]
    (if (map? table)
      (let [[k v] (first table)]
        (str (as-sql-name entities k) " " (as-sql-name entities v)))
      (as-sql-name entities table))))

(defn kv-sql
  "Given a sequence of column name keys and a matching sequence of column
  values, and an entities mapping function, return a sequence of SQL fragments
  that can be joined for part of an UPDATE SET or a SELECT WHERE clause.
  Note that we pass the appropriate operator for NULL since it is different
  in each case."
  [ks vs entities null-op]
  (map (fn [k v]
         (str (as-sql-name entities k)
              (if (nil? v) null-op " = ?")))
       ks vs))

(defn update-sql
  "Given a table name, a map of columns to set, a optional map of columns to
  match, and an entities, return a vector of the SQL for that update followed
  by its parameters. Example:
    (update :person {:zip 94540} [\"zip = ?\" 94546] identity)
  returns:
    [\"UPDATE person SET zip = ? WHERE zip = ?\" 94540 94546]"
  [table set-map [where & params] entities]
  (let [ks (keys set-map)
        vs (vals set-map)]
    (-> (str "UPDATE " (table-str table entities)
             " SET " (str/join
                      ","
                      (kv-sql ks vs entities " = NULL"))
             (when where " WHERE ")
             where)
        (cons (concat (remove nil? vs) params))
        (vec))))