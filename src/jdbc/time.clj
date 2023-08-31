(ns jdbc.time
  (:require
   [java-time]
   [jdbc.proto :refer [ISQLType ISQLResultSetReadColumn]]))

; java.time.LocalDate     - java.sql.Date
; java.time.LocalDateTime - java.sql.Timestamp
; java.time.LocalTime     - java.sql.Time

(extend-protocol ISQLType
  java.time.LocalDate
  (as-sql-type [this _conn]
    (java-time/sql-date this))
  (set-stmt-parameter! [this _conn stmt index]
    (.setDate stmt index
      (java-time/sql-date this)))

  java.time.LocalTime
  (as-sql-type [this _conn]
    (java-time/sql-time this))
  (set-stmt-parameter! [this _conn stmt index]
    (.setTime stmt index
      (java-time/sql-time this)))

  java.time.LocalDateTime
  (as-sql-type [this _conn]
    (java-time/sql-timestamp this))
  (set-stmt-parameter! [this _conn stmt index]
    (.setTimestamp stmt index
      (java-time/sql-timestamp this))))
  
(extend-protocol ISQLResultSetReadColumn
  ;
  java.sql.Timestamp
  (from-sql-type [this _conn _metadata _index]
    (java-time/local-date-time this))
  ;
  java.sql.Date
  (from-sql-type [this _conn _metadata _index]
    (java-time/local-date this))
  ;
  java.sql.Time
  (from-sql-type [this _conn _metadata _index]
    (java-time/local-time this)))