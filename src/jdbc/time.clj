(ns jdbc.time
  (:require
   [jdbc.proto :refer [ISQLType ISQLResultSetReadColumn]])
  (:import
   java.sql.Date
   java.sql.Time
   java.sql.Timestamp))

; java.time.LocalDate     - java.sql.Date
; java.time.LocalDateTime - java.sql.Timestamp
; java.time.LocalTime     - java.sql.Time

(extend-protocol ISQLType
  java.time.LocalDate
  (as-sql-type [this _conn]
    (Date/valueOf this))
  (set-stmt-parameter! [this _conn stmt index]
    (.setDate stmt index
      (Date/valueOf this)))

  java.time.LocalTime
  (as-sql-type [this _conn]
    (Time/valueOf this))
  (set-stmt-parameter! [this _conn stmt index]
    (.setTime stmt index
      (Time/valueOf this)))

  java.time.LocalDateTime
  (as-sql-type [this _conn]
    (Timestamp/valueOf this))
  (set-stmt-parameter! [this _conn stmt index]
    (.setTimestamp stmt index
      (Timestamp/valueOf this))))
  
(extend-protocol ISQLResultSetReadColumn
  ;
  java.sql.Timestamp
  (from-sql-type [this _conn _metadata _index]
    (.toLocalDateTime this))
  ;
  java.sql.Date
  (from-sql-type [this _conn _metadata _index]
    (.toLocalDate this))
  ;
  java.sql.Time
  (from-sql-type [this _conn _metadata _index]
    (.toLocalTime this)))
