(defproject funcool/clojure.jdbc "0.9.0"
  :description "clojure.jdbc is a library for low level, jdbc-based database access."
  :url "http://github.com/niwibe/clojure.jdbc"
  :license {:name "Apache 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0.txt"}
  :dependencies [[org.clojure/clojure "1.11.1" :scope "provided"]]
  :profiles
  {:dev
   {:dependencies [[com.h2database/h2 "1.4.192"]
                   [org.postgresql/postgresql "42.6.0"]
                   [hikari-cp "3.0.1"]
                   [org.clojure/data.json "2.4.0"]]
    :codeina {:sources ["src"]
              :exclude [jdbc.impl
                        jdbc.transaction
                        jdbc.types]
              :reader :clojure
              :target "doc/dist/latest/api"
              :src-uri "http://github.com/niwibe/clojure.jdbc/blob/master/"
              :src-uri-prefix "#L"}
    :plugins [[lein-ancient "0.6.10"]
              [funcool/codeina "0.5.0"]]}
   :bench {:source-paths ["bench/"]
           :main jdbc.bench
           :global-vars {*warn-on-reflection* true
                         *unchecked-math* :warn-on-boxed}
           :dependencies [[org.clojure/java.jdbc "0.5.8"]
                          [com.h2database/h2 "1.4.192"]
                          [criterium "0.4.4"]]}})
