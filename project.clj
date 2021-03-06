(defproject clj-kafka "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0-alpha16"]
                 [org.clojure/core.async "0.3.442"]
                 [org.apache.kafka/kafka-clients "0.11.0.1"]
                 [org.apache.kafka/kafka_2.11 "0.11.0.1"]
                 [org.apache.kafka/kafka-streams "0.11.0.1"]
                 [com.taoensso/nippy "2.13.0"]
                 [http-kit "2.2.0"]
                 [org.clojure/data.json "0.2.6"]
                 [org.clojure/test.check "0.9.0"]
                 [ymilky/franzy "0.0.1"]
                 [ymilky/franzy-avro "0.0.1"]
                 [ymilky/franzy-admin "0.0.1"]
                 [ymilky/franzy-json "0.0.1"]]
  :dev {:resource-paths ["/Users/admin/.m2/repository/"]})



