(ns clj-kafka.db_consumer
  (:import org.apache.kafka.clients.consumer.ConsumerConfig
           org.apache.kafka.common.serialization.ByteArraySerializer
           org.apache.kafka.common.serialization.ByteArrayDeserializer
           org.apache.kafka.clients.consumer.KafkaConsumer)
  (:require [taoensso.nippy :as nippy]
            [clojure.core.async :as async]
            [clj-kafka.common :as common]
            [clojure.data.json :as json]))

(def c-cfg
  {"bootstrap.servers" "localhost:9092"
   "group.id" "consumer-db-group"
   "auto.offset.reset" "earliest"
   "enable.auto.commit" "true"
   "key.deserializer" ByteArrayDeserializer
   "value.deserializer" ByteArrayDeserializer})

(def consumer-db (doto (KafkaConsumer. c-cfg)
                   (.subscribe ["case-valid"])))

(def status (atom :running))

(defn fn-processing-record
  [record]
  (Thread/sleep 500)
  (println "Saving:" record "in the DB"))


(defn start-consuming
  []
  (reset! status :running)
  (async/thread
    (while (= @status :running)
      (let [records (.poll consumer-db 100)]
        (doseq [record records]
          (let [m (-> record
                      (.value)
                      nippy/thaw)]
            (fn-processing-record m))))))
                                        ;maybe checking that the event-id is not in the DB
  status)
