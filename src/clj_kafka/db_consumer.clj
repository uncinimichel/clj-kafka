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

(defn start-consuming
  []
  (reset! status :running)
  (async/go
    (while (= @status :running)
      (let [records (.poll consumer-db 100)]
        (doseq [record records]
          (let [m (-> record
                      (.value)
                      nippy/thaw)]
            (println "new CIAO!!!I got this event:" m "I am going to save it to the DB!!!"))))))
  status)


;(reset! status :no)
                                        ;@status
