(ns clj-kafka.core
  (:import org.apache.kafka.clients.consumer.ConsumerConfig
           org.apache.kafka.common.serialization.ByteArraySerializer
           org.apache.kafka.common.serialization.ByteArrayDeserializer
           org.apache.kafka.clients.consumer.KafkaConsumer
           org.apache.kafka.clients.producer.KafkaProducer
           org.apache.kafka.clients.producer.ProducerRecord)
  (:require [taoensso.nippy :as nippy]
            [clojure.core.async :as async :refer [go go-loop put! take! <! >! <!! timeout chan alt! go]]))



(def p-cfg {"value.serializer" ByteArraySerializer
            "key.serializer" ByteArraySerializer
            "bootstrap.servers" "localhost:9092"})

(def producer (KafkaProducer. p-cfg))

(.send producer (ProducerRecord. "test" (nippy/freeze "ciao")))


(def c-cfg
  {"bootstrap.servers" "localhost:9092"
   "group.id" "avg-rate-consumer"
   "auto.offset.reset" "earliest"
   "enable.auto.commit" "false"
   "key.deserializer" ByteArrayDeserializer
   "value.deserializer" ByteArrayDeserializer})

(def consumer (doto (KafkaConsumer. c-cfg)
                (.subscribe ["test"])))

(go
  (while true
    (let [records (.poll consumer 100)]
      (doseq [record records]
        (let [m (-> record
                    (.value)
                    nippy/thaw)]
          (println m))))))
