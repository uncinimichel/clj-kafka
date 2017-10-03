(ns clj-kafka.core
  (:import org.apache.kafka.clients.consumer.ConsumerConfig
           org.apache.kafka.common.serialization.ByteArraySerializer
           org.apache.kafka.common.serialization.ByteArrayDeserializer
           org.apache.kafka.clients.consumer.KafkaConsumer
           org.apache.kafka.clients.producer.KafkaProducer
           org.apache.kafka.clients.producer.ProducerRecord)
  (:require [taoensso.nippy :as nippy]
            [clj-kafka.specs :as ss]
            [clojure.core.async :as async]
            [clj-kafka.common :as common]
            [clojure.data.json :as json]
            [clj-kafka.screening_consumer :as s-consumer]
            [clj-kafka.db_consumer :as db-consumer]
            [clj-kafka.es_consumer :as es-consumer]
            [clj-kafka.notification_consumer :as notification-consumer]
            [clj-kafka.case_invalid_consumer :as case-invalid-consumer]
            [clj-kafka.case_event_consumer :as case-event-consumer]))

(def p-cfg {"value.serializer" ByteArraySerializer
            "key.serializer" ByteArraySerializer
            "bootstrap.servers" "localhost:9092"})

(def producer (KafkaProducer. p-cfg))

(def e-create1  {:event/id :unique-id-1
                 :event/action :event/created
                 :event/payload {:case/id :6969
                                 :case/name "Abate"}})

(def e-create2  {:event/id :unique-id-2
                 :event/action :event/created
                 :event/payload {:case/id :9696
                                 :case/name "Silva"}})

(def e-screening  {:event/id :unique-id-3
                   :event/action :event/screening
                   :event/payload {:case/id :111}})

(def e-deleted  {:event/id :unique-id-4
                 :event/action :event/deleted
                 :event/payload {:case/id :111}})

(def e-no-valid  {:event/id :unique-id-5
                  :event/action :event/deleted
                  :event/payload {:case/id :no-there}})

(comment
  (def start-consumers
    (let [c-status (case-event-consumer/start-consuming)
          s-status (s-consumer/screening-consumer-pipeline)
          es-status (es-consumer/start-consuming)
          db-status (db-consumer/start-consuming)
          notification-status (notification-consumer/start-consuming)
          case-invalid-status (case-invalid-consumer/start-consuming)]
      [c-status s-status es-status db-status notification-status case-invalid-status])))

(comment
  (reset! s-c-status :no))

(comment
  (doseq [e ss/many-events]
    (.send producer (ProducerRecord. "case-event" (nippy/freeze e)))))

(comment
  (repeatedly 1 #(.send producer (ProducerRecord. "case-screening" (nippy/freeze e-screening)))))

                                        ;(count ss/many-creation-events)


