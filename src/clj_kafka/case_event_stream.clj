(ns clj-kafka.case_event_stream
  (:import org.apache.kafka.clients.consumer.ConsumerConfig
           org.apache.kafka.common.serialization.ByteArraySerializer
           org.apache.kafka.common.serialization.ByteArrayDeserializer
           org.apache.kafka.clients.consumer.KafkaConsumer
           org.apache.kafka.clients.producer.KafkaProducer
           org.apache.kafka.clients.producer.ProducerRecord
           org.apache.kafka.common.serialization.Serdes
           [org.apache.kafka.streams StreamsConfig KafkaStreams]
           org.apache.kafka.streams.processor.StateStoreSupplier
           [org.apache.kafka.streams.kstream ForeachAction KStreamBuilder KStream KeyValueMapper]
           org.apache.kafka.streams.state.Stores)
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

(def builder (KStreamBuilder.))

(def k-stream 
  (-> builder
      (.stream (into-array String ["case-s-test"]))
      (.groupBy (reify KeyValueMapper
                  (apply [_ k v]
                    k)))
      (.count "case-counting")
      (.to (Serdes/String)
           (Serdes/Long)
           "case-n-t")))

(comment (.foreach (reify ForeachAction
                     (apply [_ k v]
                       (println "This is a K:" (nippy/thaw (.get k)))
                       (println "This is a V:" (nippy/thaw (.get v)))))))


(def kafka-streams
  (KafkaStreams. builder
                 (StreamsConfig. {StreamsConfig/APPLICATION_ID_CONFIG    "test-app-id"
                                  StreamsConfig/BOOTSTRAP_SERVERS_CONFIG "localhost:9092"
                                  StreamsConfig/KEY_SERDE_CLASS_CONFIG org.apache.kafka.common.serialization.Serdes$StringSerde
                                  StreamsConfig/VALUE_SERDE_CLASS_CONFIG org.apache.kafka.common.serialization.Serdes$StringSerde})))

(def state {:state/cases { :111 {:case/lifecycle-state :case/archived
                                 :case/name "Paolo Maldini"} }
            :state/events {:111 true}})

(def event {:event/id :111
            :event/action :event/created
            :event/payload {:case/id :111}})

(comment
  (.start kafka-streams)
  (.send producer (ProducerRecord. "case-s-test" "ciao" "Michel123"))
  (.send producer (ProducerRecord. "case-s-test" (nippy/freeze (get-in event [:event/payload :case/id])) (nippy/freeze event))))



(def p-cfg {"value.serializer" "org.apache.kafka.common.serialization.StringSerializer"
            "key.serializer" "org.apache.kafka.common.serialization.StringSerializer"
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
  (doseq [e (take 10 ss/many-events)]
                                        ;    (clojure.pprint/pprint e)))    
    (.send producer (ProducerRecord. "case-event" (nippy/freeze e))))) 

                                        ;((filter #(= (get-in % [:event/payload :case/name]) "")
                                        ;         (take 10 ss/many-events))


                                        ; (clojure.pprint/pprint @case-event-consumer/app-state)
                                        ;ss/many-events
(comment
  (repeatedly 1 #(.send producer (ProducerRecord. "case-screening" (nippy/freeze e-screening)))))
