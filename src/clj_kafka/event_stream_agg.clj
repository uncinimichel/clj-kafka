(ns clj-kafka.catringse_event_stream
  (:import org.apache.kafka.clients.consumer.ConsumerConfig
           org.apache.kafka.common.serialization.ByteArraySerializer
           org.apache.kafka.common.serialization.ByteArrayDeserializer
           org.apache.kafka.clients.consumer.KafkaConsumer
           org.apache.kafka.clients.producer.KafkaProducer
           org.apache.kafka.clients.producer.ProducerRecord
           [org.apache.kafka.common.serialization Serdes StringSerializer]
           [org.apache.kafka.streams KeyValue StreamsConfig KafkaStreams]
           org.apache.kafka.streams.processor.StateStoreSupplier
           [org.apache.kafka.streams.kstream Reducer KeyValueMapper Predicate ValueMapper ForeachAction KStreamBuilder KStream KeyValueMapper Transformer TransformerSupplier]
           org.apache.kafka.streams.state.Stores)
  (:require   [franzy.serialization.json.deserializers :as deserializers-j]
              [franzy.serialization.serializers :as serializers]
              [franzy.serialization.deserializers :as deserializers]
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

(def case-validator-topology
  (let [builder (KStreamBuilder.)
        ciao (->
              (.stream builder (into-array String ["dvt-custom-field-json"]))
              (.mapValues (reify ValueMapper
                            (apply [_ v]
                                        ;   (println "flat" (:CASE_ID v))
                              v)))
              (.groupBy (reify KeyValueMapper
                          (apply [_ k v]
                                        ;       (println "group by" k v)
                            (:CASE_ID v)))
                        (Serdes/String)
                        (Serdes/String))
              (.reduce (reify Reducer
                         (apply [_ agg new]
                           (println "agg" agg "new" new)
                           (str agg "-" new)))
                       "reduced-cfs")
              (.to (Serdes/String) (Serdes/String)  "grp-count"))]
    builder))

(def word-validator-topology
  (let [builder (KStreamBuilder.)
        ciao (->
              (.stream builder (into-array String ["words"]))
              (.flatMapValues (reify ValueMapper
                                (apply [_ v]
                                        ;     (println 1 (java.util.ArrayList. (clojure.string/split v #" ")))
                                  (clojure.string/split v #" "))))
              (.groupBy (reify KeyValueMapper
                          (apply [_ k v]
                                        ;             (println "In the group by" v)
                            v)))
                                        ;                (Serdes/String)
                                        ;               (Syerdes/Long))
              (.count "Counts")
              (.to (Serdes/String) (Serdes/Long)  "words-count"))]
    builder))


(comment
  (common/with-topology case-validator-topology)
  (common/with-topology word-validator-topology)
  
  )
;;    (common/send-to "case-e-test" "2" event-s)))

(def c-cfg
  {"bootstrap.servers" "localhost:9092"
   "group.id" "consumer-cfs"
   "auto.offset.reset" "earliest"
   "schema.registry.url" "http://localhost:8081"
   "enable.auto.commit" "true"})
                                        ;  "key.deserializer" "franzy.serialization.avro.deserializers.AvroDeserializer"
                                        ; "value.deserializer" "franzy.serialization.avro.deserializers.AvroDeserializer"})

(comment
  (def consumer-db (doto (KafkaConsumer. c-cfg (deserializers/avro-deserializer) (deserializers/avro-deserializer))
                     (.subscribe ["dvt-custom-field"])))
  )

(def status (atom :running))

(defn start-consuming
  []
  (reset! status :running)
  (async/thread
    (while (= @status :running)
      (let [records (.poll consumer-db 100)]
        (doseq [record records]
          (let [m (-> record
                      (.value))]
            (println "Saving:" m "in ES"))))))
  status)
