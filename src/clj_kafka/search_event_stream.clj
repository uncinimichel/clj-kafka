(ns clj-kafka.case_event_stream
  (:import org.apache.kafka.clients.consumer.ConsumerConfig
           org.apache.kafka.common.serialization.ByteArraySerializer
           org.apache.kafka.common.serialization.ByteArrayDeserializer
           org.apache.kafka.clients.consumer.KafkaConsumer
           org.apache.kafka.clients.producer.KafkaProducer
           org.apache.kafka.clients.producer.ProducerRecord
           [org.apache.kafka.common.serialization Serdes StringSerializer]
           [org.apache.kafka.streams KeyValue StreamsConfig KafkaStreams]
           org.apache.kafka.streams.processor.StateStoreSupplier
           [org.apache.kafka.streams.kstream Predicate ValueMapper ForeachAction KStreamBuilder KStream KeyValueMapper Transformer TransformerSupplier]
           org.apache.kafka.streams.state.Stores)
  (:require [franzy.serialization.serializers :as serializers]
            [taoensso.nippy :as nippy]
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
  (let [builder                           (KStreamBuilder.)
        case-e                            (.stream builder (into-array String ["case-e-test"]))
        _                                 (.. builder (addStateStore (common/build-store "case-store")
                                                                     (into-array String [])))
        _                                 ()
        [c-screening c-valid c-not-valid] (.. case-e
                                              (transform case-validator (into-array String ["case-store"]))
                                              (branch (into-array Predicate [screening? valid? non-valid?])))]
    (.to c-screening "c-screening")
    (.to c-valid "c-valid")
    (.to c-not-valid "c-not-valid")
    builder))

(comment
  (common/with-topology case-validator-topology
    (common/send-to "case-e-test" "2" event-s)))

