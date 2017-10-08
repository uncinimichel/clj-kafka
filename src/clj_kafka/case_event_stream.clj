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

(def state (atom {}))

(defn- get-current-case
  [store id]
  (.get store (name id)))

(defn- update-case
  [store id case]
  (.put store (name id) case))

(comment
  (:rules @state)
  ((:event/created (:rules @state)) {:case/id "111"
                                     :case/name "a neme"}))

(defn validating-case-event-ks
  "Given an :event/action and a state it returns a boolean to say if the action it is acceptable with the current state and a new state"
  [fn-store]
  {:event/created (fn [{:keys [:case/id :case/name]}]
                    (if-let [case-in-store (fn-store id)]
                      [false case-in-store]
                      [true (hash-map :case/lifecycle-state :case/unarchived
                                      :case/name name
                                      :case/id id)]))
   :event/screening (fn [{:keys [:case/id]}]
                      (let [case-in-store (fn-store id)]
                        (if (or (nil? case-in-store)
                                (= (:case/lifecycle-state case-in-store) :case/deleted))
                          [false case-in-store]
                          [true (assoc case-in-store :case/screening-state :case/screening)])))
   :event/screened (fn [{:keys [:case/id]}]
                     (let [case-in-store (fn-store id)]
                       (if (or (nil? case-in-store)
                               (= (:case/lifecycle-state case-in-store) :case/deleted))
                         [false case-in-store]
                         [true (assoc case-in-store :case/screening-state :case/screened)])))
   :event/deleted (fn [{:keys [:case/id]}]
                    (let [case-in-store (fn-store id)]
                      (if (or (= (:case/lifecycle-state case-in-store) :case/archived)
                              (= (:case/lifecycle-state case-in-store) :case/unarchived))
                        [true (assoc case-in-store :case/lifecycle-state :case/deleted)]
                        [false case-in-store])))})

(def case-validator
  (reify TransformerSupplier
    (get [_]
      (let [;state (atom {})
            case-store "case-store"]
        (reify Transformer
          (close [_]
            (.close (.getStateStore (:context @state) case-store)))
          (init [_ processor-context]
            (println "I am starting a Transformer!!!")
            (let [store (.getStateStore processor-context case-store)]
              (swap! state #(assoc %
                                   :context processor-context
                                   :rules (validating-case-event-ks (partial get-current-case store)))))) ;partial because I still don't have the case id!!
          (transform [_ k {:keys [:event/action :event/payload] :as v}]
            (println "TransformatinoSS!!!")
            (let [{:keys [rules context]} @state
                  {:keys [:case/id]}      payload
                  store                   (.getStateStore context case-store)
                                        ;                  _                       (update-investor-portfolio store v inc)
                  fn-new-case             (action rules) ; I have to finish the partial fn
                  [valid? new-case]       (fn-new-case payload)]
              (if valid?      
                (update-case store id new-case)
                (update-case store id new-case)) ;If not valid I don't need it but I don't care because the KS is returing the same case... but can you trust that?
              (KeyValue/pair (name id) (assoc v :case/valid? valid?))))
          (punctuate [_ t]))))))

(name :ciao)

(defn- check-marker-predicate [fn-mod]
  (reify Predicate
    (test [_ k {:keys [:case/valid?]}]
      (fn-mod valid?))))

(def valid?
  (check-marker-predicate identity))

(def non-valid?
  (check-marker-predicate not))

(def builder (KStreamBuilder.))

(def case-validator-topology
  (let [case-e                (.stream builder (into-array String ["case-e-test"]))
        _                     (.. builder (addStateStore (common/build-store "case-store")
                                                         (into-array String [])))
        [c-valid c-not-valid] (.. case-e
                                  (transform case-validator (into-array String ["case-store"]))
                                  (branch (into-array Predicate [valid? non-valid?])))]

    ;; Materialize streams
    (.to c-valid "c-valid")
    (.to c-not-valid "c-not-valid")
    builder))


(def kafka-streams
  (KafkaStreams. builder
                 (StreamsConfig. {StreamsConfig/APPLICATION_ID_CONFIG    "test-app-id"
                                  StreamsConfig/BOOTSTRAP_SERVERS_CONFIG "localhost:9092"                                  
                                  "producer.interceptor.classes"          "clj_kafka.common.MyProducerInterceptor"
                                  StreamsConfig/KEY_SERDE_CLASS_CONFIG (.getClass (Serdes/String))
                                  StreamsConfig/VALUE_SERDE_CLASS_CONFIG "clj_kafka.common.EdnSerde"})))
(def event-1 {:event/id :111
              :event/count 1
              :event/action :event/created
              :event/payload {:case/id :111
                              :case/lifecycle-state :case/archived
                              :case/name "a name"}})

(comment
  (.start kafka-streams)
  (.send producer (ProducerRecord. "case-e-test" "1" event-1))
  (.send producer (ProducerRecord. "case-s-test" "ciao" (nippy/freeze event)))
  (.send producer (ProducerRecord. "case-s-test" (nippy/freeze (get-in event [:event/payload :case/id])) (nippy/freeze event))))

(def p-cfg {"bootstrap.servers" "localhost:9092"})

(def producer (KafkaProducer. p-cfg
                              (StringSerializer.)
                              (serializers/edn-serializer)))
