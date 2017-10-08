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

(defn- get-current-case
  [store id]
  (.get store (name id)))

(defn- update-case
  [store id case]
  (.put store (name id) case))

(comment
  @state
  (:rules @state)q
  (.getStateStore (:context @state) "case-store")
  ((:event/created (:rules @state)) {:case/id "111"
                                     :case/name "a neme"}))

(defn validating-case-event-ks
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
      (let [state (atom {})
            case-store "case-store"]
        (reify Transformer
          (close [_]
            (.close (.getStateStore (:context @state) case-store)))
          (init [_ processor-context]
            (let [store (.getStateStore processor-context case-store)]
              (swap! state #(assoc %
                                   :context processor-context
                                   :rules (validating-case-event-ks (partial get-current-case store))))))
          (transform [_ k {:keys [:event/action :event/payload] :as v}]
            (let [{:keys [rules context]} @state
                  {:keys [:case/id]}      payload
                  store                   (.getStateStore context case-store)
                  _                       (println "before:" (get-current-case store id))
                  fn-new-case             (action rules) 
                  [valid? new-case]       (fn-new-case payload)
                  _                       (println "Was this event valid?" valid? new-case)
                  _                       (update-case store id new-case)
                  _                       (println "after:" (get-current-case store id))]
              (KeyValue/pair (name id) (assoc new-case
                                              :event/valid? valid?
                                              :event/action action))))
          (punctuate [_ t]))))))

(defn- check-marker-predicate [fn-mod]
  (reify Predicate
    (test [_ k {:keys [:event/valid?]}]
      (fn-mod valid?))))

(defn- check-action-predicate [my-action]
  (reify Predicate
    (test [_ k {:keys [:event/action]}]
      (= my-action action))))

(def valid?
  (check-marker-predicate identity))

(def non-valid?
  (check-marker-predicate not))

(def screening?
  (and valid?
       (check-action-predicate :event/screening)))

(def case-validator-topology
  (let [builder                           (KStreamBuilder.)
        case-e                            (.stream builder (into-array String ["case-e-test"]))
        _                                 (.. builder (addStateStore (common/build-store "case-store")
                                                                     (into-array String [])))
        [c-screening c-valid c-not-valid] (.. case-e
                                              (transform case-validator (into-array String ["case-store"]))
                                              (branch (into-array Predicate [screening? valid? non-valid?])))]
    ;; Materialize streams
    (.to c-screening "c-screening")
    (.to c-valid "c-valid")
    (.to c-not-valid "c-not-valid")
    builder))

 (def event-c {:event/id :111
               :event/count 1
               :event/action :event/created
               :event/payload {:case/id :999
                               :case/lifecycle-state :case/archived
                               :case/name "a name"}})

 (def event-s {:event/id :111
               :event/count 1
               :event/action :event/screening
               :event/payload {:case/id :999}})

 (def event-d {:event/id :111
               :event/count 1
               :event/action :event/deleted
               :event/payload {:case/id :313}})

 (comment
   (common/with-topology case-validator-topology
     (common/send-to "case-e-test" "2" event-s))
   )

 
