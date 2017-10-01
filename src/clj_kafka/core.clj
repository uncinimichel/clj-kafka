(ns clj-kafka.core
  (:import org.apache.kafka.clients.consumer.ConsumerConfig
           org.apache.kafka.common.serialization.ByteArraySerializer
           org.apache.kafka.common.serialization.ByteArrayDeserializer
           org.apache.kafka.clients.consumer.KafkaConsumer
           org.apache.kafka.clients.producer.KafkaProducer
           org.apache.kafka.clients.producer.ProducerRecord)
  (:require [taoensso.nippy :as nippy]
            [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]
            [clj-kafka.specs :as ss]
            [clojure.core.async :as async]
            [clj-kafka.common :as common]
            [clojure.data.json :as json]
            [clj-kafka.screening_consumer :as s-consumer]
            [clj-kafka.db_consumer :as db-consumer]
            [clj-kafka.es_consumer :as es-consumer]
            [clj-kafka.notification_consumer :as notification-consumer]
            [clj-kafka.case_invalid_consumer :as case-invalid-consumer]))

(def p-cfg {"value.serializer" ByteArraySerializer
            "key.serializer" ByteArraySerializer
            "bootstrap.servers" "localhost:9092"})

(def producer (KafkaProducer. p-cfg))

(def c-cfg
  {"bootstrap.servers" "localhost:9092"
   "group.id" "consumer-case-event-group"
   "auto.offset.reset" "earliest"
   "enable.auto.commit" "true"
   "key.deserializer" ByteArrayDeserializer
   "value.deserializer" ByteArrayDeserializer})

(def status (atom :running))

(def consumer-case-event (doto (KafkaConsumer. c-cfg)
                           (.subscribe ["case-event"])))


(def init-state
  {
   :111 {:case/lifecycle-state :case/unarchived
         :case/name "Paolo Maldini"}})

(def e-create1  {:event/action :event/created
                 :event/payload {:case/id :6969
                                 :case/name "Abate"}})

(def e-create2  {:event/action :event/created
                 :event/payload {:case/id :9696
                                 :case/name "Silva"}})

(def e-screening  {:event/action :event/screening
                   :event/payload {:case/id :111}})

(def e-deleted  {:event/action :event/deleted
                 :event/payload {:case/id :111}})

(def e-no-valid  {:event/action :event/deleted
                  :event/payload {:case/id :no-there}})


(defn validating-case-event-ks
  "Given an :event/action and a state it returns a boolean to say if the action it is acceptable with the current state and a new state"
  [state]
  {:event/created (fn [{:keys [:case/id :case/name]}]
                    (if (get state id)
                      [false state]
                      [true (assoc state id (hash-map :case/lifecycle-state :case/unarchived
                                                      :case/name name
                                                      :case/id id))]))
   :event/screening (fn [{:keys [:case/id]}]
                      (let [case-s (get state id)]
                        (if (or (nil? case-s)
                                (= (:case/lifecycle-state case-s) :case/deleted))
                          [false state]
                          [true (assoc-in state [id :case/screening-state] :case/screening)])))
   :event/screened (fn [{:keys [:case/id]}]
                     (let [case-s (get state id)]
                       (if (or (nil? case-s)
                               (= (:case/lifecycle-state case-s) :case/deleted))
                         [false state]
                         [true (assoc-in state [id :case/screening-state] :case/screened)])))
   :event/deleted (fn [{:keys [:case/id]}]
                    (let [case-s (get state id)]
                      (if (or (= (:case/lifecycle-state case-s) :case/archived)
                              (= (:case/lifecycle-state case-s) :case/unarchived))
                        [true (assoc-in state [id :case/lifecycle-state] :case/deleted)]
                        [false state])))})

(defn validate-event
  [state event]
  (let [{:keys [:event/action :event/payload]} event
        fn-new-state (action (validating-case-event-ks state))
        [is-event-valid? new-state] (fn-new-state payload)]
    (println "this event: " event "is or not valid? " is-event-valid?)
    (if is-event-valid?
      (if (= :event/screening action)
        (.send producer (ProducerRecord. "case-screening" (nippy/freeze event)))
        (.send producer (ProducerRecord. "case-valid" (nippy/freeze event))))
      (.send producer (ProducerRecord. "case-invalid" (nippy/freeze event))))
    new-state))

(defn lazy-consumer [consumer]
  (lazy-seq
   (let [records (.poll consumer 100)]
     (concat records (lazy-consumer consumer)))))

(def min-batch-size 1000)

(defn start-consuming-case-event
  [init-state]
  (async/go
    (while true
      (let [records (.poll consumer-case-event 100)]
        (doseq [record records]
          (let [m (-> record
                      (.value)
                      nippy/thaw)]
            (println m)))))))


(comment
  (start-consuming-case-event init-state)
  (.send producer (ProducerRecord. "case-event" (nippy/freeze {:ciao "mamma"}))))

(comment
  (repeatedly 1 #(.send producer (ProducerRecord. "case-screening" (nippy/freeze e-screening)))))

(comment
  (def start-consumers
    (let [s-status (s-consumer/screening-consumer-pipeline)
          es-status (es-consumer/start-consuming)
          db-status (db-consumer/start-consuming)
          notification-status (notification-consumer/start-consuming)
          case-invalid-status (case-invalid-consumer/start-consuming)]
      [s-status es-status db-status notification-status case-invalid-status])))

(comment
  (reset! s-c-status :no))

(comment
  (def new-state (reduce validate-event
                         init-state
                         [e-screening])))
