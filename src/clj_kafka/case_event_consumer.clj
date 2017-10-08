(ns clj-kafka.case_event_consumer
  (:import org.apache.kafka.clients.consumer.ConsumerConfig
           org.apache.kafka.common.serialization.ByteArraySerializer
           org.apache.kafka.common.serialization.ByteArrayDeserializer
           org.apache.kafka.clients.consumer.KafkaConsumer
           org.apache.kafka.clients.producer.KafkaProducer
           org.apache.kafka.clients.producer.ProducerRecord)
  (:require [taoensso.nippy :as nippy]
            [clojure.core.async :as async]))

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

(def app-state (atom {:111 {:case/lifecycle-state :case/unarchived
                            :case/name "Paolo Maldini"}}))

(def consumer-case-event (doto (KafkaConsumer. c-cfg)
                           (.subscribe ["case-event"])))

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
  (println event "EVENT!!!!")
  (let [{:keys [:event/action :event/payload]} event
        fn-new-state (get action (validating-case-event-ks state))
        [is-event-valid? new-state] (fn-new-state payload)]
    (if is-event-valid?
      (if (= :event/screening action)
        (.send producer (ProducerRecord. "case-screening" (nippy/freeze event)))
        (.send producer (ProducerRecord. "case-valid" (nippy/freeze event))))
      (.send producer (ProducerRecord. "case-invalid" (nippy/freeze event))))
    new-state))

(def status (atom :running))

(defn start-consuming
  []
  (reset! status :running)
  (async/thread
    (while (= @status :running)
      (let [records (.poll consumer-case-event 100)]
        (doseq [record records]
          (let [event (-> record
                          (.value)
                          nippy/thaw)
                new-state (validate-event @app-state event)]
            (reset! app-state new-state)
            (println "After consuming:" event "I got this state:" @app-state))))))
  status)
@app-state
