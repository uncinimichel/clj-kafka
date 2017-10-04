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

(def app-state (atom {:state/cases { :111 {:case/lifecycle-state :case/unarchived
                                           :case/name "Paolo Maldini"}}
                      :state/events {:111 true}}))

(def consumer-case-event (doto (KafkaConsumer. c-cfg)
                           (.subscribe ["case-event"])))

partition-all


(defn validating-case-event-ks
  "Given an :event/action and a state it returns a boolean to say if the action it is acceptable with the current state and a new state"
  [{:keys [:state/events :state/cases] :as state }]
  {:event/reply (fn [{:keys [:event/id]}]
                  (let [is-valid? (get :events id)]
                                        ;If the event is in there, means that I have been processing this event,
                                        ;So I am going to send back the state and if that event was valid or not.
                    [is-valid? state]))
   :event/created (fn [{:keys [:case/id :case/name]}]
                    (if (get cases id)
                      [false state]
                      [true (assoc-in state [:cases id] (hash-map :case/lifecycle-state :case/unarchived
                                                                  :case/name name
                                                                  :case/id id))]))
   :event/screening (fn [{:keys [:case/id]}]
                      (let [case-s (get cases id)]
                        (if (or (nil? case-s)
                                (= (:case/lifecycle-state case-s) :case/deleted))
                          [false state]
                          [true (assoc-in state [:cases id :case/screening-state] :case/screening)])))
   :event/screened (fn [{:keys [:case/id]}]
                     (let [case-s (get cases id)]
                       (if (or (nil? case-s)
                               (= (:case/lifecycle-state case-s) :case/deleted))
                         [false state]
                         [true (assoc-in state [:cases id :case/screening-state] :case/screened)])))
   :event/archived (fn [{:keys [:case/id]}]
                     (let [case-s (get cases id)]
                       (if (= (:case/lifecycle-state case-s) :case/deleted)
                         [false state]
                         [true (assoc-in state [:cases id :case/lifecycle-state] :case/archived)])))
   :event/updated (fn [{:keys [:case/id]}]
                    (let [case-s (get cases id)]
                      (if (= (:case/lifecycle-state case-s) :case/deleted)
                        [false state]
                        [true (assoc-in state [:cases id :case/lifecycle-state] :case/updated)])))
   :event/deleted (fn [{:keys [:case/id]}]
                    (let [case-s (get cases id)]
                      (if (or (= (:case/lifecycle-state case-s) :case/archived)
                              (= (:case/lifecycle-state case-s) :case/unarchived))
                        [true (assoc-in state [:cases id :case/lifecycle-state] :case/deleted)]
                        [false state])))})

(defn validate-event
  [state event]
  (println "This event is coming in:" event)
  (let [{:keys [:event/action :event/payload]} (if (get-in state [:events (:event/id event)])
                                                 {:event/action :event/reply
                                                  :event/payload event}
                                                 event)
        _ (println "Processing this action:" action)
        fn-new-state (action (validating-case-event-ks state))
        [is-event-valid? new-state] (fn-new-state payload)
        _ (println "The action was:" is-event-valid?)]
    (if is-event-valid?
      (if (= :event/screening action)
        (.send producer (ProducerRecord. "case-screening" (nippy/freeze event)))
        (.send producer (ProducerRecord. "case-valid" (nippy/freeze event))))
      (.send producer (ProducerRecord. "case-invalid" (nippy/freeze event))))
    new-state))

(comment
  (validate-event {:state/cases { :111 {:case/lifecycle-state :case/archived
                                        :case/name "Paolo Maldini"} }
                   :state/events {:111 true}}
                  {:event/id :111
                   :event/action :event/created
                   :event/payload {:case/id :112}}))

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
