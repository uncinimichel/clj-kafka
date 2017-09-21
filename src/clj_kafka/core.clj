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



(defn processing-case-ks
  "Given an action and a state it produces a new state"
  [case-s]
  {:archived (fn []
               (if (= (:lifecycle-state case-s) :unarchived)
                 (assoc case-s :lifecycle-state :archived)
                 case-s))
   :unarchived (fn []
                 (if (= (:lifecycle-state case-s) :archived)
                   (assoc case-s :lifecycle-state :unarchived)
                   case-s))
   :deleted (fn []
              (if (or (= (:lifecycle-state case-s) :archived)
                      (= (:lifecycle-state case-s) :unarchived))
                (assoc case-s :lifecycle-state :deleted)
                case-s))})

                                        ; 1 get a cmd
                                        ; 2 check if it is a valid command
                                        ; 3 VALID    --> update state, commit 
                                        ; 3 NOT-VALID --> send error
(defn processing-cmds
  [commands init-state]
  (loop [state init-state
         cmds commands
         good-cmds []
         bad-cmds []]
    (let [c (first cmds)]
      (if (nil? c)
        {:new-state state
         :good good-cmds
         :bad bad-cmds}
        (let [{:keys [id lifecycle-state]} c
              old-case-state (get state id)
              new-case-state ((lifecycle-state (processing-case-ks old-case-state)))]
          (if (= old-case-state new-case-state)
            (recur state
                   (rest cmds)
                   good-cmds
                   (conj bad-cmds c))
            (recur (assoc state id new-case-state)
                   (rest cmds)
                   (conj good-cmds c)
                   bad-cmds)))))))

(def cmds (gen/sample (s/gen :unq/cmd-lifecycle)))
(def init-state (gen/generate (s/gen :unq/state)))
(def r (processing-cmds cmds init-state))
(:state r)
(:good r)
(:bad r)
