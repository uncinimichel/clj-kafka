(ns clj-kafka.common
  (:import [org.apache.kafka.common.serialization Serde Deserializer Serializer Serdes StringSerializer]
           [org.apache.kafka.clients.producer KafkaProducer ProducerConfig ProducerInterceptor ProducerRecord]
           [org.apache.kafka.streams.state QueryableStoreTypes Stores])
  (:require [franzy.serialization.deserializers :as deserializers]
            [franzy.serialization.serializers :as serializers]
            [org.httpkit.client :as http]
            [clojure.core.async :as async :refer [go go-loop put! take! <! >! <!! timeout chan alt! go]]))

(defn http-get [url]
  (let [c (chan)]
    (println "Calling this url:" url)
    (http/get url (fn [r] (put! c r)))
    c))

;;;
;;; Serialization stuff
;;;

(deftype NotSerializeNil [edn-serializer]
  Serializer
  (configure [_ configs isKey] (.configure edn-serializer configs isKey))
  (serialize [_ topic data]
    (when data (.serialize edn-serializer topic data)))
  (close [_] (.close edn-serializer)))

;; Can be global as they are thread-safe
(def serializer (NotSerializeNil. (serializers/edn-serializer)))
(def deserializer (deserializers/edn-deserializer))

(deftype EdnSerde []
  Serde
  (configure [this map b])
  (close [this])
  (serializer [this]
    serializer)
  (deserializer [this]
    deserializer))

(defn deserialize [serde payload]
  (.. serde
      (deserializer)
      (deserialize "" payload)))


(def output-results (atom {}))

(deftype MyProducerInterceptor []
  ProducerInterceptor
  (close [_])
  (configure [_ configs])
  (onAcknowledgement [_ metadata exception])
  (onSend [_ record]
    (let [topic (keyword (.topic record))]

                                        ;   (println "Icome here at keast!! topic:" topic)
                                        ;    (println "record" record)
                                        ;      (println "Interceptor topic:" {:key   (deserialize (Serdes/String) (.key record))
                                        ;                                    :value (deserialize deserializer (.value record))})
                                        ;      (swap! output-results             
      ;;        (fn [m t v] (update m t (fn [o n] (conj (vec o) n)) v))
      ;;        topic
      ;;        {:key   (deserialize (Serdes/String) (.key record))
      ;;         :value (deserialize deserializer (.value record))})
      record)))

(defn build-store
  ([name]
   (build-store name {:key-serde   (Serdes/String)
                      :value-serde (EdnSerde.)}))
  ([name {:keys [key-serde value-serde] :as serdes}]
   (..
    (Stores/create name)
    (withKeys key-serde)
    (withValues value-serde)
    (persistent)
    (build))))
