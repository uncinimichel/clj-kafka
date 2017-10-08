(ns clj-kafka.common
  (:import [org.apache.kafka.common.serialization Serde Deserializer Serializer Serdes StringSerializer]
           [org.apache.kafka.clients.producer KafkaProducer ProducerConfig ProducerInterceptor ProducerRecord]
           [org.apache.kafka.streams.state QueryableStoreTypes Stores]
           [org.apache.kafka.streams KafkaStreams StreamsConfig])
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

                                        ; Maybe for logs???

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

                                        ; Stuff useful for doing stuff :D

(def stream-config 
  (StreamsConfig. {StreamsConfig/APPLICATION_ID_CONFIG    "test-app-id"
                   StreamsConfig/BOOTSTRAP_SERVERS_CONFIG "localhost:9092"                                  
                   "producer.interceptor.classes"          "clj_kafka.common.MyProducerInterceptor"
                   StreamsConfig/KEY_SERDE_CLASS_CONFIG (.getClass (Serdes/String))
                   StreamsConfig/VALUE_SERDE_CLASS_CONFIG "clj_kafka.common.EdnSerde"}))
 
(defmacro with-topology
  [name & body]
  `(with-open [ks# (KafkaStreams. ~name stream-config)]
     ~@body
     (.cleanUp ks#)
     (.start ks#)
                                        ;give him some seconds before close KS so it can produce
        (Thread/sleep 5000)))

(defn send-to
  [topic k v]
  (let [config {ProducerConfig/BOOTSTRAP_SERVERS_CONFIG "localhost:9092"
                ProducerConfig/ACKS_CONFIG              "all"
                ProducerConfig/RETRIES_CONFIG           "1"}]
    (with-open [producer (KafkaProducer. config
                                         (StringSerializer.)
                                         (serializers/edn-serializer))]
      (.get (.send producer (ProducerRecord. (name topic) k v)))
      (.flush producer))))

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
