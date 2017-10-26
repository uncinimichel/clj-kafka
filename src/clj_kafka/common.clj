(ns clj-kafka.common
  (:import
   [org.apache.kafka.connect.json JsonSerializer]
   [org.apache.kafka.common.serialization Serde Deserializer Serializer Serdes IntegerSerializer StringSerializer]
   [org.apache.kafka.clients.consumer KafkaConsumer ConsumerConfig]
   [org.apache.kafka.clients.producer KafkaProducer ProducerConfig ProducerInterceptor ProducerRecord]
   [org.apache.kafka.streams.state QueryableStoreTypes Stores]
   [org.apache.kafka.streams KafkaStreams StreamsConfig])
  (:require [abracad.avro :as avro]
            [franzy.serialization.avro.deserializers :as deserializers-a]
            [franzy.serialization.json.deserializers :as deserializers-j]
            [franzy.serialization.json.serializers :as serializers-j]
            [franzy.serialization.deserializers :as deserializers]
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

(deftype JsonSerde []
  Serde
  (configure [this map b])
  (close [this])
  (serializer [this]
    (serializers-j/json-serializer {:key-fn true}))
  (deserializer [this]
    (deserializers-j/json-deserializer {:key-fn true})))

(def stream-config 
  (StreamsConfig. {StreamsConfig/APPLICATION_ID_CONFIG    "test-app-id"
                   StreamsConfig/BOOTSTRAP_SERVERS_CONFIG "localhost:9092"
                   ConsumerConfig/AUTO_OFFSET_RESET_CONFIG "earliest"
                   StreamsConfig/DEFAULT_KEY_SERDE_CLASS_CONFIG (.getName (.getClass (Serdes/String)))
;;                   StreamsConfig/DEFAULT_VALUE_SERDE_CLASS_CONFIG (.getName (.getClass (Serdes/String)))}))
                   StreamsConfig/DEFAULT_VALUE_SERDE_CLASS_CONFIG "clj_kafka.common.JsonSerde"}))
                                        ;StreamsConfig/VALUE_SERDE_CLASS_CONFIG "clj_kafka.common.EdnSerde"}))

(defmacro with-topology
  [name & body]
  `(with-open [ks# (KafkaStreams. ~name stream-config)]
     ~@body
     (.cleanUp ks#)
     (.start ks#)
     (Thread/sleep 500000)))

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

(def schema (avro/parse-schema
             {:name "example", :type "record",
              :fields [{:name "ID" :type "string"}
                       {:name "VERSION"
                        :type "bytes"
                        :scale 0
                        :precision 64
                        :connect.version 1
                        :connect.parameters { :scale 0 }
                        :connect.name "org.apache.kafka.connect.data.Decimal"
                        :logicalType "decimal"
                        }
                       {:name "VALUE" :type "string"}
                       {:name "CASE_ID" :type "string"}
                       {:name "CUSTOM_FIELD_TYPE_ID" :type "string"}]})) 


(defn consumer-from
  [topic des-k ser-v]
  (let [config {ConsumerConfig/BOOTSTRAP_SERVERS_CONFIG "localhost:9092"
                ConsumerConfig/GROUP_ID_CONFIG "some-consumers"
                ConsumerConfig/AUTO_OFFSET_RESET_CONFIG "earliest"
                "enable.auto.commit" "true"
                "schema.registry.url" "http://localhost:8081"}]
    (with-open [c (KafkaConsumer. config
                                  des-k
                                  ser-v)]
      (let [_ (.subscribe c [topic])
            records (.poll c 100)]
        (doseq [r records]
          (clojure.pprint/pprint (-> r
                                     (.value))))))))


(comment
  (consumer-from "dvt-custom-field-json"
                 (deserializers/keyword-deserializer)
                 (deserializers-j/json-deserializer {:key-fn true})))

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

(let [schema (avro/parse-schema
              {:name "example", :type "record",
               :fields [{:name "left", :type "string"}
                        {:name "right", :type "long"}]
               :abracad.reader "vector"})]
  (->> ["foo" 31337]
       (avro/binary-encoded schema)
       (avro/decode schema)))
