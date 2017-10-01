(ns clj-kafka.screening_consumer
  (:import org.apache.kafka.clients.consumer.ConsumerConfig
           org.apache.kafka.common.serialization.ByteArraySerializer
           org.apache.kafka.common.serialization.ByteArrayDeserializer
           org.apache.kafka.clients.consumer.KafkaConsumer
           org.apache.kafka.clients.producer.KafkaProducer
           org.apache.kafka.clients.producer.ProducerRecord)
  (:require [taoensso.nippy :as nippy]
            [clojure.core.async :as async]
            [clj-kafka.common :as common]
            [clojure.data.json :as json]))


(def p-cfg {"value.serializer" ByteArraySerializer
            "key.serializer" ByteArraySerializer
            "bootstrap.servers" "localhost:9092"})

(def producer (KafkaProducer. p-cfg))


(def c-cfg
  {"bootstrap.servers" "localhost:9092"
   "group.id" "consumer-screening-group"
   "auto.offset.reset" "earliest"
   "enable.auto.commit" "true"
   "key.deserializer" ByteArrayDeserializer
   "value.deserializer" ByteArrayDeserializer})


(def status (atom :running))

(def consumer-screening-case (doto (KafkaConsumer. c-cfg)
                               (.subscribe ["case-screening"])))

(def mock-url-results "http://www.mocky.io/v2/59cf936113000023060608e2")

(defn parse-response-to-json
  [response]
  (let [{:keys [body error]} response]
    (if error
      (println "An errorororroororor" error)
      (json/read-str body
                     :key-fn keyword))))

(def xf-screening-name
  (map (fn
         [event]
         (let [case-id (:case/id event)
               event-id (:event/id event)
               name (:case/name event)
               results (:results (parse-response-to-json (async/<!! (common/http-get mock-url-results))))]
           (assoc event :case/results results)))))

(def xf-sending-results
  (map (fn
         [event]
         (println "sending this over:" event)
         (.send producer (ProducerRecord. "case-event" (nippy/freeze event))))))

(def parallelism (+ (.availableProcessors (Runtime/getRuntime)) 1))

(defn start-consuming
  [consumer out status]
  (async/go
    (while (= @status :running)
      (let [records (.poll consumer 100)]
        (doseq [record records]
          (let [m (-> record
                      (.value)
                      nippy/thaw)]
            (println "Screening consumer" m)
            (async/>!! out m)))))
    (println "closing the screening consumer")
    (async/close! out)))

(defn screening-consumer-pipeline
  []
  (let [screening-chan (async/chan 1)
        out-screening-name (async/chan 1)
        out-sending-results (async/chan 1)]
    (reset! status :running)
    (start-consuming consumer-screening-case screening-chan status)
    (async/pipeline parallelism out-screening-name xf-screening-name screening-chan)
    (async/pipeline parallelism out-sending-results xf-sending-results out-screening-name)
    (async/go
      (while (= @status :running)
        (println "getting this from the out chan" (async/<!! out-sending-results)))
      status)))

;(screening-consumer-pipeline)
