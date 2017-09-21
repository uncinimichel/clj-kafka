(ns clj-kafka.specs
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]))

                                        ; Case spec
(s/def ::id #{:111 :222 :333 :444 :555})
(s/def ::name string?)

(s/def ::creator string?)
(s/def ::owner string?)
(s/def ::modifier string?)
(s/def ::assigned string?)
(s/def ::last-screened-date string?)

(s/def :unq/result
  (s/keys :req-un [::id ::name]))

(s/def ::screening-state #{:anonymous :initial :ongoing})
(s/def ::lifecycle-state #{:archived :unarchived :deleted})
(s/def ::provider (s/coll-of #{:world-check :passport-check :media-check} :distinct true))
(s/def ::results (s/coll-of :unq/result))

(s/def ::outstanding-actions boolean?)

(s/def ::case
  (s/keys :req-un [::id ::creator ::owner ::screening-state ::lifecycle-state ::outstanding-actions]))
                                        ; :opt-un [::modifier ::assigned ::last-screened-date ::provider]))


(s/def :unq/state (s/map-of ::id ::case))
(s/def :unq/cmd-lifecycle (s/keys :req-un [::id ::lifecycle-state]))
