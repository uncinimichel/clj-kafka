(ns clj-kafka.specs
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]))

                                        ; RESULT
(s/def :result/id string?)
(s/def :result/name string?)

                                        ; CASE
(s/def :case/id string?)
(s/def :case/name string?)
(s/def :case/creator string?)
(s/def :case/owner string?)
(s/def :case/modifier string?)
(s/def :case/assigned string?)
(s/def :case/last-screened-date string?)


                                        ; EVENT

(s/def :event/id uuid?)
(def c (gen/generate (s/gen :event/id)))
(s/def :event/action #{:event/created :event/archived :event/deleted :event/screening :event/screened :event/reply})
(s/def :event/payload
  (s/keys :req [:case/id :case/name :case/results]))

(s/def ::k-event
  (s/keys :req [:event/id
                :event/action
                :event/payload]))


(s/def ::result
  (s/keys :req [:result/id :result/name]))

(s/def :case/screening-state #{:case/anonymous :case/initial :case/ongoing})
(s/def :case/lifecycle-state #{:case/archived :case/unarchived :case/deleted})
(s/def :case/provider (s/coll-of #{:case/world-check :case/passport-check :case/media-check} :distinct true))
(s/def :case/results (s/coll-of ::result))

(s/def :case/outstanding-actions boolean?)

(s/def ::case
  (s/keys :req [:case/id :case/creator :case/owner :case/screening-state :case/lifecycle-state :case/outstanding-actions]
          :opt [:case/modifier :case/assigned :case/last-screened-date :case/provider ::result]))

(def a-k-e (gen/generate (s/gen ::k-event)))
(def a-c (gen/sample (s/gen ::case)))

                                        ; STATE

(s/def :state/cases (s/map-of :case/id ::case))
(s/def :state/events (s/map-of :event/id true?))
(s/def ::state
  (s/keys :req [:state/cases :state/events]))

(def a-state (gen/generate (s/gen ::state)))

                                        ; Create many case

(def many-events (gen/sample (s/gen ::k-event) 100))


(def many-creation-events (filter #(= (:event/action %) :event/created)
                                  (gen/sample (s/gen ::k-event) 100)))

(def all-created-cases-ids (map #(get-in % [:event/payload :case/id])
                                many-creation-events))
