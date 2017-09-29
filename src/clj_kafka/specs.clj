(ns clj-kafka.specs
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]))
(def ciao 3)

                                        ; Case spec
(s/def ::id string?)

::id

(s/def :event/id string?)
(s/def :case/id string?)

(s/def ::event-id string?)
(s/def ::case-id string?)

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
  (s/keys :req-un [::id ::creator ::owner ::screening-state ::lifecycle-state ::outstanding-actions]
          :opt-un [::modifier ::assigned ::last-screened-date ::provider]))

(s/def :event/action #{:archived :deleted :updated :screening})

(s/def ::event
  (s/keys :req [:event/id
                :case/id
                :event/action]))

(println (gen/generate (s/gen ::event)))

(s/def :unq/state (s/map-of ::id ::case))

(s/def :unq/archive-cmd (s/map-of ::id ::case))

(defn is-cmd-valid?
  [case-s]
  (println case-s)
  {:archived (fn []
               (= (:lifecycle-state case-s) :unarchived))
   :unarchived (fn []
                 (= (:lifecycle-state case-s) :archived))
   :deleted (fn []
              (or (= (:lifecycle-state case-s) :archived)
                  (= (:lifecycle-state case-s) :unarchived)))})



(defn processing-case-ks
  "Given an action and a state it produces a new state"
  [case-s]
  {:archived (fn [{:keys [:lifecycle-state]}])
   :unarchived (fn [{:keys [:lifecycle-state]}])
   :deleted (fn[{:keys [:lifecycle-state]}])})

(defn processing-cmds
  [cmds state]
  (map (fn [{:keys [id cmd]}]
         ((cmd (is-cmd-valid? (get state id)))))
       cmds))

                                        ;(processing-cmds cmds state)


(def cmd1 {:id 1125 :cmd :archived})
(def cmd2 {:id 1125 :cmd :unarchived})
(def cmd3 {:id 1125 :cmd :deleted})

(def event1 {:event/id 1111
             :case/id 1125
             :cmd :screening})

(def cmds [cmd1 cmd2 cmd3])

(def case1
  {:id 1125
   :creator "6muau9En0Hgxrg13O2MhSr"
   :owner "ZW7wHDtj3F"
   :screening-state :ongoing
   :lifecycle-state :archived
   :outstanding-actions true})

(def case2
  {:id 8888
   :creator "6muau9En0Hgxrg13O2MhSr"
   :owner "ZW7wHDtj3F"
   :screening-state :ongoing
   :lifecycle-state :deleted
   :outstanding-actions true})

(def case3
  {:id 44444
   :creator "6muau9En0Hgxrg13O2MhSr"
   :owner "ZW7wHDtj3F"
   :screening-state :ongoing
   :lifecycle-state :unarchived
   :outstanding-actions true})

(def state {1125 case1
            8888 case2
            4444 case3
            })
