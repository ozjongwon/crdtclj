(ns crdt.automerge
  (:require [clojure.data.priority-map :refer [priority-map]]
            [clojure.set :as set])
  (:import [org.automerge AutomergeSys]))

(defonce lib-initialized? false)

(defrecord Document [pointer actor-id transaction-pointer])

(defn make-document []
  (when-not lib-initialized?
    (try (do (clojure.lang.RT/loadLibrary "automerge_jni")
             (alter-var-root #'lib-initialized? (constantly true))
             (let [doc-ptr (AutomergeSys/createDoc)]
               (println doc-ptr)
               (map->Document {:pointer doc-ptr
                               :actor-id (AutomergeSys/getActorId doc-ptr)})))
         (catch Exception e
           ;; FIXME: do something
           (println e)))))

(let [doc (make-document)
      tx (.startTransaction doc)
      text (.set tx ObjectId/ROOT "text" ObjectType/TEXT)]
  (.spliceText tx text 0  0 "Hello world")
  (.commit tx tx)
  (let [doc-bytes (.save doc)
        doc2 (Document. (.load doc-bytes))]
    (def result (str (.text doc2 text)))))


(comment

  ;; Data structures

  (defrecord Change [actor seq op key value start end])
  (defrecord Clock [actor seq])
  (defrecord Doc [changes clock values conflicts])

  ;; Actor ID generation
  (defn generate-actor-id []
    (str (java.util.UUID/randomUUID)))

  ;; Initialize new document
  (defn init []
    (->Doc #{} {} {} {}))

  ;; Vector clock operations
  (defn increment-clock [clock actor]
    (update clock actor (fnil inc 0)))

  (defn compare-clocks [c1 c2]
    (cond
      (= c1 c2) 0
      (every? (fn [[k v]] (<= (get c2 k 0) v)) c1) 1
      (every? (fn [[k v]] (<= (get c1 k 0) v)) c2) -1
      :else :concurrent))

  ;; Change handling
  (defn make-change [actor seq op key value & [start end]]
    (->Change actor seq op key value start end))

  (defn apply-change [doc change]
    (let [{:keys [actor seq op key value start end]} change
          current-seq (get-in doc [:clock actor] 0)]
      (if (> seq current-seq)
        (-> doc
            (update :changes conj change)
            (assoc-in [:clock actor] seq)
            (cond->
                (= op :set) (assoc-in [:values key] value)
                (= op :delete) (update :values dissoc key)
                (= op :insert) (update-in [:values key]
                                          #(if (vector? %)
                                             (into (into [] (take start %))
                                                   (concat [value] (drop start %)))
                                             [value]))
                (= op :list-delete) (update-in [:values key]
                                               #(into (into [] (take start %))
                                                      (drop (inc end) %))))))))

  ;; Conflict resolution
  (defn resolve-conflicts [changes]
    (reduce (fn [acc change]
              (let [{:keys [actor seq op key value]} change]
                (case op
                  :set (if-let [existing (get acc key)]
                         (if (pos? (compare [seq actor]
                                            [(get-in existing [:seq])
                                             (get-in existing [:actor])]))
                           (assoc acc key change)
                           acc)
                         (assoc acc key change))
                  acc)))
            {}
            (sort-by (juxt :seq :actor) changes)))

  ;; Public API
  (defn new-doc []
    (let [actor (generate-actor-id)]
      {:doc (init)
       :actor actor}))

  (defn set-value [{:keys [doc actor] :as state} key value]
    (let [seq (inc (get-in doc [:clock actor] 0))
          change (make-change actor seq :set key value)]
      (update state :doc apply-change change)))

  (defn get-value [{:keys [doc]} key]
    (get-in doc [:values key]))

  (defn insert [{:keys [doc actor] :as state} key value index]
    (let [seq (inc (get-in doc [:clock actor] 0))
          change (make-change actor seq :insert key value index)]
      (update state :doc apply-change change)))

  (defn delete [{:keys [doc actor] :as state} key]
    (let [seq (inc (get-in doc [:clock actor] 0))
          change (make-change actor seq :delete key nil)]
      (update state :doc apply-change change)))

  ;; Merging documents
  (defn merge-docs [doc1 doc2]
    (reduce apply-change
            doc1
            (sort-by (juxt :seq :actor)
                     (set/difference (:changes doc2) (:changes doc1)))))

  ;; History tracking
  (defn get-history [doc]
    (->> (:changes doc)
         (sort-by (juxt :seq :actor))
         (map #(select-keys % [:actor :seq :op :key :value]))))

  ;; Example usage:
  (comment
    ;; Create two replicas
    (def replica1 (new-doc))
    (def replica2 (new-doc))

    ;; Make concurrent changes
    (def replica1 (-> replica1
                      (set-value "name" "Alice")
                      (set-value "age" 30)))

    (def replica2 (-> replica2
                      (set-value "name" "Bob")
                      (set-value "location" "NYC")))

    ;; Merge changes
    (def merged (merge-docs (:doc replica1) (:doc replica2)))

    ;; Check values
    (get-value {:doc merged} "name")    ;; Returns last-write-wins value
    (get-value {:doc merged} "age")     ;; Returns 30
    (get-value {:doc merged} "location") ;; Returns "NYC"
    )
  )
