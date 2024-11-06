(ns crdt.automerge
  (:require [clojure.data.priority-map :refer [priority-map]]
            [clojure.set :as set])
  (:import [org.automerge AutomergeSys Document ObjectId ObjectType BuildInfo]))

;; cd ~/Work/crdt/; lein clean; lein deps; cd target/native/x86_64-unknown-linux-gnu/; ln -s libautomerge_jni.so libautomerge_jni_0_2_0.so

;; 1. Create a doc /w Tx
;; 2. Create a text (content of the doc)
;; 3. Edit the text (check text)
;; 4. Commit
;; 5. Save the doc
;; 6. Create another doc from the save /w Tx
;; 7. Check the content text is same as the previous text
;; 8. Edit the text (check text)
;; 9. Commit
;; 10. Start a transaction with the first doc
;; 11. Edit the text (check text)
;; 12. Commit
;; 13. check the text
;; 14. Merge doc1
;; 15 check content of doc1 and doc2
;;

(defmacro with-document-tx [[doc tx] & body]
  `(let [~doc (Document.)
         ~tx (.startTransaction ~doc)]
     ~@body
     (.commit ~tx)))

(defn splice
  ([tx text content]
   (splice tx text content 0 0))
  ([tx text content start]
   (splice tx text content start 0))
  ([tx text content start delete-count]
   (.spliceText tx text start delete-count content)))

(defmacro with-text [[text tx] & body]
  `(let [~text  (.set ~tx ObjectId/ROOT "text" ObjectType/TEXT)]
     ~@body))

(defn document-save [doc]
  (.save doc))

(defn document-text [doc text]
  (-> (.text doc text)
      .get))

(defn document-merge [doc1 doc2]
  (.merge doc1 doc2))

(defmacro with-loading-document [[doc doc-bytes] & body]
  `(let [~doc (Document/load ~doc-bytes)]
     ~@body))

(defmacro with-tx [[tx doc] & body]
  `(let [~tx (.startTransaction ~doc)]
     ~@body
     (.commit ~tx)))

(defn init-lib []
  (with-document-tx [doc tx]
    (with-text [text tx]
      (splice tx text "Hello World"))))


(defn print-documents-text [doc1 doc2 text]
  (println "***\nDoc 1 text:" (document-text doc1 text)
           "\nDoc 2 text:" (document-text doc2 text)))

(defn example []
  (let [doc1 (atom nil)
        doc2 (atom nil)
        shared-text (atom nil)]
    (with-document-tx [doc tx]
      (with-text [text tx]
        (splice tx text "Hello World")
        (reset! doc1 doc)
        (reset! shared-text text)))
    (with-loading-document [doc (document-save @doc1)]
      (print-documents-text @doc1 doc @shared-text)
      (with-tx [tx doc]
        (splice tx @shared-text " beautiful" 5))
      (reset! doc2 doc))
    (print-documents-text @doc1 @doc2 @shared-text)
    (with-tx [tx @doc1]
      (splice tx @shared-text " there" 5))
    (print-documents-text @doc1 @doc2 @shared-text)
    (document-merge @doc1 @doc2)
    (print-documents-text @doc1 @doc2 @shared-text)
    (document-merge @doc2 @doc1)
    (print-documents-text @doc1 @doc2 @shared-text)))

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
