(ns crdt.types
  (:require [clojure.set :as set]))

;; Protocol for all CRDTs
(defprotocol CRDT
  (merge* [this other] "Merge this CRDT with another")
  (value [this] "Get the current value")
  (object->str [this] "For printing"))

(defn print-crdt [crdt-str writer]
  (.write writer crdt-str))

;; Grow-Only Set (G-Set)
(deftype GSet [items]
  CRDT
  (merge* [_ other]
    (GSet. (set/union items (.-items other))))
  (value [_] items)
  (object->str [this]
    (str (value this) " <GSet:" (str (.-items this)) ">"))

  clojure.lang.IPersistentSet
  (cons [this item]
    (GSet. (conj items item)))
  (empty [this]
    (GSet. #{}))
  (equiv [this other]
    (= items (.-items other)))
  (seq [_]
    (seq items))
  (get [_ k]
    (get items k))
  (contains [_ k]
    (contains? items k))
  (disjoin [this k]
    (throw (UnsupportedOperationException. "Cannot remove items from G-Set"))))



;; (defmethod print-method GSet [s writer]
;;   (print-g-set s writer))

;; (defmethod print-dup GSet [s writer]
;;   (print-g-set s writer))


(.addMethod clojure.pprint/simple-dispatch GSet
            (fn [s]
              (print-crdt (object->str s) *out*)))


;; Constructor functions
(defn g-set
  ([] (GSet. #{}))
  ([& items] (GSet. (set items))))

(defn example-g-set []
  (let [replica1 (-> (g-set)
                     (conj :a)
                     (conj :b))
        replica2 (-> (g-set)
                     (conj :b)
                     (conj :c))
        merged (merge* replica1 replica2)]
    {:replica1  replica1
     :replica2 replica2
     :merged merged}))


;; Two-Phase Set (2P-Set) with additions and removals
(comment
  (deftype TwoPSet [additions tombstone]
    CRDT
    (merge* [_ other]
      (TwoPSet. (set/union additions (.-additions other))
                (set/union tombstone (.-tombstone other))))
    (value [_]
      (set/difference additions tombstone))
    (inspect [_] (list additions tombstone))

    clojure.lang.IPersistentSet
    (cons [this item]
      (if (contains? tombstone item)
        this
        (TwoPSet. (conj additions item) tombstone)))
    (empty [_]
      (TwoPSet. #{} #{}))
    (equiv [_ other]
      (= (set/difference additions tombstone)
         (set/difference (.-additions other) (.-tombstone other))))
    (seq [this]
      (seq (value this)))
    (get [this k]
      (get (value this) k))
    (contains [_ k]
      (and (contains? additions k)
           (not (contains? tombstone k))))
    (disjoin [this k]
      (TwoPSet. additions (conj tombstone k))))

  (defn two-p-set
    ([] (TwoPSet. #{} #{}))
    ([& items] (TwoPSet. (set items) #{})))

  ;; Example usage functions
  (defn example-2p-set []
    (let [replica1 (-> (two-p-set)
                       (conj :a)
                       (conj :b)
                       (disj :a))
          replica2 (-> (two-p-set)
                       (conj :b)
                       (conj :c))
          merged (merge* replica1 replica2)]
      {:replica1 replica1
       :replica2 replica2
       :merged merged}))

  ;; G-Counter (Grow-only Counter)
  (deftype GCounter [counts]             ; counts is a map of replica-id -> count
    CRDT
    (merge* [this other]
      (GCounter.
       (merge-with max (.-counts this) (.-counts other))))
    (value [_]
      (apply + (vals counts))))

  ;; Print method for GCounter
  (defmethod print-method GCounter [^GCounter c ^java.io.Writer w]
    (doto w
      (.write "#GCounter{:counts ")
      (.write (str 1))
      (.write "}")))

  ;; Print method for PNCounter
  ;; (defmethod print-method PNCounter
  ;;   [^PNCounter c ^Writer w]
  ;;   (.write w "#PNCounter{:inc ")
  ;;   (print-method (.-increments c) w)
  ;;   (.write w ", :dec ")
  ;;   (print-method (.-decrements c) w)
  ;;   (.write w "}"))

  ;; Print method for LWWSet
  ;; (defmethod print-method LWWSet
  ;;   [^LWWSet s ^Writer w]
  ;;   (.write w "#LWWSet{:adds ")
  ;;   (print-method (.-additions s) w)
  ;;   (.write w ", :removes ")
  ;;   (print-method (.-removals s) w)
  ;;   (.write w "}"))

  ;; Print method for ORSet
  ;; (defmethod print-method ORSet
  ;;   [^ORSet s ^Writer w]
  ;;   (.write w "#ORSet{:elements ")
  ;;   (print-method (.-elements s) w)
  ;;   (.write w "}"))

  ;; Print method for Position (used in RGA)
  ;; (defmethod print-method Position
  ;;   [^Position p ^Writer w]
  ;;   (.write w "#Position{:rid ")
  ;;   (print-method (.-replica-id p) w)
  ;;   (.write w ", :counter ")
  ;;   (print-method (.-counter p) w)
  ;;   (.write w "}"))

  ;; Print method for RGANode
  ;; (defmethod print-method RGANode
  ;;   [^RGANode n ^Writer w]
  ;;   (.write w "#RGANode{:value ")
  ;;   (print-method (.-value n) w)
  ;;   (.write w ", :pos ")
  ;;   (print-method (.-position n) w)
  ;;   (.write w ", :left ")
  ;;   (print-method (.-left-pos n) w)
  ;;   (.write w ", :deleted? ")
  ;;   (print-method (.-deleted? n) w)
  ;;   (.write w "}"))

  ;; Print method for RGA
  ;; (defmethod print-method RGA
  ;;   [^RGA rga ^Writer w]
  ;;   (.write w "#RGA{:nodes ")
  ;;   (print-method (.-nodes rga) w)
  ;;   (.write w ", :counter ")
  ;;   (print-method (.-counter rga) w)
  ;;   (.write w ", :rid ")
  ;;   (print-method (.-replica-id rga) w)
  ;;   (.write w "}"))

  (defn g-counter [replica-id]
    (GCounter. {replica-id 0}))

  (defn increment [^GCounter counter replica-id]
    (GCounter.
     (update (.-counts counter) replica-id (fnil inc 0))))

  ;; PN-Counter (Positive-Negative Counter)
  (deftype PNCounter [increments decrements] ; Each is a G-Counter
    CRDT
    (merge* [this other]
      (PNCounter.
       (merge* (.-increments this) (.-increments other))
       (merge* (.-decrements this) (.-decrements other))))
    (value [_]
      (- (value increments) (value decrements))))

  (defn pn-counter [replica-id]
    (PNCounter. (g-counter replica-id) (g-counter replica-id)))

  (defn pn-increment [^PNCounter counter replica-id]
    (PNCounter.
     (increment (.-increments counter) replica-id)
     (.-decrements counter)))

  (defn pn-decrement [^PNCounter counter replica-id]
    (PNCounter.
     (.-increments counter)
     (increment (.-decrements counter) replica-id)))

  ;; LWW-Element-Set (Last-Write-Wins-Element-Set)
  (deftype LWWSet [additions removals]   ; Each is a map of element -> timestamp
    CRDT
    (merge* [this other]
      (LWWSet.
       (merge-with max (.-additions this) (.-additions other))
       (merge-with max (.-removals this) (.-removals other))))
    (value [_]
      (set (for [elem (set/union (keys additions) (keys removals))
                 :let [add-time (get additions elem 0)
                       remove-time (get removals elem 0)]
                 :when (> add-time remove-time)]
             elem))))

  (defn lww-set []
    (LWWSet. {} {}))

  (defn lww-add [^LWWSet s elem timestamp]
    (LWWSet.
     (assoc (.-additions s) elem timestamp)
     (.-removals s)))

  (defn lww-remove [^LWWSet s elem timestamp]
    (LWWSet.
     (.-additions s)
     (assoc (.-removals s) elem timestamp)))

  ;; OR-Set (Observed-Remove Set)
  (deftype ORSet [elements]         ; elements is a map of elem -> #{unique-tags}
    CRDT
    (merge* [this other]
      (ORSet.
       (merge-with set/union (.-elements this) (.-elements other))))
    (value [_]
      (set (keys elements))))

  (defn or-set []
    (ORSet. {}))

  (defn unique-tag []
    (str (java.util.UUID/randomUUID)))

  (defn or-add [^ORSet s elem]
    (ORSet.
     (update (.-elements s) elem (fnil conj #{}) (unique-tag))))

  (defn or-remove [^ORSet s elem]
    (ORSet.
     (dissoc (.-elements s) elem)))

  ;; RGA (Replicated Growable Array) - a sequence CRDT
  (defrecord Position [replica-id counter])

  (defn compare-positions [^Position p1 ^Position p2]
    (let [c (compare (.-counter p1) (.-counter p2))]
      (if (zero? c)
        (compare (.-replica-id p1) (.-replica-id p2))
        c)))

  (deftype RGANode [value position left-pos deleted?])

  (deftype RGA [nodes counter replica-id] ; nodes is a sorted-map of Position -> RGANode
    CRDT
    (merge* [this other]
      (RGA.
       (merge-with
        (fn [n1 n2]
          (if (> (.-counter (.-position n2)) (.-counter (.-position n1)))
            n2
            n1))
        (.-nodes this)
        (.-nodes other))
       (max (.-counter this) (.-counter other))
       (.-replica-id this)))
    (value [_]
      (into []
            (comp
             (filter (fn [[_ ^RGANode node]] (not (.-deleted? node))))
             (map (fn [[_ ^RGANode node]] (.-value node))))
            (sort-by first compare-positions nodes))))

  (defn rga [replica-id]
    (RGA. (sorted-map) 0 replica-id))

  (defn rga-insert [^RGA rga value after-pos]
    (let [counter (inc (.-counter rga))
          position (Position. (.-replica-id rga) counter)
          node (RGANode. value position after-pos false)]
      (RGA.
       (assoc (.-nodes rga) position node)
       counter
       (.-replica-id rga))))

  (defn rga-remove [^RGA rga position]
    (if-let [^RGANode node (get (.-nodes rga) position)]
      (RGA.
       (assoc (.-nodes rga) position
              (RGANode. (.-value node) (.-position node) (.-left-pos node) true))
       (.-counter rga)
       (.-replica-id rga))
      rga))

  ;; Example usage functions
  (defn example-all-crdts []
    (let [ ;; G-Counter example
          gc1 (-> (g-counter :r1)
                  (increment :r1)
                  (increment :r1))
          gc2 (-> (g-counter :r2)
                  (increment :r2))

          ;; PN-Counter example
          pn1 (-> (pn-counter :r1)
                  (pn-increment :r1)
                  (pn-decrement :r1))
          pn2 (-> (pn-counter :r2)
                  (pn-increment :r2))

          ;; LWW-Set example
          lww1 (-> (lww-set)
                   (lww-add :a 1)
                   (lww-add :b 2))
          lww2 (-> (lww-set)
                   (lww-add :b 3)
                   (lww-remove :a 4))

          ;; OR-Set example
          or1 (-> (or-set)
                  (or-add :a)
                  (or-add :b))
          or2 (-> (or-set)
                  (or-add :b)
                  (or-add :c))

          ;; RGA example
          rga1 (-> (rga :r1)
                   (rga-insert "A" nil)
                   (rga-insert "B" (Position. :r1 1)))
          rga2 (-> (rga :r2)
                   (rga-insert "C" (Position. :r1 1)))]

      {:g-counter {:gc1 (value gc1)
                   :gc2 (value gc2)
                   :merged (value (merge* gc1 gc2))}
       :pn-counter {:pn1 (value pn1)
                    :pn2 (value pn2)
                    :merged (value (merge* pn1 pn2))}
       :lww-set {:lww1 (value lww1)
                 :lww2 (value lww2)
                 :merged (value (merge* lww1 lww2))}
       :or-set {:or1 (value or1)
                :or2 (value or2)
                :merged (value (merge* or1 or2))}
       :rga {:rga1 (value rga1)
             :rga2 (value rga2)
             :merged (value (merge* rga1 rga2))}})))
























(comment
  ;; (remove-ns  'crdt.protocols)

  (defprotocol CRDTOperations
    (merge! [this]))

  (defprotocol AddContains
    (add! [this x]
      "Add an element x to the data structure.")
    (contains? [this x]
      "Check if the the data structure contains the element x."))

  (defprotocol Remove
    (remove! [this x]
      "Mark an element x for removal."))

  (defprotocol Get
    (get [this & x]
      "Get the data structure to value v."))

  (defprotocol Set
    (set! [this x]
      "Set the data structure to value v."))

  (defprotocol Unset
    (unset! [this x]
      "Unset the data structure to value v."))

  (defprotocol Put
    (put! [this x]
      "Add or update a key-value pair (k, v) in the data structure."))

  ;; (defrecord GrowOnlySet [set]
  ;;   AddContains
  ;;   (add! [this x]
  ;;     (conj set x)
  ;;     this)
  ;;   (contains? [this x]
  ;;     (clojure.core/contains? set x)))

  (defmethod )

  (deftype GrowOnlySet [data]
    clojure.lang.IPersistentSet
    (seq [this]
      (seq @data))
    (count [this]
      (count @data))
    (cons [this v]
      (GrowOnlySet. (conj @data v)))
    (empty [this]
      (empty? @data))
    (equiv [this that]
      (= @data that))
    (disjoin [this k]
      (swap! disj data k))
    (contains [this x]
      (clojure.core/contains? @data x))
    (get [this k]
      (clojure.core/get @data k)))

  (deftype Bag [data]
    clojure.lang.IPersistentSet

    (seq [this] (seq data))

    (count [this] (count @data))

    (cons [this o]
      (Bag. (conj @data o)))

    (empty [this] (Bag. (atom #{})))

    (equiv [this other] (or (= @data other)
                            (= @data @(:data other))))

    (disjoin [this key]
      (Bag. (atom (disj @data key))))

    (contains [this key] (clojure.core/contains? @data key))

    (get [this key] (clojure.core/get @data key)))

  (defn make-grow-only-set [ & init-data]
    (GrowOnlySet. (atom (or init-data #{}))))

  ;; (defrecord TwoPhaseSet []
  ;;   AddContains
  ;;   Remove
  ;;   (add! [this x])
  ;;   (remove! [this x])
  ;;   (contains? [this x]))

  ;; (defrecord PartialOrderNumberSetWithNodeRemoval []
  ;;   AddContains
  ;;   Remove
  ;;   (add! [this x])
  ;;   (remove! [this x])
  ;;   (contains? [this x]))

  ;; (defrecord GrowOnlyRegister []
  ;;   Set
  ;;   Get
  ;;   (set! [this x])
  ;;   (get [this & _]
  ;;     (list this)))

  ;; (defrecord PartialOrderRegister []
  ;;   Set
  ;;   Get
  ;;   (set! [this x])
  ;;   (get [this & _]))

  ;; (defrecord TwoPhaseRegister []
  ;;   Set
  ;;   Unset
  ;;   Get
  ;;   (set! [this x])
  ;;   (unset! [this x])
  ;;   (get [this & _]))

  ;; (defrecord GrowOnlyMap []
  ;;   Put
  ;;   Get
  ;;   (put! [this x])
  ;;   (get [this & k]))

  ;; (defrecord PartialOrderMap []
  ;;   Put
  ;;   Remove
  ;;   Get
  ;;   (put! [this x])
  ;;   (get [this & k]
  ;;     (list :this k)))
  )
