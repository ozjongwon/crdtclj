(ns crdt.crdt-types
  (:require [clojure.set :as set]))

;; Protocol for all CRDTs
(defprotocol CRDT
  (merge* [this other] "Merge this CRDT with another")
  (value [this] "Get the current value")
  (object->str [this] "For printing"))

(defprotocol CRDTSet
  (add [this v])
  (del [this v]))

(defn- print-crdt [crdt]
  (.write *out* (object->str crdt)))

(defmacro defcrdt [name [& fields] & body]
  (let [ipersistent #{'clojure.lang.IPersistentSet}
        c (gensym "c")
        w        (gensym "w")]
    `(do (deftype ~name [~@ fields]
           ~@body)
         ~(if (some #(contains? ipersistent %) body)
            `(.addMethod clojure.pprint/simple-dispatch ~name print-crdt)
            `(defmethod print-method ~name [~(vary-meta c assoc :tag '~name)
                                            ~(vary-meta w assoc :tag java.io.Writer)]
               (binding [*out* ~w]
                 (print-crdt ~c)))))))

;; G-Set (Grow-only Set)
(defcrdt GSet [items]
  CRDT
  (merge* [_ other]
          (GSet. (set/union items (.-items other))))
  (value [_] items)
  (object->str [this]
               (str (value this) " <GSet:" (str items) ">"))
  CRDTSet
  (add [this item]
       (GSet. (conj items item)))

  clojure.lang.IPersistentSet
  (cons [this item]
        (.add this item))
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

;; Constructor functions
(defn g-set
  ([] (GSet. #{}))
  ([& items] (GSet. (set items))))

(comment
  (defn example-g-set []
    (let [replica1 (-> (g-set)
                       (add :a)
                       (add :b))
          replica2 (-> (g-set)
                       (add :b)
                       (add :c))
          merged (merge* replica1 replica2)]
      {:replica1  replica1
       :replica2 replica2
       :merged merged})))

;; Two-Phase Set (2P-Set) with additions and removals
(defcrdt TwoPSet [additions tombstone]
  CRDT
  (merge* [_ other]
          (TwoPSet. (set/union additions (.-additions other))
                    (set/union tombstone (.-tombstone other))))
  (value [_]
         (set/difference additions tombstone))
  (object->str [this]
               (str (value this) " <TwoPSet:" "additions: " additions
                    "tombstone: " tombstone ">"))
  CRDTSet
  (add [this item]
       (if (contains? tombstone item)
         this
         (TwoPSet. (conj additions item) tombstone)))
  (del [this item]
       (if (contains? additions item)
         (TwoPSet. additions (conj tombstone item))
         this))

  clojure.lang.IPersistentSet
  (cons [this item]
        (add this item))
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
  ;; (disjoin [this k]
  ;;          (del this k))
  )

(defn two-p-set
  ([] (TwoPSet. #{} #{}))
  ([& items] (TwoPSet. (set items) #{})))

(comment ;; Example usage functions
  (defn example-2p-set []
    (let [replica1 (-> (two-p-set)
                       (add :a)
                       (add :b)
                       (del :a))
          replica2 (-> (two-p-set)
                       (add :b)
                       (add :c))
          merged (merge* replica1 replica2)]
      {:replica1 replica1
       :replica2 replica2
       :merged merged})))

;; G-Counter (Grow-only Counter)
(defcrdt GCounter [counts]             ; counts is a map of replica-id -> count
  CRDT
  (merge* [this other]
          (GCounter. (merge-with max counts (.-counts other))))
  (value [_]
         (apply + (vals counts)))
  (object->str [this]
               (str (value this) " <GCounter:"  counts ">")))

(defn g-counter [replica-id]
  (GCounter. {replica-id 0}))

;; PN-Counter (Positive-Negative Counter)
(defcrdt PNCounter [increments decrements] ; Each is a G-Counter
  CRDT
  (merge* [this other]
          (PNCounter. (merge-with max  increments (.-increments other))
                      (merge-with max decrements (.-decrements other))))
  (value [_]
         (- (apply + (vals increments)) (apply + (vals decrements))))
  (object->str [this]
               (str (value this) " <PNCounter:"  increments decrements ">")))

(defn increase [counter replica-id]
  (cond (instance? GCounter counter)
        (GCounter. (update (.-counts counter) replica-id (fnil inc 0)))

        (instance? PNCounter counter)
        (PNCounter. (update (.-increments counter) replica-id (fnil inc 0))
                    (.-decrements counter))

        :else (ex-info "Unspported counter: " {:counter counter :id replica-id})))

(defn decrease [^PNCounter counter replica-id]
  (cond (instance? PNCounter counter)
        (PNCounter. (.-increments counter)
                    (update (.-decrements counter) replica-id (fnil dec 0)))

        :else (ex-info "Unspported counter: " {:counter counter :id replica-id})))

(defn pn-counter [replica-id]
  (PNCounter. {replica-id 0} {replica-id 0}))

(comment
  (defn example-g-counter []
    (let [ ;; G-Counter example
          gc1 (-> (g-counter :r1)
                  (increase :r1)
                  (increase :r1))
          gc2 (-> (g-counter :r2)
                  (increase :r2))]
      {:gc1 gc1
       :gc2 gc2
       :merged (merge* gc1 gc2)})))

(comment
  (defn example-pn-counter []
    (let [ ;; PN-Counter example
          pn1 (-> (pn-counter :r1)
                  (increase :r1)
                  (decrease :r1)
                  (decrease :r2))
          pn2 (-> (pn-counter :r2)
                  (increase :r2))]
      {:pn1  pn1
       :pn2  pn2
       :merged (merge* pn1 pn2)})))


;; LWW-Element-Set (Last-Write-Wins-Element-Set)
(defcrdt LWWSet [additions removals]
  CRDT
  (merge* [this other]
          (LWWSet. (merge-with max additions (.-additions other))
                   (merge-with max removals (.-removals other))))
  (value [_]
         (set (for [[item ts0] additions
                    :let [ts (get removals item 0)]
                    :when (< ts0 ts)]
                item)))
  (object->str [this]
               (str (value this) " <LWSet" additions removals ">"))

  CRDTSet
  (add [this [item ts]]
       (let [ts0 (get additions item)]
         (if (and ts0 (>= ts0 ts))
           this
           (LWWSet. (assoc additions item ts) removals))))
  (del [this [item ts]]
       (let [ts0 (get removals item)]
         (if (and ts0 (>= ts0 ts))
           this
           (LWWSet. additions (assoc removals item ts))))))

(defn lww-set []
  (LWWSet. {} {}))

(comment
  (defn example-lww-set []
    (let [lww1 (-> (lww-set)
                   (add [:a 1])
                   (add [:b 2]))
          lww2 (-> (lww-set)
                   (add [:b 3])
                   (del [:a 4]))]
      {:lww1  lww1
       :lww2  lww2
       :merged (merge* lww1 lww2)})))

(defcrdt ORSet [elements]
  CRDT
  (merge* [this other]
          (ORSet.
           (merge-with set/union elements (.-elements other))))
  (value [_]
         (set (keys elements)))
  (object->str [this]
               (str (value this) " <ORSet" elements ">"))

  CRDTSet
  (add [this [item tag]]
       (ORSet. (update elements item (fnil conj #{}) tag)))
  (del [this item]
       (ORSet. (dissoc elements item))))

(defn or-set []
  (ORSet. {}))

(comment
  (defn example-or-set []
    (let [or1 (-> (or-set)
                  (add [:a 1])
                  (add [:b 2]))
          or2 (-> (or-set)
                  (add [:b 3])
                  (add [:c 4]))]
      {:or1 or1
       :or2 or2
       :merged (merge* or1 or2)})))


(comment
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
    (let [;; LWW-Set example
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
