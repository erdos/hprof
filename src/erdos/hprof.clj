(ns erdos.hprof
  (:require [clojure.java.io :as io]
            [clojure.pprint]))

(require '[datalevin.core :as datalevin])

(set! *warn-on-reflection* true)

(defmacro !! [x] `(doto ~x assert))

(declare +oop-size+ +data-input-stream+ +read-bytes+)
(declare read-int read-long read-byte read-unsigned-byte read-short read-unsigned-short read-char read-float read-double)

;; specification
;; 
;; https://github.com/unofficial-openjdk/openjdk/blob/60b7a8f8661234c389e247942a0012da30146a57/src/hotspot/share/services/heapDumper.cpp#L58

(def ^:private dis-param (quote ^java.io.DataInputStream +data-input-stream+))

(defmacro ^:private def-reader-2 [method bytes]
  (assert (contains? &env '+read-bytes+))
  `(do (vswap! ~'+read-bytes+ + ~bytes)
       (~method ~dis-param)))

(defmacro ^:private def-reader [name bytes method]
  `(defmacro ~name []
     (if (contains? ~'&env '~'+read-bytes+)
       (list 'def-reader-2 '~method ~bytes)
       (list '~method dis-param))))

(def-reader read-int 4 .readInt)
(def-reader read-long 8 .readLong)
(def-reader read-byte 1 .readByte)
(def-reader read-unsigned-byte 1 .readUnsignedByte)
(def-reader read-short 2 .readShort)
(def-reader read-unsigned-short 2 .readUnsignedShort)
(def-reader read-char 2 .readChar)
(def-reader read-float 4 .readFloat)
(def-reader read-double 8 .readDouble)

(defmacro read-unsigned-int [] `(Integer/toUnsignedLong (read-int)))

(defmacro read-id []
  `(case (int ~'+oop-size+)
     4 (read-int)
     8 (read-long)))

(defn- read-string-literal
  ([^java.io.DataInputStream +data-input-stream+ len]
   ;; read string of length
   (let [b (byte-array len)]
     (dotimes [i len]
       (aset b i (read-byte)))
     (new String b)))
  ([^java.io.DataInputStream +data-input-stream+]
   ;; read 0-terminated string
   (loop [c (read-byte)
          bb (java.nio.ByteBuffer/allocate 18)]
     (if (= 0 (int c))
       (new String (.array bb))
       (recur (read-byte) (.put bb c))))))

(defn- read-of-type [type ^java.io.DataInputStream +data-input-stream+ +oop-size+ +read-bytes+]
  (case (int type)
    2 (read-id)
    4 (read-byte)
    5 (read-char)
    6 (read-float)
    7 (read-double)
    8 (read-byte)
    9 (read-short)
    10 (read-int)
    11 (read-long)
    (throw (ex-info "Could not read for type " {:type type}))))

(defn- read-of-type-kw [type ^java.io.DataInputStream +data-input-stream+ +oop-size+]
  (case type
    :object (read-id)
    :boolean (read-byte)
    :char (read-char)
    :float (read-float)
    :double (read-double)
    :byte (read-byte)
    :short (read-short)
    :int (read-int)
    :long (read-long)
    (throw (ex-info "Could not read for type " {:type type}))))

(defn- type->kw [type-byte]
  (case type-byte
    2 :object
    4 :boolean
    5 :char
    6 :float
    7 :double
    8 :byte
    9 :short
    10 :int
    11 :long))

(defn- read-pair [^java.io.DataInputStream +data-input-stream+ +oop-size+ +read-bytes+]
  (let [t (read-unsigned-byte)]
    {:type (type->kw t)
     :value (read-of-type t +data-input-stream+ +oop-size+ +read-bytes+)}))

(def ^:private record-readers (vec (repeat 255 nil)))
(defmacro ^:private def-read-record [rectype hex body]
  (assert (keyword? rectype))
  (assert (integer? hex))
  `(alter-var-root #'record-readers
                   assoc ~hex
                   (fn [~dis-param ~'+oop-size+] (assoc ~body :record/type ~(keyword (name rectype))))))

(def ^:private sub-record-readers (vec (repeat 255 nil)))
(defmacro ^:private def-read-sub-record [rectype hex body]
  (assert (keyword? rectype))
  (assert (integer? hex))
  `(alter-var-root #'sub-record-readers
                   assoc ~hex (fn [~dis-param ~'+oop-size+ ~'+read-bytes+] (-> ~body (assoc :record/type ~(keyword (name rectype)))))))

(def-read-record :HPROF_UTF8 0x01
  (let [timestamp (read-int)
        len (read-int)]
    (assert (= 0 timestamp))
    {:record/id (read-id)
     :record/utf8 (-> len (- +oop-size+) (->> (read-string-literal +data-input-stream+)))}))

(def-read-record :HPROF_LOAD_CLASS 0x02
  (let [[timestamp len] [(read-int) (read-int)]
        serial-number (read-int)
        class-object-id (read-id)
        stack-trace-serial-nr (read-int)
        class-name-id (read-id)]
    (assert (pos? serial-number))
    {:serial-number serial-number
     :class-object-id class-object-id
     :stack-trace-serial-nr stack-trace-serial-nr
     :class-name-id  class-name-id}))

(def-read-record :HPROF_FRAME 0x04
  (let [[timestamp len] [(read-int) (read-int)]
        stack-frame-id (read-id)
        method-name-id (read-id)
        method-sign-id (read-id)
        source-file-name-id (read-id)
        class-serial-nr (read-int)
        line-number (read-int)]
    {:stack-frame-id stack-frame-id
     :method-name-id method-name-id
     :method-sign-id method-sign-id
     :source-file-name-id  source-file-name-id
     :class-serial-nr class-serial-nr
     :line-number (cond (pos? line-number) line-number
                        (= -1 line-number) :unknown
                        (= -2 line-number) :compiled-method
                        (= -3 line-number) :native-method)}))

(def-read-record :HPROF_TRACE 0x05
  (let [[timestamp len] [(read-int) (read-int)]
        stack-trace-serial-nr (read-int)
        thread-serial-nr      (read-int)
        nr-frames             (read-int)
        stack-frame-ids (doall (repeatedly nr-frames #(read-id)))]
    {:stack-trace-serial-nr stack-trace-serial-nr
     :thread-serial-number thread-serial-nr
     :stack-frame-ids stack-frame-ids}))

;; a set of heap allocation sites, obtained after GC
(def-read-record :HPROF_ALLOC_SITES 0x06
  (let [_ (do (read-int) (read-int))
        flags (read-unsigned-short)
        cutoff-ratio (read-int)
        total-live-bytes (read-int)
        total-live-instances (read-int)
        total-bytes-allocated (read-long)
        total-instances-allocated (read-long)
        nr-sites (read-int)
        sites (doall (for [_ (range nr-sites)]
                       {:is-array (read-byte)
                        :class-serial-nr (read-int)
                        :stack-trace-serial-nr (read-int)
                        :nr-bytes-alive (read-int)
                        :nr-instances-alive (read-int)
                        :nr-bytes-allocated (read-int)
                        :nr-innstances-allocated (read-int)}))]
    {:flags flags ;; TODO
     :cutoff-ratio cutoff-ratio
     :total-live-bytes total-live-bytes
     :total-live-instances total-live-instances
     :total-bytes-allocated total-bytes-allocated
     :total-instances-allocated total-instances-allocated
     :sites (vec sites)}))

(def-read-record :HPROF_HEAP_DUMP 0x0C
  (let [[timestamp len] [(read-int) (read-unsigned-int)]]
    {:record/length len}))

(def-read-record :HPROF_HEAP_DUMP_SEGMENT 0x1C
  (let [[timestamp len] [(read-int) (read-unsigned-int)]]
    {:record/length len}))

(def-read-record :HPROF_HEAP_DUMP_END 0x2C
  (let [[timestamp len] [(read-int) (read-int)]]
    (assert (zero? len))
    {}))

(def-read-sub-record :HPROF_GC_ROOT_JNI_GLOBAL 0x01
  {:object-id (read-id)
   :jni-global-ref-id (read-id)})

(def-read-sub-record :HPROF_GC_ROOT_JNI_LOCAL 0x02
  {:object-id (read-id)
   :thread-serial-number (read-int)
   :frame-nr-in-stack-trace (read-int)})

(def-read-sub-record :HPROF_GC_ROOT_JAVA_FRAME 0x03
  {:object-id (read-id)
   :thread-serial-number (read-int)
   :frame-nr-in-stack-trace (read-int) ;; (-1 for empty)
   })

(def-read-sub-record :HPROF_GC_ROOT_NATIVE_STACK 0x04
  {:object-id (read-id)
   :thread-serial-number (read-int)})

(def-read-sub-record :HPROF_GC_ROOT_STICKY_CLASS 0x05
  {:system-object-id (read-id)})

;; Reference from thread block
(def-read-sub-record :HPROF_GC_ROOT_THREAD_BLOCK 0x06
  {:object-id (read-id)
   :thread-serial-number (read-int)})

;; Busy monitor
(def-read-sub-record :HPROF_GC_ROOT_MONITOR_USED 0x07
  {:object-id (read-id)})

;; thread object
(def-read-sub-record :HPROF_GC_ROOT_THREAD_OBJ 0x08
  {:thread-object-id (read-id)
   :thread-sequence-number (read-int)
   :stack-trace-sequence-number (read-int)})

;; dump of class object
(def-read-sub-record :HPROF_GC_CLASS_DUMP 0x20
  (let [id (read-id)
        stsn (read-int) ;; stack trace serial number
        sc-id (read-id) ;; super-class object id
        cl-id (read-id) ;; class-loader object id
        signers-id (read-id) ;; signers object  id
        prdom-id (read-id) ;; protection domain object id
        res0 (read-id) ;; reserved 0
        res1 (read-id) ;; reserved 0
        instance-size (read-int) ;; instance size in bytes
        cp (doall (for [_ (range (read-unsigned-short))
                        :let [idx (read-short)]]
                    (assoc (read-pair +data-input-stream+ +oop-size+ +read-bytes+)
                           :ip-idx idx)))
        statics (doall (for [_ (range (read-unsigned-short))
                             :let [id (read-id)]]
                         (assoc (read-pair +data-input-stream+ +oop-size+ +read-bytes+)
                                :id id)))
        inst (doall (for [_ (range (read-unsigned-short))]
                      {:field-id (read-id)
                       :type (type->kw (read-unsigned-byte))}))]
    {:class-object-id id
     :stack-trace-serial-nr stsn
     :super-class-object-id sc-id
     :class-loader-object-id cl-id
     :signers-object-id signers-id
     :protection-domain-object-id prdom-id
     :reserved [res0 res1]
     :instance-size instance-size
     :constant-pool cp
     :statics statics
     :inst inst}))

;; dump of a normal object.
(def-read-sub-record :HPROF_GC_INSTANCE_DUMP 0x21
  (let [id      (read-id)
        ss-ss   (read-int)
        cl-id   (read-id)
        size    (read-int) ;; nr of bytes that follow
        content (byte-array (repeatedly size #(read-byte))) ;; read n bytes
        ]
    {:id id
     :stack-trace-serial-nr ss-ss
     :class-object-id cl-id
     :content content}))

;; dump of an object array
(def-read-sub-record :HPROF_GC_OBJ_ARRAY_DUMP 0x22
  (let [id (read-id)
        ss-ss (read-int)
        nr (read-int) ;; nr of elements
        arr-cl-id (read-id) ;; array class id
        elements (vec (repeatedly nr #(read-id)))]
    {:array-object-id id
     :stack-trace-serial-nr ss-ss
     :array-class-id arr-cl-id
     :elements elements}))

(def-read-sub-record :HPROF_GC_PRIM_ARRAY_DUMP 0x23
  (let [id (read-id)
        ss (read-int)
        nr (read-int)
        type (read-unsigned-byte)
        elements (vec (repeatedly nr #(read-of-type type +data-input-stream+ +oop-size+ +read-bytes+)))]
    {:array-object-id id
     :stack-trace-serial-nr ss
     :elements elements}))

(defn read-sub-record [^java.io.DataInputStream +data-input-stream+ +oop-size+]
  (let [+read-bytes+ (volatile! 0)
        b (int (read-byte))]
    (if-let [r (get sub-record-readers b +read-bytes+)]
      (assoc (r +data-input-stream+ +oop-size+ +read-bytes+) :record/length @+read-bytes+)
      (throw (ex-info (str "No reader for subrecord " b) {})))))

;; read n bytes of subrecords
(defn- read-sub-records-of-bytes [^java.io.DataInputStream +data-input-stream+ byte-count oop-size]
  (cond (zero? byte-count) []
        (neg? byte-count)  (assert false (str "Byte cnt is " byte-count))
        :else (let [head (read-sub-record +data-input-stream+ oop-size)]
                (cons head (lazy-seq (read-sub-records-of-bytes +data-input-stream+ (- byte-count (:record/length head)) oop-size))))))

(defn read-record [^java.io.DataInputStream +data-input-stream+ +oop-size+]
  (let [b (int (read-byte))]
    (if-let [r (get record-readers b)]
      (r +data-input-stream+ +oop-size+)
      (throw (ex-info (str "Can not find reader for byte " (int b)) {:byte b})))))

(defn- read-record+subrecords [+data-input-stream+ oop-size]
  (when (pos? (.available ^java.io.DataInputStream +data-input-stream+))
    (let [head (read-record +data-input-stream+ oop-size)]
      (case (:record/type head)
        (:HPROF_HEAP_DUMP :HPROF_HEAP_DUMP_SEGMENT)
        (lazy-cat [head]
                  (read-sub-records-of-bytes +data-input-stream+ (:record/length head) oop-size)
                  (read-record+subrecords +data-input-stream+ oop-size))
        ;; else
        (cons head (lazy-seq (read-record+subrecords +data-input-stream+ oop-size)))))))


;; maps over a sequence with mapping both elements and an accumulator record
(defn- map-with-acc [accum-mapper elem-mapper accumulator xs]
  ((fn f [accum xs]
     (when-let [[head & tail] (seq xs)]
       (let [accum (accum-mapper accum head)
             head  (elem-mapper accum head)]
         (cons head (lazy-seq (f accum tail))))))
   accumulator xs))

(defn map-string-vals [records]
  (map-with-acc
   (fn [strings record]
     (if (= :HPROF_UTF8 (:record/type record))
       (assoc strings (:record/id record) (:record/utf8 record))
       strings))
   (fn [strings record]
     (case (:record/type record)
       :HPROF_LOAD_CLASS (-> record
                             (dissoc :class-name-id)
                             (assoc :class-name (get strings (:class-name-id record))))
       :HPROF_GC_CLASS_DUMP (-> record
                                (update :inst (partial mapv (fn [x] (assoc x :name (get strings (:field-id x)))))))
       :HPROF_FRAME      (-> record
                             (dissoc :method-name-id :source-file-name-id)
                             (cond-> (not= 0 (:source-file-name-id record))
                               (assoc :source-file-name (strings (:source-file-name-id record))))
                             (assoc :method-name (get strings (:method-name-id record))
                                    ; :source-file-name (get strings (:source-file-name-id record))
                                    ))
       record))
   {} records))

(defn map-class-names [records]
  (map-with-acc
   (fn [classnames elem]
     (if (= :HPROF_LOAD_CLASS (:record/type elem))
       (assoc classnames (:class-object-id elem) (:class-name elem))
       classnames))
   (fn [classnames elem]
     (if (= :HPROF_GC_CLASS_DUMP (:record/type elem))
       (assoc elem
              :class-name (!! (classnames (:class-object-id elem)))
              :super-class-name
              ;; java.lang.Object does not have a super type
              (if (zero? (:super-class-object-id elem))
                "java/lang/Object"
                (!! (classnames (:super-class-object-id elem)))))
       elem))
   {} records))

(defn read-instance-fields [id-size fields content]
  (assert id-size)
  (assert fields)
  (assert content)
  (let [bis (new java.io.ByteArrayInputStream content)
        dis (new java.io.DataInputStream bis)]
    (mapv (fn [field]
            {:type (:type field)
             :field-id (:field-id field)
             :value (read-of-type-kw (:type field) dis id-size)})
          fields)))

;; super-class-object-id
;;
(defn map-instance-fields [id-size records]
  (map-with-acc
   ;; for class dump: save field descriptors
   (fn [class->fields elem]
     (if (= :HPROF_GC_CLASS_DUMP (:record/type elem))
       ;; TODO: also collect parent fields!!
       (assoc class->fields
              (:class-object-id elem)
              (concat
               (when (not= "java/lang/Object" (:class-name elem))
                (doto (class->fields (:super-class-object-id elem))
                  (assert  (str "Cannot hande " elem ))))
               (:inst elem)))
       class->fields))
   ;; for instace dumps: parse fields
   (fn [class->fields elem]
     (if (= :HPROF_GC_INSTANCE_DUMP (:record/type elem))
       (-> elem
           (dissoc :content)
           (assoc :fields (read-instance-fields id-size (!! (class->fields (:class-object-id elem))) (:content elem))))
       elem))
   {} records))

;; TODO: write mapper for instance fields!

(defn read-hprof-seq [input-stream]
  (let [+data-input-stream+ (new java.io.DataInputStream input-stream)]
    (assert (= "JAVA PROFILE 1.0.2" (read-string-literal +data-input-stream+)))
    (let [id-size    (read-int)
          timestamp  (read-long)]
      (->> (read-record+subrecords +data-input-stream+ id-size)
           (map-string-vals)
           (map-class-names)
           ;(map-instance-fields id-size)
           ))))

(def schema {:record/id {:db/valueType :db.type/ref, :db.unique :db.unique/identity}
             })

(def db-conn (datalevin/get-conn "/tmp/hprof-test-2" schema))

(defn read-hprof-file [input]
  (with-open [istream (new java.io.DataInputStream (io/input-stream input))]
    (->> (read-hprof-seq istream)
         (remove (comp #{:HPROF_UTF8} :record/type))
         (datalevin/transact! db-conn))
;;    (run! println (read-hprof-seq istream))
    :ok
    ))



(defn -main [hprof-file]
  (println :!)
  (read-hprof-file (io/file hprof-file))
  (datalevin/close db-conn)
  (int 0))
