(ns erdos.hprof
  (:require [clojure.java.io :as io]
            [clojure.pprint]))

(set! *warn-on-reflection* true)

;; specification
;; 
;; https://github.com/unofficial-openjdk/openjdk/blob/60b7a8f8661234c389e247942a0012da30146a57/src/hotspot/share/services/heapDumper.cpp#L58

(def ^:dynamic *oop-size* nil)
(def ^:dynamic ^java.io.DataInputStream *data-input-stream* nil)
(def ^:dynamic *read-bytes* (volatile! 0))

(defmacro read-int [] `(do (vswap! *read-bytes* + 4) (.readInt *data-input-stream*)))
(defmacro read-long [] `(do (vswap! *read-bytes* + 8) (.readLong *data-input-stream*)))
(defmacro read-byte [] `(do (vswap! *read-bytes* inc) (.readByte *data-input-stream*)))
(defmacro read-unsigned-byte [] `(do (vswap! *read-bytes* inc) (.readUnsignedByte *data-input-stream*)))
(defmacro read-short [] `(do (vswap! *read-bytes* + 2) (.readShort *data-input-stream*)))
(defmacro read-unsigned-short [] `(do (vswap! *read-bytes* + 2) (.readUnsignedShort *data-input-stream*)))
(defmacro read-char [] `(do (vswap! *read-bytes* + 2) (.readChar *data-input-stream*)))
(defmacro read-float [] `(do (vswap! *read-bytes* + 4) (.readFloat *data-input-stream*)))
(defmacro read-double [] `(do (vswap! *read-bytes* + 8) (.readDouble *data-input-stream*)))

(defmacro read-id []
  `(case (int *oop-size*)
     4 (read-int)
     8 (read-long)))

(defn- read-string-literal
  ([len]
   ;; read string of length
   (let [b (byte-array len)]
     (dotimes [i len]
       (aset b i (read-byte)))
     (new String b)))
  ([]
   ;; read 0-terminated string
   (loop [c (read-byte)
          bb (java.nio.ByteBuffer/allocate 18)]
     (if (= 0 (int c))
       (new String (.array bb))
       (recur (read-byte) (.put bb c))))))

(defn- read-of-type [type]
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

(defn- read-pair []
  (let [t (read-unsigned-byte)]
    {:type (case t
             2 :object
             4 :boolean
             5 :char
             6 :float
             7 :double
             8 :byte
             9 :short
             10 :int
             11 :long)
     :value (read-of-type t)}))

(def ^:private record-readers (vec (repeat 255 nil)))
(defmacro ^:private def-read-record [rectype hex body]
  (assert (keyword? rectype))
  (assert (integer? hex))
  `(alter-var-root #'record-readers
                   assoc ~hex
                   (fn []
                     (-> ~body
                         (assoc :record/type ~(keyword (name rectype)))))))

(def ^:private sub-record-readers (vec (repeat 255 nil)))
(defmacro ^:private def-read-sub-record [rectype hex body]
  (assert (keyword? rectype))
  (assert (integer? hex))
  `(alter-var-root #'sub-record-readers
                   assoc ~hex (fn [] (-> ~body (assoc :record/type ~(keyword (name rectype)))))))

(def-read-record :HPROF_UTF8 0x01
  (let [timestamp (read-int)
        len (read-int)]
    (assert (= 0 timestamp))
    {:record/id (read-id)
     :record/utf8 (-> len (- *oop-size*) read-string-literal)}))

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
     :thread-serial-nnr thread-serial-nr
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
  (let [[timestamp len] [(read-int) (read-int)]]
    {:record/length len}))

(def-read-record :HPROF_HEAP_DUMP_SEGMENT 0x1C
  (let [[timestamp len] [(read-int) (read-int)]]
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
   :stack-trace-sequence-nummber (read-int)})

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
                    (assoc (read-pair)
                           :ip-idx idx)))
        statics (doall (for [_ (range (read-unsigned-short))
                             :let [id (read-id)]]
                         (assoc (read-pair)
                                :id id)))
        inst (doall (for [_ (range (read-unsigned-short))]
                      {:id (read-id)
                       :type (read-unsigned-byte)}))]
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
        content (vec (repeatedly size #(read-byte))) ;; read n bytes
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
        elements (vec (repeatedly nr #(read-of-type type)))]
    {:array-object-id id
     :stack-trace-serial-nr ss
     :elements elements}))

(defn read-sub-record []
  (vreset! *read-bytes* 0)
  (let [b (int (read-byte))]
    (if-let [r (get sub-record-readers b)]
      (assoc (r) :record/length @*read-bytes*)
      (throw (ex-info (str "No reader for subrecord " b) {})))))

;; read n bytes of subrecords
(defn- read-sub-records-of-bytes [byte-count]
  (cond (zero? byte-count) []
        (neg? byte-count)  (assert false)
        :else (let [head (read-sub-record)]
                (cons head (lazy-seq (read-sub-records-of-bytes (- byte-count (:record/length head))))))))

(defn read-record []
  (let [b (int (read-byte))]
    (if-let [r (get record-readers b)]
      (r)
      (throw (ex-info (str "Can not find reader for byte " (int b)) {:byte b})))))

(defn- read-record+subrecords []
  (when (pos? (.available *data-input-stream*))
    (let [head (read-record)]
      (case (:record/type head)
        (:HPROF_HEAP_DUMP :HPROF_HEAP_DUMP_SEGMENT)
        (lazy-cat [head]
                  (read-sub-records-of-bytes (:record/length head))
                  (read-record+subrecords))
        ;; else
        (cons head (lazy-seq (read-record+subrecords)))))))

;; TODO: use it to field instance field values properly.
#_
(defn- map-instance-fields 
  ([records] (map-instance-fields {} records))
  ([id->class records]
   (when-let [[head & tail] (seq records)]
     ;; if head is a class definition then put it in the 
     (cond head-is-class-def
           (cons head (lazy-seq (map-instance-fields (assoc id->class id head) tail)))

           head-is-instance
           ;; parse fields from head to correct types

           :else
           (cons head (lazy-seq (map-instance-fields id->class tail)))))))

(defn read-hprof-file [input]
  (with-open [reader (new java.io.DataInputStream (io/input-stream input))]
    (binding [*data-input-stream* reader]
      (println "before reading header")
      (assert (= "JAVA PROFILE 1.0.2" (read-string-literal)))
      (println "header read")
      (let [id-size    (read-int)
            timestamp  (read-long)]
        (println :id-size id-size :timestamp timestamp)
        (binding [*oop-size* id-size]
          (->> (read-record+subrecords)
               #_(drop-while (comp #{:HPROF_UTF8 :HPROF_FRAME :HPROF_LOAD_CLASS :HPROF_TRACE
                                     :HPROF_GC_CLASS_DUMP} :record/type))
               #_(run! println)
               (dorun)
               ))))))

(defn -main [& args]
  (println :!)
  (dorun
   (read-hprof-file (io/file "/Users/janos.erdos/dump1.hprof")))
  (int 0))