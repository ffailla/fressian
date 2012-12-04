;; Copyright (c) Metadata Partners, LLC.
;; All rights reserved.

(ns org.fressian.fressian-test
  (:use [clojure.test.generative :only (defspec) :as test]
        [org.fressian.test-helpers :only (assert=)])
  (:require [org.fressian.generators :as gen]
            [org.fressian.api :as fressian])
  (:import [org.fressian.impl BytesOutputStream]))

(set! *warn-on-reflection* true)

(defn roundtrip
  "Fressian and defressian o"
  ([o]
     (-> o fressian/byte-buf fressian/defressian))
  ([o write-handlers read-handlers]
     (-> o
         (fressian/byte-buf :handlers write-handlers)
         (fressian/defressian :handlers read-handlers))))

(defspec fressian-character-encoding
  roundtrip
  [^{:tag `gen/single-char-string} s]
  (assert (= s %)))

(defspec fressian-scalars
  roundtrip
  [^{:tag `gen/scalar} s]
  (assert= s %))

;; terminology TBD: using "builtin" here to describe all out-of-box
;; fressian types
(defspec fressian-builtins
  roundtrip
  [^{:tag `gen/fressian-builtin} s]
  (assert= s %))

(defspec fressian-int-packing
  roundtrip
  [^{:tag `gen/longs-near-powers-of-2} input]
  (assert (= input %)))

(defspec fressian-names
  (fn [o]  
    (roundtrip o fressian/clojure-write-handlers fressian/clojure-read-handlers))
  [^{:tag `gen/symbolic} s]
  (assert= s %))

(defn size
  "Measure the size of a fressianed object. Returns a map of
  :size, :second, :caching-size, and :cached-size.
  (:second will differ from size if there is internal caching.)"
  ([o] (size o nil))
  ([o write-handlers]
     (let [baos (BytesOutputStream.)
           writer (fressian/create-writer baos write-handlers)]
       (.writeObject writer o)
       (let [size (.length baos)]
         (.writeObject writer o)
         (let [second (- (.length baos) size)]
           (.writeObject writer o true)
           (let [caching-size (- (.length baos) second size)]
             (.writeObject writer o true)
             {:size size
              :second second
              :caching-size caching-size
              #_:bytes #_(seq (.internalBuffer baos))
              :cached-size (- (.length baos) caching-size second size)}))))))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn roundtrip-socket
  [host port obj]
  (let [sock (java.net.Socket. ^String host port)
        os (.getOutputStream sock)
        is (.getInputStream sock)
        bbw (java.nio.ByteBuffer/allocate 8)
        bbr (java.nio.ByteBuffer/allocate 8)]
    (.write os (.array (.putLong bbw (long 1))))
    (.read is (.array bbr)) ; need to check if the long read is equal to (count objs)
    (let [wtr (fressian/create-writer os fressian/clojure-write-handlers)
          rdr (fressian/create-reader is fressian/clojure-read-handlers true)]
      (.writeObject wtr obj)
      (.writeFooter wtr)
      (let [ret (.readObject rdr)]
        (.validateFooter rdr)
        ret))))

(defn fressian-server
  []
  (let [svr (java.net.ServerSocket. 19876)
        latch (java.util.concurrent.CountDownLatch. 1)
        readfn (fn [^org.fressian.FressianReader rdr n]
                 (map (fn [_] 
                        (let [o (.readObject rdr)]
                          (.validateFooter rdr) 
                          (println "read" o "from fressian client")
                          o))
                      (range n)))
        writefn (fn [^org.fressian.FressianWriter wtr objs]
                  (map (fn [o]
                         (.writeObject wtr o)
                         (.writeFooter wtr)
                         (println "wrote" o "to fressian client")) 
                       objs))]
    (println "started fressian server")
    {:server svr
     :latch latch
     :future (future
               (loop [cnt (.getCount latch)]
                 (if (pos? cnt)
                   (let [client (.accept svr)]
                     (println "accepted fressian client")
                     (let [is (.getInputStream client)
                           os (.getOutputStream client)
                           bbr (java.nio.ByteBuffer/allocate 8)
                           _ (.read is (.array bbr)) ; read the num of objs that are going to be sent
                           n (.getLong bbr)
                           _ (.write os (.array bbr)) ; ack back what was read
                           _ (println "preparing to read" n "objects from fressian-socket")
                           wtr (fressian/create-writer os fressian/clojure-write-handlers)
                           rdr (fressian/create-reader is fressian/clojure-read-handlers true)
                           objs (readfn rdr n)]
                       (doall (writefn wtr objs))))
                   (.close svr))
                 (recur (.getCount latch))))}))


(comment

(defspec fressian-socket-character-encoding
  (partial roundtrip-socket "127.0.0.1" 19876)
  [^{:tag `gen/single-char-string} s]
  (assert (= s %)))

(defspec fressian-socket-scalars
  (partial roundtrip-socket "127.0.0.1" 19876)
  [^{:tag `gen/scalar} s]
  (assert= s %))

  ;; terminology TBD: using "builtin" here to describe all out-of-box
  ;; fressian types
(defspec fressian-socket-builtins
  (partial roundtrip-socket "127.0.0.1" 19876)
  [^{:tag `gen/fressian-builtin} s]
  (assert= s %))

(defspec fressian-socket-int-packing
  (partial roundtrip-socket "127.0.0.1" 19876)
  [^{:tag `gen/longs-near-powers-of-2} input]
  (assert (= input %)))

  (defspec fressian-socket-names
    (partial roundtrip-socket "127.0.0.1" 19876)
    [^{:tag `gen/symbolic} s]
    (assert= s %))

  (clojure.test.generative/test-vars (var org.fressian.fressian-test/fressian-socket-character-encoding))
  (clojure.test.generative/test-vars (var org.fressian.fressian-test/fressian-socket-scalars))
  (clojure.test.generative/test-vars (var org.fressian.fressian-test/fressian-socket-builtins))
  (clojure.test.generative/test-vars (var org.fressian.fressian-test/fressian-socket-int-packing))
  (clojure.test.generative/test-vars (var org.fressian.fressian-test/fressian-socket-names))
)