;; Copyright (c) Metadata Partners, LLC.
;; All rights reserved.

(ns org.fressian.caching-test
  (:use [clojure.test.generative :only (defspec) :as test]
        [org.fressian.test-helpers :only (assert=) :as th])
  (:require [org.fressian.generators :as gen]
            [clojure.test.generative.generators :as tgen]
            [org.fressian.api :as fressian])
  (:import [org.fressian.impl BytesOutputStream]))

(set! *warn-on-reflection* true)

(defn cache-session->fressian
  "write-args are a series of [fressianble cache?] pairs."
  [write-args]
  (let [baos (BytesOutputStream.)
        writer (fressian/create-writer baos)]
    (doseq [[idx [obj cache]] (map-indexed vector write-args)]
      (let [_ (.writeObject writer obj cache)])
      (when (= 39 (mod idx 40)) (.writeFooter writer)))
    (fressian/bytestream->buf baos)))

(defn roundtrip-cache-session
  "Roundtrip cache-session through fressian and back."
  [cache-session]
  (-> cache-session cache-session->fressian fressian/create-reader fressian/read-batch))

(defspec strings-with-caching
  roundtrip-cache-session
  [^{:tag (`gen/cache-session `tgen/string)} args]
  (assert (= (map first args) %)))

(defspec fressian-with-caching
  roundtrip-cache-session
  [^{:tag (`gen/cache-session `gen/fressian-builtin)} args]
  (assert= (map first args) %))

(defn compare-cache-and-uncached-versions
  "For each o in objects, print o, its uncached value, and its cached value.
   Used to verify cache skipping"
  [objects]
  (doseq [o objects]
    (println o
             " [Uncached:  "(fressian/byte-buffer-seq (cache-session->fressian [[o false]])) "]"
             " [Cached: " (fressian/byte-buffer-seq (cache-session->fressian [[o true]])) "]")))

(comment
(set! *warn-on-reflection* true)
(in-ns 'user)
(in-ns 'org.fressian.fressian-test)

(require :reload 'org.fressian.fressian-test)

(import org.fressian.CachedObject)
(compare-cache-and-uncached-versions [true false nil -1 0 1 1000 10000 "" "FOO" 1.0 2.0
                                      (CachedObject. 0) (CachedObject. "BAR")])
)


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn roundtrip-cache-socket
  [host port write-args]
  (let [sock (java.net.Socket. host port)
        os (.getOutputStream sock)
        is (.getInputStream sock)
        bbw (java.nio.ByteBuffer/allocate 8)
        bbr (java.nio.ByteBuffer/allocate 8)]
    (.write os (.array (.putLong bbw (count write-args))))
    (.read is (.array bbr)) ; need to check if the long read is equal to (count objs)
    (let [wtr (fressian/create-writer os fressian/clojure-write-handlers)
          rdr (fressian/create-reader is fressian/clojure-read-handlers true)]
      (doseq [[idx [obj cache]] (map-indexed vector write-args)]
        (let [_ (.writeObject wtr obj cache)])
        (.writeFooter wtr))
      (into [] (map-indexed (fn [idx _]
                              (let [ret (.readObject rdr)]
                                (.validateFooter rdr)
                                ret))
                            write-args)))))

(comment 

(defspec fressian-socket-strings-with-caching
  (partial roundtrip-cache-socket "127.0.0.1" 19876)
  [^{:tag (`gen/cache-session `tgen/string)} args]
  (assert (= (map first args) %)))

(defspec fressian-socket-with-caching
  (partial roundtrip-cache-socket "127.0.0.1" 19876)
  [^{:tag (`gen/cache-session `gen/fressian-builtin)} args]
  (assert= (map first args) %))

  (clojure.test.generative/test-vars (var org.fressian.caching-test/fressian-socket-with-caching))
  (clojure.test.generative/test-vars (var org.fressian.caching-test/fressian-socket-strings-with-caching))

)