;;from  https://www.reddit.com/r/lisp/comments/qk3eu2/revisited_a_casual_clojure_common_lisp/
(ns tfmt-clj.v2
  (:require 
   [clojure.pprint :refer [cl-format]]
   [criterium.core :as criterium])
  (:import 
   [java.io BufferedWriter]
   [java.lang.management ManagementFactory GarbageCollectorMXBean]
   [java.util Date])
  (:gen-class))

(set! *warn-on-reflection* true)

;;; Tooling to take a bunch of (mock) maps representing rows as from clojure.java.jdbc
;;; e.g. {:x_id 1 :the_text "foo" :the_timestamp <inst>} ... whatever
;;; and emit it in fixed width rows via [cl-]format.  Kind of what one might imagine
;;; the psql client doing to ensure the column widths are large enough for output, 
;;; though not with a bunch of inefficient maps as input.

;;; This is casual code, the sort of code I see often in production written by
;;; casual programmers, no attempts were made at efficiency. The focus was more on 
;;; writing code similar to what I see other people writing in clojure dealing with jdbc.
;;; Perhaps I'm doing an injustice presenting this as some approximation of what I see
;;; clojure programmers do, YMMV.
;;;
;;; It is going to be used to compare to an equivalent bit of Common Lisp code.
;;; While the common lisp code isn't identical in its data structures 
;;; (a list here and there instead of a vector), it does go to some pains to at least
;;; use hash-maps for row representations just like we do in this clojure test.
;;; Although those are not immutable structures, but hey, that's kind of the point of 
;;; the comparison, answering the question: how much do we pay for all this functional
;;; lazy-seq abstracted clojure goodness?

(def ^{:const true
       :type String}
  the-chars                  ; named to avoid smashing Clojure's `chars` function
  "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 !@#$%^&*()[]\\,./| `~;:'\"{}-_")
(def ^{:const true
       :type Integer}
  chars-length (count the-chars))

(defn random-string 
  "Make and return a random string of length N.  If `id?` is true, restrict chars to first 36
  which make for friendly identifiers."
  ([^Long n]
   (random-string n false))
  ([^Long n id?]
   ;; There's no particularly good clojure way to make a string from a sequence of chars...
   ;; If you're willing to drop into java, StringBuilder would be good.
   ;; This is not a casual code, some clojure programmers don't even know java.
   ;; Used here as a small bone to the dog.
   (let [^StringBuilder sb (StringBuilder. n)]
     (dotimes [_ n]
       (.append sb ^char (nth the-chars (rand-int (if id? 36 chars-length)))))
     (.toString sb))
   #_
   (apply str (repeatedly n #(nth the-chars (rand-int 
                                             (if id? 36 chars-length)))))))

(defn generate-rows
  "Return a sequence of N maps acting as pretend rows from a database"
  [^Long n]
  (let [^Date now (Date.)]
    (mapv (fn [^Long id]
            {:primary_key (+ 1000000 id)
             :the_text (random-string (+ 4 (rand-int (mod id 12))))
             :the_timestamp (Date. ^long (+ (.getTime now) id))
             :the_bool (if (= 0 (mod id 2)) true false)
             :the_float_value (float id)})
          (range 0 n))))

(defn narrow-header-info
  "Given a sequence of keywords corresponding to a row map, return a sequence
  of collections of strings by tokenizing the keywords if there is more than one token.

  E.g. `(narrow-header-info [:a :b_c :d_e_f]) => [[\"a\"] [\"b\" \"c\"] [\"d\" \"e\" \"f\"]])`"
  [keywords]
  (reduce (fn [result k] (conj result (clojure.string/split (name k) #"_")))
          []
          keywords))

(defn normalize-headers
  "Return an updated result from `narrow-header-info` with the same number of header tokens
  for each column, prepending empty strings to be printed if the column has fewer tokens than
  other columns.

  E.g. ```(normalize-headers [[\"a\"] [\"b\" \"c\"] [\"d\" \"e\" \"f\"]])
          => [[\"\" \"\" \"a\"] [\"\" \"b\" \"c\"] [\"d\" \"e\" \"f\"]]```"
  [headers]
  (let [^Long max-rows (apply max (mapv count headers))]
    (mapv (fn [column-tokens]
            (let [^Long diff (- max-rows (count column-tokens))]
              (if (> diff 0)
                (into [] (concat (repeat diff "") column-tokens))
                column-tokens)))
          headers)))

#_
(defn header-max-width
  "Return the maximum header width of data returned by `narrow-header-info`.
  E.g. `(header-max-width (narrow-header-info [:a :b_c2 :d_e03_f :g])) => 3`"
  [nhi]
  (reduce (fn [^Long result column-labels]
            (let [^Long m (apply max (mapv count column-labels))] ;could be reduce
              (if (> m result)
                m
                result)))
          0 nhi))

(defn stringify-values
  "Given rows as returned by `generate-rows`, return an equivalent collection of modified
  rows whose values are all string representations of the input rows.  The conversion
  is done by calling `(str-fn map-value)` for all map entries.

  E.g. `(stringify-values [{:a 1}] str) => [:a \"1\"]`"
  [rows ^clojure.lang.IFn str-fn]
  ;; Multiple ways we could do this in clojure, either modifying the existing tree
  ;; (which returns new possibly shared tree copy), or building a new tree, or building a vector
  ;; of k/v pairs then putting them into a tree.  We've done the last.
  (mapv (fn [row-map] 
          (into {} 
                (mapv (fn [[k v]] [k (str-fn v)]) 
                      (seq row-map))))
        rows))

(defn value-max-width
  "Given rows as returned by `stringify-values`
  return a single map keyed by map keys found in the rows, and valued by the maximum width 
  string value for that map key across all rows.  So we're going to look at every k/v pair
  in every row.

  E.g. `(value-max-width [{:a \"1\" :b \"xx\"} {:a \"123\" :b \"y\"}]) => {:a 3 :b 2}`"
  [rows]
  (reduce (fn [^clojure.lang.PersistentArrayMap result row]
            (reduce (fn [r [k ^String v]]
                      (let [c (count v) ;didn't like ^Long, unsure why
                            e (get r k)]
                        (if-not e
                          (assoc r k c)
                          (if (> c e)
                            (assoc r k c)
                            r))))
                    result
                    (seq row)))
          {}
          rows))

(defn write-padded-column 
  "Write out text of a single column/field of a row, 
  using the specified fill character and width."
  [^BufferedWriter writer ^String text ^Character fill-char ^Long width]
  (let [n-fill (- width (count text))]
    (.write writer text)
    (dotimes [_ n-fill]
      (.write writer (int fill-char)))))

(defn report-rows
  "Generate N rows and print them nicely to a file F.

  We couild have used `clojure.pprint/print-rows` but I wanted the function
  logic to be expressed more or less equivaledntly in the two lisp dialects."
  [^Long n f]
  (let [rows (generate-rows n)          ;potentially lots of data
        row-keys (sort (keys (first rows)))

        ;; [["" "a"] ["b" "c"] ...]
        narrow-headers (normalize-headers (narrow-header-info row-keys))
        n-header-lines (apply max (mapv count narrow-headers))
        n-columns (count row-keys)
        rows (stringify-values rows str)    ;let old rows be GC'd after this
        max-value-widths (value-max-width rows)

        ;; Header width [n] for  row-keys [n]
        max-header-widths (mapv (fn [col-headers]
                                  (apply max (mapv count col-headers)))
                                narrow-headers)
        ;; header widths as a map keyed by row keys
        header-width-map (zipmap row-keys max-header-widths)

        ;; Merge header widths with value widths
        max-widths (merge-with max max-value-widths header-width-map)

        ;; Each column-widths[n] corresponds to row-keys[n].
        column-widths (mapv #(get max-widths %) row-keys)]

    (with-open [^BufferedWriter writer (clojure.java.io/writer f)]
      ;;; Emit the column headers, which may require multiple lines
      (dotimes [line-number n-header-lines]
        (dotimes [column-index n-columns]
          (write-padded-column writer (nth (nth narrow-headers column-index) line-number)
                               \space (nth column-widths column-index))
          (.write writer (int \space)))
        (.write writer (int \newline)))

      ;; A row of dashes
      (dotimes [column-index n-columns]
        (write-padded-column writer "-" \- (nth column-widths column-index))
        (.write writer (int \space)))
      (.write writer (int \newline))

      ;; Now the values
      (doseq [row rows]
        (dotimes [column-index n-columns]
          (write-padded-column writer (get row (nth row-keys column-index)) \space
                               (nth column-widths column-index))
          (.write writer (int \space)))
        (.write writer (int \newline))))))

#_
(defn gc-stats
  []
  (let [garbageCollectorMXBeans (ManagementFactory/getGarbageCollectorMXBeans)]
    (doseq [^GarbageCollectorMXBean garbageCollectorMXBean garbageCollectorMXBeans]
      (cl-format true "  ~21a  Total Collections: ~7d  Total Elapsed MS: ~9d~%"
                 (.getName garbageCollectorMXBean)
                 (.getCollectionCount garbageCollectorMXBean)
                 (.getCollectionTime garbageCollectorMXBean)))))

#_ ; to compare to the first version of this tes.
(defn -main
  [& args]
  (let [n 50000]
    (println "Timing for" n "rows.  GC stats approximate and may reflect post timing cleanups.")
    (System/gc)
    (gc-stats)
    (time (report-rows n "/tmp/clojure-test-rows.out"))
    (gc-stats)))
            
(defn -main
  [& args]
  (let [n (if (= (count args) 1)
            (Integer/parseInt (first args))
            50000)]
    (println "Timing for" n "rows.  GC stats approximate and may reflect post timing cleanups.")
    (criterium/quick-bench (report-rows n "/tmp/clojure-test-rows.out"))
    (flush)))

;; Clojure 1.10.3, Java 11.0.12. Criterium quick-bench via uberjar. 500,000 rows, -Xmx8g
;;;; java -Xmx8g -jar /home/dave/tfmt-clj/target/uberjar/tfmt-clj-0.1.0-SNAPSHOT-standalone.jar 500000
;;;; Timing for 500000 rows.  GC stats approximate and may reflect post timing cleanups.
;;;; Evaluation count : 6 in 6 samples of 1 calls.
;;;;              Execution time mean : 2.284601 sec
;;;;     Execution time std-deviation : 142.610635 ms
;;;;    Execution time lower quantile : 2.079832 sec ( 2.5%)
;;;;    Execution time upper quantile : 2.388458 sec (97.5%)
;;;;                    Overhead used : 7.256331 ns
;;;; 

;; Clojure 1.10.3, Java 11.0.12. Criterium quick-bench via uberjar. 50,000 rows.
;; Default memory on a 32MB machine, whatever that is.
;;;; Evaluation count : 6 in 6 samples of 1 calls.
;;;;              Execution time mean : 228.755579 ms
;;;;     Execution time std-deviation : 27.718911 ms
;;;;    Execution time lower quantile : 208.253441 ms ( 2.5%)
;;;;    Execution time upper quantile : 273.481688 ms (97.5%)
;;;;                    Overhead used : 7.182008 ns
;;;; 
;;;; Found 1 outliers in 6 samples (16.6667 %)
;;;;         low-severe       1 (16.6667 %)
;;;;  Variance from outliers : 31.3626 % Variance is moderately inflated by outliers

;; Clojure 1.10.3, Java 11.0.12. Criterium quick-bench via slime REPL. 50,000 rows.
;;;; Timing for 50000 rows.  GC stats approximate and may reflect post timing cleanups.
;;;; Evaluation count : 6 in 6 samples of 1 calls.
;;;;              Execution time mean : 437.090746 ms
;;;;     Execution time std-deviation : 17.293583 ms
;;;;    Execution time lower quantile : 421.713980 ms ( 2.5%)
;;;;    Execution time upper quantile : 455.828005 ms (97.5%)
;;;;                    Overhead used : 17.024310 ns

;; Clojure 1.10.3, Java 11.0.12.   Best of 3 tries via slime REPL (to compare to V0 timings)
;; Using clojure's `time` instead of criterium.    The old value was 15132.509855 msecs.
;;;; Timing for 50000 rows.  GC stats approximate and may reflect post timing cleanups.
;;;;  G1 Young Generation    Total Collections:      57  Total Elapsed MS:       283
;;;;  G1 Old Generation      Total Collections:       2  Total Elapsed MS:       169
;;;; "Elapsed time: 419.534237 msecs"
;;;;  G1 Young Generation    Total Collections:      58  Total Elapsed MS:       291
;;;;  G1 Old Generation      Total Collections:       2  Total Elapsed MS:       169



;;tfmt-clj.v2> (c/quick-bench (report-rows 50000 "rows.out"))
;;Evaluation count : 6 in 6 samples of 1 calls.
;;Execution time mean : 261.849348 ms
