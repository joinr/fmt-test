(ns tfmt-clj.core
  (:require 
   [clojure.pprint :refer [cl-format]]
   [criterium.core :as c])
  (:import 
   [java.lang.management ManagementFactory]
   [java.util Date])
  (:gen-class))

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

(def ^:const the-chars                  ; named to avoid smashing Clojure's `chars` function
  "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 !@#$%^&*()[]\\,./| `~;:'\"{}-_")
(def ^:const chars-length (count the-chars))

(defn random-string 
  "Make and return a random string of length N.  If `id?` is true, restrict chars to first 36
  which make for friendly identifiers."
  ([n]
   (random-string n false))
  ([n id?]
   ;; There's no particularly good clojure way to make a string from a sequence of chars...
   ;; If you're willing to drop into java, StringBuilder would be good.
   (apply str (repeatedly n #(nth the-chars (rand-int 
                                             (if id? 36 chars-length)))))))

(defn generate-rows
  "Return a sequence of N maps acting as pretend rows from a database"
  [n]
  (let [now (Date.)]
    (mapv (fn [id1 id2 id3 id4 id5]
            {:primary_key (+ 1000000 id1)
             :the_text (random-string (+ 4 (rand-int (mod id2 12))))
             :the_timestamp (Date. ^long (+ (.getTime now) id3))
             :the_bool (if (= 0 (mod id4 2)) true false)
             :the_float_value (float id5)})
          (range 0 n)
          (range 0 n)
          (range 0 n)
          (range 0 n)
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
  (let [max-rows (apply max (mapv count headers))]
    (mapv (fn [column-tokens]
            (let [diff (- max-rows (count column-tokens))]
              (if (> diff 0)
                (into [] (concat (repeat diff "") column-tokens))
                column-tokens)))
          headers)))

(defn header-max-width
  "Return the maximum header width of data returned by `narrow-header-info`.
  E.g. `(header-max-width (narrow-header-info [:a :b_c2 :d_e03_f :g])) => 3`"
  [nhi]
  (reduce (fn [result column-labels]
            (let [m (apply max (mapv count column-labels))] ;could be reduce
              (if (> m result)
                m
                result)))
          0 nhi))

(defn stringify-values
  "Given rows as returned by `generate-rows`, return an equivalent collection of modified
  rows whose values are all string representations of the input rows.  The conversion
  is done by calling `(str-fn map-value)` for all map entries.

  E.g. `(stringify-values [{:a 1}] str) => [:a \"1\"]`"
  [rows str-fn]
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
  (reduce (fn [result row]
            (reduce (fn [r [k v]]
                      (let [c (count v)
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

(def state (atom nil))

(defn report-rows
  "Generate N rows and print them nicely to a file F."
  [n f]
  (let [rows (generate-rows n)          ;potentially lots of data
        row-keys (keys (first rows))    ;this will be the order of columns

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

        ;; Map keyed by row-keys, valued by format directives for each column
        fmt-strings (reduce (fn [result k]
                              (assoc result k (str "~" (get max-widths k) "a ")))
                            {}
                            row-keys)
        _ (reset! state {:max-width max-widths :fmt-strings fmt-strings :row-keys row-keys
                         :narrow-headers narrow-headers})]

    (with-open [os (clojure.java.io/writer f)]
      ;;; Emit the column headers, which may require multiple lines
      (dotimes [line-number n-header-lines]
        (dotimes [column-index n-columns]
          (cl-format os
                     (get fmt-strings (nth row-keys column-index))
                     (nth (nth narrow-headers column-index) line-number)))
        (cl-format os "~%"))

      ;; A row of dashes
      (doseq [k row-keys]
        (cl-format os "~? " (str "~" (get max-widths k) ",,,'-a") "-"))
      (cl-format os "~%")

      ;; Now the values
      (doseq [row rows]
        (doseq [k row-keys]
          (cl-format os (get fmt-strings k) (get row k)))
        (cl-format os "~%")))))

;;~8788ms yikes!

;;2x as fast, still slow.
(defn report-rows-compiled
  "Generate N rows and print them nicely to a file F."
  [n f]
  (let [rows (generate-rows n)          ;potentially lots of data
        row-keys (keys (first rows))    ;this will be the order of columns

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

        ;; Map keyed by row-keys, valued by format directives for each column
        fmt-strings (reduce (fn [result k]
                              (assoc result k (#'clojure.pprint/compile-format (str "~" (get max-widths k) "a "))))
                            {}
                            row-keys)
        _ (reset! state {:max-width max-widths :fmt-strings fmt-strings :row-keys row-keys
                         :narrow-headers narrow-headers})]

    (with-open [os (clojure.java.io/writer f)]
      ;;; Emit the column headers, which may require multiple lines
      (dotimes [line-number n-header-lines]
        (dotimes [column-index n-columns]
          (cl-format os
                     (get fmt-strings (nth row-keys column-index))
                     (nth (nth narrow-headers column-index) line-number)))
        (cl-format os "~%"))

      ;; A row of dashes
      (doseq [k row-keys]
        (cl-format os "~? " (str "~" (get max-widths k) ",,,'-a") "-"))
      (cl-format os "~%")

      ;; Now the values
      (doseq [row rows]
        (doseq [k row-keys]
          (cl-format os (get fmt-strings k) (get row k)))
        (cl-format os "~%")))))

(defn report-rows-format
  "Generate N rows and print them nicely to a file F."
  [n f]
  (let [rows (generate-rows n)          ;potentially lots of data
        row-keys (keys (first rows))    ;this will be the order of columns

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

        fmt-strings (reduce (fn [result k]
                              (assoc result k (str "~" (get max-widths k) "a ")))
                            {}
                            row-keys)

        ;; Map keyed by row-keys, valued by format directives for each column
        fast-fmt-strings (reduce (fn [result k]
                                   (assoc result k (str "%-" (get max-widths k) "s ")))
                                 {}
                                 row-keys)]

    (with-open [os (clojure.java.io/writer f)]
      (binding [*out* os]
      ;;; Emit the column headers, which may require multiple lines
        (dotimes [line-number n-header-lines]
          (dotimes [column-index n-columns]
            (cl-format os
                       (get fmt-strings (nth row-keys column-index))
                       (nth (nth narrow-headers column-index) line-number)))
          (cl-format os "~%"))

        ;; A row of dashes
        (doseq [k row-keys]
          (cl-format os "~? " (str "~" (get max-widths k) ",,,'-a") "-"))
        (println (format  "%n"))

        ;; Now the values
        (doseq [row rows]
          (doseq [k row-keys]
            (print (format (get fast-fmt-strings k) (get row k))))
          (print (format  "%n")))))))

;;~890 ms, 10x, still blech

(defn report-rows-format-wl
  "Generate N rows and print them nicely to a file F."
  [n f]
  (let [rows (generate-rows n)          ;potentially lots of data
        row-keys (keys (first rows))    ;this will be the order of columns

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

        fmt-strings (reduce (fn [result k]
                              (assoc result k (str "~" (get max-widths k) "a ")))
                            {}
                            row-keys)

        ;; Map keyed by row-keys, valued by format directives for each column
        fast-fmt-strings (reduce (fn [result k]
                                   (assoc result k (str "%-" (get max-widths k) "s ")))
                                 {}
                                 row-keys)]

    (with-open [os (clojure.java.io/writer f)]
      (binding [*out* os]
      ;;; Emit the column headers, which may require multiple lines
        (dotimes [line-number n-header-lines]
          (dotimes [column-index n-columns]
            (cl-format os
                       (get fmt-strings (nth row-keys column-index))
                       (nth (nth narrow-headers column-index) line-number)))
          (cl-format os "~%"))

        ;; A row of dashes
        (doseq [k row-keys]
          (cl-format os "~? " (str "~" (get max-widths k) ",,,'-a") "-"))
        (.write os (format  "%n"))

        ;; Now the values
        (doseq [row rows]
          (doseq [k row-keys]
            (.write os (format (get fast-fmt-strings k) (get row k))))
          (.write os (format  "%n")))))))

;;~611ms, shaving.

(defn generate-rows-seq
  "Return a sequence of N maps acting as pretend rows from a database"
  [n]
  (let [now (Date.)]
    (map (fn [id]
           {:primary_key (+ 1000000 id)
            :the_text (random-string (+ 4 (rand-int (mod id 12))))
            :the_timestamp (Date. ^long (+ (.getTime now) id))
            :the_bool (if (= 0 (mod id 2)) true false)
            :the_float_value (float id)})
         (range 0 n))))

(defn report-rows-format-wl-seq
  "Generate N rows and print them nicely to a file F."
  [n f]
  (let [rows     (generate-rows-seq n)          ;potentially lots of data
        row-keys (keys (first rows))    ;this will be the order of columns

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

        fmt-strings (reduce (fn [result k]
                              (assoc result k (str "~" (get max-widths k) "a ")))
                            {}
                            row-keys)

        ;; Map keyed by row-keys, valued by format directives for each column
        fast-fmt-strings (reduce (fn [result k]
                                   (assoc result k (str "%-" (get max-widths k) "s ")))
                                 {}
                                 row-keys)]

    (with-open [os (clojure.java.io/writer f)]
      (binding [*out* os]
      ;;; Emit the column headers, which may require multiple lines
        (dotimes [line-number n-header-lines]
          (dotimes [column-index n-columns]
            (cl-format os
                       (get fmt-strings (nth row-keys column-index))
                       (nth (nth narrow-headers column-index) line-number)))
          (cl-format os "~%"))

        ;; A row of dashes
        (doseq [k row-keys]
          (cl-format os "~? " (str "~" (get max-widths k) ",,,'-a") "-"))
        (.write os (format  "%n"))

        ;; Now the values
        (doseq [row rows]
          (doseq [k row-keys]
            (.write os (format (get fast-fmt-strings k) (get row k))))
          (.write os (format  "%n")))))))
;;~430ms

(defn gc-stats
  []
  (let [garbageCollectorMXBeans (ManagementFactory/getGarbageCollectorMXBeans)]
    (doseq [garbageCollectorMXBean garbageCollectorMXBeans]
      (cl-format true "  ~21a  Total Collections: ~7d  Total Elapsed MS: ~9d~%"
                 (.getName garbageCollectorMXBean)
                 (.getCollectionCount garbageCollectorMXBean)
                 (.getCollectionTime garbageCollectorMXBean)))))

(defn -main
  [& args]
  (let [n 50000]
    (println "Timing for" n "rows.  GC stats approximate and may reflect post timing cleanups.")
    (System/gc)
    (gc-stats)
    (time (report-rows n "/tmp/clojure-test-rows.out"))
    (gc-stats)))
            
;; Clojure 1.10.3, Java 11.0.12.   Best of 3 tries.
;;;; Timing for 50000 rows.  GC stats approximate and may reflect post timing cleanups.
;;;;   G1 Young Generation    Total Collections:     153  Total Elapsed MS:       502
;;;;   G1 Old Generation      Total Collections:       3  Total Elapsed MS:       163
;;;; "Elapsed time: 15132.509855 msecs"
;;;;   G1 Young Generation    Total Collections:     236  Total Elapsed MS:       629
;;;;   G1 Old Generation      Total Collections:       3  Total Elapsed MS:       163
