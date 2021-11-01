(ns tfmt-clj.v4
  (:require
   [clojure.pprint :refer [cl-format]]
   [criterium.core :as criterium])
  (:import
   [java.io BufferedWriter]
   [java.lang.management ManagementFactory GarbageCollectorMXBean]
   [java.util Date]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(def ^"[C" the-chars
  (char-array "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 !@#$%^&*()[]\\,./| `~;:'\"{}-_"))
(def ^{:const true
       :tag 'long}
  chars-length (alength the-chars))

;;~3.46x faster than clojure.core/rand-int
(let [^java.util.Random seed (java.util.Random.)]
  (defn  faster-rand-int ^long [^long n]
    (cond (pos? n) (.nextInt  seed n)
          (neg? n) (- (.nextInt  seed n))
          :else 0)))

(defn random-string
  ([n] (random-string n false))
  ([^long n id?]
   (let [res    (char-array n)
         bound  (if id? 36 chars-length)]
     (dotimes [i n]
       (aset res i (aget the-chars (faster-rand-int bound))))
     (String. res))))

(defn generate-rows
  "Return a sequence of N maps acting as pretend rows from a database"
  [^long n]
  (let [^Date now (Date.)]
    (mapv (fn [^long id]
            (try
              {:primary_key (+ 1000000 id)
               :the_text (random-string (+ 4 (faster-rand-int (mod id 12))))
               :the_timestamp (Date. ^long (+ (.getTime now) id))
               :the_bool (if (= 0 (mod id 2)) true false)
               :the_float_value (float id)}
              (catch Exception e (throw (ex-info "bad-in" {:id id})) )))
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
  (let [^long max-rows (apply max (map count headers))]
    (mapv (fn [column-tokens]
            (let [diff (- max-rows (count column-tokens))]
              (if (> diff 0)
                (into [] (concat (repeat diff "") column-tokens))
                column-tokens)))
          headers)))

;;2x faster if we just use reduce-kv and ignore all the intermediate schlock.
;;also change the API for the str-fn to allow a 2 arity k v arg.
;;So we can compute stats (like max width values) in 1 pass.

;;4x faster if we don't compute any intermediate hash maps and just
;;dump the strings in an arraylist, returning a simple closure
;;that lets us get the [row col] th entry or the [nth] entry.
(defn stringify-values
  [rows str-fn]
  (let [cols    (count (first rows))
        entries (java.util.ArrayList. (* (count rows) cols))]
    (doseq [row rows]
      (reduce-kv (fn [_ k v ]
                   (.add entries (str-fn k v))) nil  row))
    (fn store
      ([^long n] (.get entries n))
      ([^long row ^long col] (.get entries (+ (* row cols) col))))))

(defn write-padded-column
  "Write out text of a single column/field of a row,
  using the specified fill character and width."
  [^BufferedWriter writer ^String text ^long fill-char ^long width]
  (let [n-fill    (- width (count text))]
    (.write writer text)
    (dotimes [_ n-fill]
      (.write writer fill-char))))

(defn rows->stringified-widths [rows]
  (let [maxes  (atom {})
        mwv    (fn [k v]
                 (let [^long
                       oldv (@maxes k 0)
                       res  (str v)
                       newv (count res)
                       _    (when (> newv oldv) (swap! maxes assoc k newv))]
                   res))
        rows (stringify-values rows mwv)]
    [rows @maxes]))

(defn report-rows
  "Generate N rows and print them nicely to a file F.

  We couild have used `clojure.pprint/print-rows` but I wanted the function
  logic to be expressed more or less equivaledntly in the two lisp dialects."
  [^long n f]
  (let [rows     (generate-rows n)          ;potentially lots of data
        raw-keys (keys (first rows))
        raw->idx (zipmap raw-keys (range (count raw-keys)))
        row-keys (vec (sort raw-keys))
        col->raw (mapv raw->idx row-keys)

        narrow-headers (normalize-headers (narrow-header-info row-keys))
        n-header-lines (apply max (mapv count narrow-headers))
        n-columns (count row-keys)
        ;;fusing ops using HOF gets us another 20% reduction
        [rows max-value-widths] (rows->stringified-widths rows)

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
          (write-padded-column writer ((narrow-headers column-index) line-number)
                               (int \space) (column-widths column-index))
          (.write writer (int \space)))
        (.write writer (int \newline)))

      ;; A row of dashes
      (dotimes [column-index n-columns]
        (write-padded-column writer "-" (int \-) (column-widths column-index))
        (.write writer (int \space)))
      (.write writer (int \newline))

      ;; Now the values
      (doseq [i (range n)]
        (dotimes [j  n-columns]
          (write-padded-column writer (rows i (col->raw j)) (int \space)
                                      (column-widths j))
          (.write writer (int \space)))
        (.write writer (int \newline))))))


(defn -main
  [& args]
  (let [n (if (= (count args) 1)
            (Integer/parseInt (first args))
            50000)]
    (println "Timing for" n "rows.  GC stats approximate and may reflect post timing cleanups.")
    (criterium/quick-bench (report-rows n "/tmp/clojure-test-rows.out"))
    (flush)))


;;(criterium/quick-bench (report-rows 50000 "rows.out"))
;;Evaluation count : 12 in 6 samples of 2 calls.
;;Execution time mean : 83.798856 ms
