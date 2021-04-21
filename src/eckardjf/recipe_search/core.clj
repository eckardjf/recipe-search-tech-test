(ns eckardjf.recipe-search.core
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [stemmer.snowball :as snowball])
  (:import (java.text Normalizer Normalizer$Form)
           (java.util.zip ZipInputStream)
           (java.io File)))

(defn make-index [] (atom {:docs [] :l-avg 0 :dict {}}))

(defn tokenize [s] (re-seq #"\w+" s))

(defn strip-accents [s]
  (let [decomposed (Normalizer/normalize s Normalizer$Form/NFD)]
    (-> (re-matcher #"\p{InCombiningDiacriticalMarks}+" decomposed) (.replaceAll ""))))

(def normalize (comp string/lower-case strip-accents))

(defn analyze [s] (->> s normalize tokenize (map (snowball/stemmer :english))))

(defn index-file [index file]
  (let [{:keys [docs]} index
        tokens (analyze (slurp file))
        l (count tokens)
        n (count docs)]
    (-> (reduce-kv (fn [m t tf]
                     (update-in m [:dict t] (fnil conj []) {:doc-id n :tf tf :l l}))
                   index
                   (frequencies tokens))
        (update :docs conj file)
        (update :l-avg (fn [prev]
                         (/ (+ l (* prev n))
                            (inc n)))))))

(defn index-directory [index dir]
  (doseq [f (filter #(.isFile %) (file-seq (io/file dir)))]
    (swap! index index-file f)))

(defn bm25-idf [index term]
  (let [{:keys [docs dict]} index
        N (count docs)
        df (count (dict term))]
    (Math/log (inc (/ (+ (- N df) 0.5)
                      (+ df 0.5))))))

(defn bm25-tf [index posting & {:keys [k b] :or {k 1.2 b 0.75}}]
  (let [{:keys [l-avg]} index
        {:keys [tf l]} posting]
    (/ (* tf (+ k 1))
       (* (+ tf k) (+ (- 1 b) (* b (/ l l-avg)))))))

(defn term-scores [index term]
  (let [{:keys [dict]} index
        pl (dict term)]
    (reduce (fn [m {:keys [doc-id] :as p}]
              (assoc m doc-id (* (bm25-tf index p) (bm25-idf index term))))
            {} pl)))

(defn top-docs [index query n]
  (->> (analyze query)
       (map (partial term-scores index))
       (apply merge-with +)
       (sort-by val >)
       (take n)
       (map (partial zipmap [:doc-id :score]))))

(defn doc [index doc-id] (nth (:docs index) doc-id))

(def recipe-pattern #"(?s)^(?<title>.+?)(\nIntroduction:\n(?<introduction>.*?))?(\nIngredients:\n(?<ingredients>.*?))?(\nMethod:\n(?<method>.*))?$")

(defn parse-recipe [^File f]
  (let [s (slurp f)
        m (re-matcher recipe-pattern s)]
    (when (.find m)
      (merge {:filename (.getName f)}
             (when-let [title (.group m "title")]
               {:title (string/trim title)})
             (when-let [introduction (.group m "introduction")]
               {:introduction (string/trim introduction)})
             (when-let [ingredients (.group m "ingredients")]
               {:ingredients (string/trim ingredients)})
             (when-let [method (.group m "method")]
               {:method (string/trim method)})))))

(defn search
  ([index query] (search index query 5))
  ([index query n]
   (->> (top-docs index query n)
        (map (fn [{:keys [doc-id] :as result}]
               (-> (merge result (parse-recipe (doc index doc-id)))
                   (select-keys [:doc-id :score :filename :title :introduction])))))))

(defn unzip!
  ([from] (unzip! from "."))
  ([from to]
   (with-open [is (ZipInputStream. (io/input-stream from))]
     (loop [entry (.getNextEntry is)]
       (when entry
         (if (.isDirectory entry)
           (let [d (io/file to (.getName entry))]
             (when-not (.exists d) (.mkdirs d)))
           (io/copy is (io/file to (.getName entry))))
         (recur (.getNextEntry is)))))))

(comment

  ;; create an empty index
  (def index (make-index))

  ;; unzip the sample archive to the current directory (will create a directory "recipes")
  (unzip! "https://media.riverford.co.uk/downloads/hiring/sse/recipes.zip")

  ;; index the recipes
  (time (index-directory index "recipes"))

  ;; example searches
  (search @index "broccoli stilton soup")
  (search @index "nicoise salad")
  (search @index "aperitif")
  (search @index "ragù")

  )