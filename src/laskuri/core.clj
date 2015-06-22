(ns laskuri.core
  (:require [flambo.conf :as conf])
  (:require [flambo.api :as f]
            [flambo.tuple :as ft])
  (:require [laskuri.util :as util])
  (:require [clj-time.core :as t])
  (:require [clojure.string :as string])
  (:require [clojure.java.io :as io])
  (:require [clojure.edn :as edn])
  (:use [clojure.tools.logging :only (info error)])
  (:require [environ.core :refer [env]])
  (:require [clojure.java.io :refer [writer]])
  (:import [org.apache.spark.api.java JavaSparkContext StorageLevels])
  (:import [java.io File BufferedWriter])
  (:gen-class main true)
  )

; Five years of logs is 700 GB. Max partition size is 2GB.
; 1000 gives ~700 MB partition size.
(def repartition-amount nil)

(defn save-rdd [rdd file-path]
  (let [lines (f/map rdd #(pr-str [(._1 ^scala.Tuple2 %) 
                                   (if (instance? Iterable (._2 ^scala.Tuple2 %))
                                    (vec (._2 ^scala.Tuple2 %))
                                    (._2 ^scala.Tuple2 %))]))]
      (.saveAsTextFile ^org.apache.spark.api.java.JavaRDD lines file-path)))

(defn get-parsed-lines [ctx location]
  "Get a new input stream of parsed lines."
  (let [logfiles (f/text-file ctx location)
        parsed-lines (f/map logfiles (f/fn [s] (util/try-parse-line s)))
        parsed-lines (f/filter parsed-lines (f/fn [line] (not (nil? line))))]
    parsed-lines))

(defn swap
  "Swap keys and values of an K,V pair"
  [coll]
  (f/map-to-pair coll (ft/key-val-fn (f/fn [cnt k] (ft/tuple k cnt)))))

(defn count-by-key-sorted
  "For a key, value collection, count the key, returning key, count ordered by count.
  Is a 'transformation' producing an RDD, is not an 'action'.
  The Spark count-by-key is an action, so not massively useful for large datasets."
  [collection]
  (swap
    (f/sort-by-key 
      (swap
        (f/reduce-by-key
          (f/map-to-pair collection (ft/key-val-fn (f/fn [k _] (ft/tuple k 1))))
          (f/fn [a b] (+ a b))))
      false)))

(defn count-by-key
  "For a key, value collection, count the key, returning key, count.
  Is a 'transformation' producing an RDD, is not an 'action'.
  The Spark count-by-key is an action, so not massively useful for large datasets."
  [collection]
  (f/reduce-by-key
    (f/map-to-pair collection (ft/key-val-fn (f/fn [k _] (ft/tuple k 1))))
    (f/fn [a b] (+ a b))))

(defn count-pairs
  [collection]
  (count-by-key (f/map-to-pair collection (ft/key-val-fn (f/fn [old-key old-value] (ft/tuple [old-key old-value] 1))))))

(defn sort-by-selector
  "For a key value collection, order by the selector (which acts on the kv pair) and then return in original format."
  [collection sel] 
  (f/map
    (f/sort-by-key
      (f/map collection
             (f/fn [kv] [(sel kv) kv])))
    (ft/key-val-fn (f/fn [_ v] v))))

;; The parts of the analysis are divided into all-time, per year, month and day. This is because each pipline involves (potentially) re-reading the input stream
;; and there seems to be a bug or something that crops up when rereading the stream multiple times. Still unsolved: http://stackoverflow.com/questions/27403732/kryoexception-buffer-overflow-with-very-small-input
;; This does involve re-generating the stream each time, and not caching it, which isn't optimal. 


(defn generate-all-time
  "Generate figures for all-time."
  [ctx input-location output-location parsed-lines tasks]
  (let [; doi -> date
        doi-date (f/map-to-pair parsed-lines (f/fn [[date doi [subdomain domain tld] status]]
                                            (ft/tuple doi date)))
                      
        ;; Outputs
        ; doi -> first date visited
        doi-first-date (f/reduce-by-key doi-date (f/fn [a b] (util/min-date-vector a b)))]
    
    (when (:ever-doi-first-date tasks)
      ; (.saveAsTextFile doi-first-date (str output-location "/ever-doi-first-date"))
      
      (save-rdd doi-first-date (str output-location "/ever-doi-first-date"))
      )))

(defn generate-per-period
  "Generate figures per-day."
  [ctx period input-location output-location parsed-lines tasks]
  {:pre [(#{:year :month :day nil} period)]}
  (let [; date truncated to period
        ; date represents the beginning of the period (i.e. first second of the day, month or year).
        parsed-lines-period   (f/map parsed-lines (f/fn [[date doi domain status]]
                                ; Date is stored as a triple of [year month day]
                                [(condp = period
                                 :year (take 1 date)
                                 :month (take 2 date)
                                 :day (take 3 date)
                                 nil date
                                 date) doi domain]))
        
        ; For the following, the period is included in the key because we're counting unique 'X per period'
        ; (e.g. '10.5555/12345678 per month').
        
        ; [doi domain period] -> date
        doi-domain-period-date (f/map-to-pair parsed-lines-period (f/fn [[date doi [subdomain domain tld] status]]
                                            (ft/tuple [doi (str domain "." tld) date] date)))
        
        ; [doi period] -> date
        doi-period-date (f/map-to-pair parsed-lines-period (f/fn [[date doi [subdomain domain tld] status]]
                                            (ft/tuple [doi date] date)))

        ; [full-url-domain-only domain period] -> date
        domain-period-date (f/map-to-pair parsed-lines-period (f/fn [[date doi [subdomain domain tld] status]]
                                            (ft/tuple [(str domain "." tld) domain date] date)))

        
        ; [full-url-including-subdomain domain period] -> date
        subdomain-period-date (f/map-to-pair parsed-lines-period (f/fn [[date doi [subdomain domain tld] status]]
                                            (ft/tuple [(str subdomain "." domain "." tld) domain date] date)))

        ;; outputs
        ;; these are sorted by their respective targets to make importing in bulk easier.
        
        ; [doi domain period] -> count
        doi-domain-period-count (count-by-key doi-domain-period-date)

        ; [doi domain] -> [period count] for grouping into a timeline
        doi-domain-period-count (f/map-to-pair doi-domain-period-count (ft/key-val-fn
                                                                         (f/fn [[doi domain date] cnt]
                                                                               (ft/tuple [doi domain] [date cnt]))))
        
        ; Convert to timeline
        doi-domain-periods-count (f/group-by-key doi-domain-period-count)
       
       ; [doi period] -> count
       doi-period-count (count-by-key doi-period-date) 
       
       doi-period-count (f/map-to-pair doi-period-count (ft/key-val-fn (f/fn [[doi date] cnt]
                                                      (ft/tuple doi [date cnt]))))
       
       ; group by DOI. This gives a timeline per DOI.
       doi-periods-count (f/group-by-key doi-period-count)
       
       ; domain -> count per period
       domain-period-count (count-by-key domain-period-date)
       domain-period-count (f/map-to-pair domain-period-count (ft/key-val-fn (f/fn [[host domain date] cnt]
                                                          (ft/tuple host [date cnt]))))
       
       ; group by domain. This gives a timeline per domain.
       domain-periods-count (f/group-by-key domain-period-count)
       
       ; subdomain -> count per period
       subdomain-period-count (count-by-key subdomain-period-date)
       subdomain-period-count (f/map-to-pair subdomain-period-count (ft/key-val-fn (f/fn [[host domain date] cnt]
                                                                    (ft/tuple [host domain] [date cnt]))))
       subdomain-periods-count (f/group-by-key subdomain-period-count)
       
       
       ; period -> [[host, count]]
       period-domain-count (f/map-to-pair domain-period-count (ft/key-val-fn (f/fn [host [date cnt]]
                                                                    (ft/tuple date [host cnt]))))
       
       ; group by period
       period-domains-count (f/group-by-key period-domain-count)
       period-domains-count-sorted (f/map period-domains-count (ft/key-val-fn (f/fn [date items]
                                                                     (ft/tuple date (take 100 (reverse (sort-by second items)))))))
        
        ]
      
      (when (:doi-domain-periods-count tasks)
        ; (.saveAsTextFile doi-domain-periods-count (str output-location "/" (name period) "-doi-domain-periods-count"))
        (save-rdd doi-domain-periods-count (str output-location "/" (name period) "-doi-domain-periods-count"))
        )
        
      (when (:doi-periods-count tasks)
        ; (.saveAsTextFile doi-periods-count (str output-location "/" (name period) "-doi-periods-count"))
        (save-rdd doi-periods-count (str output-location "/" (name period) "-doi-periods-count"))
        )
        
      (when (:domain-periods-count tasks)
        ; (.saveAsTextFile domain-periods-count (str output-location "/" (name period) "-domain-periods-count"))
        (save-rdd domain-periods-count (str output-location "/" (name period) "-domain-periods-count"))
        
        )
        
      (when (:subdomain-periods-count tasks)
        ; (.saveAsTextFile subdomain-periods-count (str output-location "/" (name period) "-subdomain-periods-count"))
        (save-rdd subdomain-periods-count (str output-location "/" (name period) "-subdomain-periods-count"))
        
        )
        
      (when (:top-domains tasks)
        ; (.saveAsTextFile period-domains-count-sorted (str output-location "/" (name period) "-top-domains"))
        (save-rdd period-domains-count-sorted (str output-location "/" (name period) "-top-domains"))
        
        )
      
      
      ))

(def acceptable-all-time-tasks #{:ever-doi-first-date})
(def acceptable-period-tasks #{:doi-domain-periods-count :doi-periods-count :domain-periods-count :subdomain-periods-count :top-domains})

(defn read-config
  [input]
  {:post [#(acceptable-all-time-tasks (:all-time %))
          #(acceptable-period-tasks (:year %))
          #(acceptable-period-tasks (:month %))
          #(acceptable-period-tasks (:day %))]}
  
  (let [tasks (edn/read-string input)
        all-time (set (map first (filter #(= :all-time (second %)) tasks)))
        year (set (map first (filter #(= :year (second %)) tasks)))
        month (set (map first (filter #(= :month (second %)) tasks)))
        day (set (map first (filter #(= :day (second %)) tasks)))]
    
    ; Transform into task-names.
    {:all-time all-time
     :year year
     :month month
     :day day}))

(defn -main
  [& args]
  (let [input-location (env :input-location)
        output-location (env :output-location)
        dev-local (= (.toLowerCase (or ^java.lang.String (env :dev-local) "false")) "true")
        
        {all-time-tasks :all-time
         year-tasks :year
         month-tasks :month
         day-tasks :day} (read-config (:laskuri-tasks env))
        
        ; If local, use this config. Otherwise empty, will be loaded from `spark-submit`. 
        conf (if dev-local
                (-> (conf/spark-conf)
                   (conf/master "local[5]")
                   (conf/app-name "laskuri")
                   (conf/set "spark.driver.memory" "500m")
                   (conf/set "spark.executor.memory" "500m")
                   (conf/set "spark.kryoserializer.buffer.mb" "256")
                   (conf/set "spark.eventLog.enabled" "true"))
                (conf/spark-conf))
          sc (f/spark-context conf)
          
          parsed-lines (get-parsed-lines sc input-location)
        
          ; filter out lines that didn't resolve, leaving good DOIs
          parsed-lines-ok (f/filter parsed-lines (f/fn [[date doi domain status]] (not= 0 (.length ^java.lang.String status))))
          
          ; Repartition. The input is/may be gzip files, in which case they'll be in one big partition each.
          ; This can be nil for no repartition.
          parsed-rebalanced (if repartition-amount
            (f/repartition parsed-lines-ok repartition-amount)
            parsed-lines-ok)
          
          parsed-cached (f/persist parsed-rebalanced StorageLevels/DISK_ONLY)]
        
    (when (and input-location output-location)
      (info "Input" input-location)
      (info "Output" output-location)   
      (when (not (empty? all-time-tasks))
        (generate-all-time sc input-location output-location parsed-cached all-time-tasks))
      
      (when (not (empty? year-tasks))
        (info "per year")
        (generate-per-period sc :year input-location output-location parsed-cached year-tasks))
      
      (when (not (empty? month-tasks))
        (info "per month")
        (generate-per-period sc :month input-location output-location parsed-cached month-tasks))
      
      (when (not (empty? day-tasks))
        (info "per day")
        (generate-per-period sc :day input-location output-location parsed-cached day-tasks)))))