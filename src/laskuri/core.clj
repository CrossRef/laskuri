(ns laskuri.core
  (:require [flambo.conf :as conf])
  (:require [flambo.api :as f])
  (:require [laskuri.util :as util])
  (:require [clj-time.core :as t])
  (:require [clojure.string :as string])
  (:require [clojure.java.io :as io])
  (:require [clojure.edn :as edn])
  (:use [clojure.tools.logging :only (info error)])
  (:require [environ.core :refer [env]])
  (:import [org.apache.spark.api.java JavaSparkContext StorageLevels])
  (:gen-class main true))

; Five years of logs is 700 GB. Max partition size is 2GB.
; 1000 gives ~700 MB partition size.
(def repartition-amount nil)

(defn get-parsed-lines [ctx location redact?]
  "Get a new input stream of parsed lines."
  (let [logfiles (f/text-file ctx location)
        parsed-lines (f/map logfiles (f/fn [s] (util/try-parse-line s)))
        parsed-lines (f/filter parsed-lines (f/fn [line] (not (nil? line))))]
    (if redact?
      (f/map parsed-lines (f/fn [[date doi domain status]] [date doi (util/redact-domain domain) status]))
      parsed-lines)))

(defn swap
  "Swap keys and values of an K,V pair"
  [coll]
  (f/map coll (f/fn [[cnt k]] [k cnt])))

(defn count-by-key-sorted
  "For a key, value collection, count the key, returning key, count ordered by count.
  Is a 'transformation' producing an RDD, is not an 'action'.
  The Spark count-by-key is an action, so not massively useful for large datasets."
  [collection]
  (swap
    (f/sort-by-key 
      (swap
        (f/reduce-by-key
          (f/map collection (f/fn [[k _]] [k 1]))
          (f/fn [a b] (+ a b))))
      false)))

(defn count-by-key
  "For a key, value collection, count the key, returning key, count.
  Is a 'transformation' producing an RDD, is not an 'action'.
  The Spark count-by-key is an action, so not massively useful for large datasets."
  [collection]
  (f/reduce-by-key
    (f/map collection (f/fn [[k _]] [k 1]))
    (f/fn [a b] (+ a b))))

(defn count-pairs
  [collection]
  (count-by-key (f/map collection (f/fn [[old-key old-value]] [[old-key old-value] 1]))))

(defn sort-by-selector
  "For a key value collection, order by the selector (which acts on the kv pair) and then return in original format."
  [collection sel] 
  (f/map (f/sort-by-key (f/map collection (f/fn [kv] [(sel kv) kv]))) (f/fn [[_ v]] v)))

;; The parts of the analysis are divided into all-time, per year, month and day. This is because each pipline involves (potentially) re-reading the input stream
;; and there seems to be a bug or something that crops up when rereading the stream multiple times. Still unsolved: http://stackoverflow.com/questions/27403732/kryoexception-buffer-overflow-with-very-small-input
;; This does involve re-generating the stream each time, and not caching it, which isn't optimal. 

(defn generate-all-time
  "Generate figures for all-time."
  [ctx input-location output-location redact parsed-lines tasks]
  (let [; doi -> date
        doi-date (f/map parsed-lines (f/fn [[date doi [subdomain domain tld] status]]
                                            [doi date]))
                      
        ;; Outputs
        ; doi -> first date visited
        doi-first-date (f/reduce-by-key doi-date (f/fn [a b] (util/min-date-vector a b)))]
    
    (when (:ever-doi-first-date tasks)
      (.saveAsTextFile (f/map doi-first-date pr-str) (str output-location "/ever-doi-first-date")))))

(defn generate-per-period
  "Generate figures per-day."
  [ctx period input-location output-location redact parsed-lines tasks]
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
        doi-domain-period-date (f/map parsed-lines-period (f/fn [[date doi [subdomain domain tld] status]]
                                            [[doi (str domain "." tld) date] date]))
        
        ; [doi period] -> date
        doi-period-date (f/map parsed-lines-period (f/fn [[date doi [subdomain domain tld] status]]
                                            [[doi date] date]))

        ; [full-url-domain-only domain period] -> date
        domain-period-date (f/map parsed-lines-period (f/fn [[date doi [subdomain domain tld] status]]
                                            [[(str domain "." tld) domain date] date]))

        
        ; [full-url-including-subdomain domain period] -> date
        subdomain-period-date (f/map parsed-lines-period (f/fn [[date doi [subdomain domain tld] status]]
                                            [[(str subdomain "." domain "." tld) domain date] date]))

        ;; outputs
        ;; these are sorted by their respective targets to make importing in bulk easier.
        
        ; [doi domain period] -> count
        doi-domain-period-count (count-by-key doi-domain-period-date)

        ; [doi domain] -> [period count] for grouping into a timeline
        doi-domain-period-count (f/map doi-domain-period-count (f/fn [[[doi domain date] cnt]]
                                                       [[doi domain] [date cnt]]))
        
        ; Convert to timeline
        doi-domain-periods-count (f/group-by-key doi-domain-period-count)
        
        ; [doi period] -> count
        doi-period-count (count-by-key doi-period-date) 
        
        doi-period-count (f/map doi-period-count (f/fn [[[doi date] cnt]]
                                                       [doi [date cnt]]))
        
        ; group by DOI. This gives a timeline per DOI.
        doi-periods-count (f/group-by-key doi-period-count)
        
        ; domain -> count per period
        domain-period-count (count-by-key domain-period-date)
        domain-period-count (f/map domain-period-count (f/fn [[[host domain date] cnt]]
                                                           [host [date cnt]]))
        
        ; group by domain. This gives a timeline per domain.
        domain-periods-count (f/group-by-key domain-period-count)
        
        ; subdomain -> count per period
        subdomain-period-count (count-by-key subdomain-period-date)
        subdomain-period-count (f/map subdomain-period-count (f/fn [[[host domain date] cnt]]
                                                                     [[host domain] [date cnt]]))
        subdomain-periods-count (f/group-by-key subdomain-period-count)
        
        
        ; period -> [[host, count]]
        period-domain-count (f/map domain-period-count (f/fn [[host [date cnt]]] [date [host cnt]]))
        
        ; group by period
        period-domains-count (f/group-by-key period-domain-count)
        period-domains-count-sorted (f/map period-domains-count (f/fn [[date items]] [date (take 100 (reverse (sort-by second items)))]))]
      
      (when (:doi-domain-periods-count tasks)
        (.saveAsTextFile (f/map doi-domain-periods-count pr-str) (str output-location "/" (name period) "-doi-domain-periods-count")))
        
      (when (:doi-periods-count tasks)
        (.saveAsTextFile (f/map doi-periods-count pr-str) (str output-location "/" (name period) "-doi-periods-count")))
        
      (when (:domain-periods-count tasks)
        (.saveAsTextFile (f/map domain-periods-count pr-str) (str output-location "/" (name period) "-domain-periods-count")))
        
      (when (:subdomain-periods-count tasks)
        (.saveAsTextFile (f/map subdomain-periods-count pr-str) (str output-location "/" (name period) "-subdomain-periods-count")))
        
      (when (:top-domains tasks)
        (.saveAsTextFile (f/map period-domains-count-sorted pr-str) (str output-location "/" (name period) "-top-domains")))))

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
        redact (= (.toLowerCase (or (env :redact) "false")) "true")
        dev-local (= (.toLowerCase (or (env :dev-local) "false")) "true")
        
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
          
          parsed-lines (get-parsed-lines sc input-location redact)
        
          ; filter out lines that didn't resolve, leaving good DOIs
          parsed-lines-ok (f/filter parsed-lines (f/fn [[date doi domain status]] (not= 0 (.length status))))
          
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
        (generate-all-time sc input-location output-location redact parsed-cached all-time-tasks))
      
      (when (not (empty? year-tasks))
        (info "per year")
        (generate-per-period sc :year input-location output-location redact parsed-cached year-tasks))
      
      (when (not (empty? month-tasks))
        (info "per month")
        (generate-per-period sc :month input-location output-location redact parsed-cached month-tasks))
      
      (when (not (empty? day-tasks))
        (info "per day")
        (generate-per-period sc :day input-location output-location redact parsed-cached day-tasks)))))