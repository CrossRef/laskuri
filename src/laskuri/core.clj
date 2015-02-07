(ns laskuri.core
  (:require [flambo.conf :as conf])
  (:require [flambo.api :as f])
  (:require [laskuri.util :as util])
  (:require [clj-time.core :as t])
  (:require [clojure.string :as string])
  (:require [clojure.java.io :as io])
  (:use [clojure.tools.logging :only (info error)])
  (:require [environ.core :refer [env]])
  (:import [org.apache.spark.api.java JavaSparkContext StorageLevels])
  (:gen-class main true))

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

(defn sort-by-selector
  "For a key value collection, order by the selector (which acts on the kv pair) and then return in original format."
  [collection sel] 
  (f/map (f/sort-by-key (f/map collection (f/fn [kv] [(sel kv) kv]))) (f/fn [[_ v]] v)))

;; The parts of the analysis are divided into all-time, per year, month and day. This is because each pipline involves (potentially) re-reading the input stream
;; and there seems to be a bug or something that crops up when rereading the stream multiple times. Still unsolved: http://stackoverflow.com/questions/27403732/kryoexception-buffer-overflow-with-very-small-input
;; This does involve re-generating the stream each time, and not caching it, which isn't optimal. 

(defn generate-all-time
  "Generate figures for all-time."
  [ctx input-location output-location redact parsed-lines]
  (let [; doi -> date
        doi-date (f/map parsed-lines (f/fn [[date doi [subdomain domain tld] status]]
                                            [doi date]))
        
        ; domain -> date
        domain-date (f/map parsed-lines (f/fn [[date doi [subdomain domain tld] status]]
                                              [(str domain "." tld) date]))

        ; [domain with subdomain, domain] -> doi
        subdomain-doi (f/map parsed-lines (f/fn [[date doi [subdomain domain tld] status]]
                                                [[(str subdomain "." domain "." tld) domain] doi]))
        
        ; domain -> doi
        domain-doi (f/map parsed-lines (f/fn [[date doi [subdomain domain tld] status]]
                                             [(str domain "." tld) doi]))
              
        ;; Outputs
        
        ; doi -> first date visited
        doi-first-date (f/reduce-by-key doi-date (f/fn [a b] (util/min-date-vector a b)))

        ; doi -> count
        doi-count (count-by-key-sorted doi-date)
        
        ; domain -> count
        domain-count (count-by-key-sorted domain-date)
        
        ; domain and subdomain, domain -> count
        ; including both subdomain and domain is necessary for the consumer of this dataset.
        subdomain-count (count-by-key-sorted subdomain-doi)]
    (.saveAsTextFile (f/map doi-first-date pr-str) (str output-location "/ever-doi-first-date"))
    (.saveAsTextFile (f/map doi-count pr-str) (str output-location "/ever-doi-count"))
    (.saveAsTextFile (f/map domain-count pr-str) (str output-location "/ever-domain-count"))
    (.saveAsTextFile (f/map subdomain-count pr-str) (str output-location "/ever-subdomain-count"))))

(defn generate-per-period
  "Generate figures per-day."
  [ctx period input-location output-location redact parsed-lines]
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
        
        ; [doi period] -> count
        doi-period-count (count-by-key doi-period-date) 
        
        ; sort by domain. Due to an unresolve scoping issue, this can't be extracted into a function yet.
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
    
        (prn "******" (.toDebugString doi-periods-count))
            (prn "******" (.toDebugString domain-periods-count))
                (prn "******" (.toDebugString subdomain-periods-count))
                    (prn "******" (.toDebugString period-domains-count-sorted))
    
      (.saveAsTextFile (f/map doi-periods-count pr-str) (str output-location "/" (name period) "-doi-periods-count"))
      (.saveAsTextFile (f/map domain-periods-count pr-str) (str output-location "/" (name period) "-domain-periods-count"))
      (.saveAsTextFile (f/map subdomain-periods-count pr-str) (str output-location "/" (name period) "-subdomain-periods-count"))
      (.saveAsTextFile (f/map period-domains-count-sorted pr-str) (str output-location "/" (name period) "-top-domains"))))

(defn -main
  [& args]
  (let [input-location (env :input-location)
        output-location (env :output-location)
        redact (= (.toLowerCase (or (env :redact) "false")) "true")
        dev-local (= (.toLowerCase (or (env :dev-local) "false")) "true")
        
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
          
          parsed-cached (f/persist parsed-lines-ok StorageLevels/DISK_ONLY)]
        
    (when (and input-location output-location)
      (info "Input" input-location)
      (info "Output" output-location)   
      (when (= (.toLowerCase (or (env :alltime) "false")) "true")
        (info "generate-all-time")
        (generate-all-time sc input-location output-location redact parsed-cached))
      
      (when (= (.toLowerCase (or (env :year) "false")) "true")
        (info "per year")
        (generate-per-period sc :year input-location output-location redact parsed-cached))
      
      (when (= (.toLowerCase (or (env :month) "false")) "true")
        (info "per month")
        (generate-per-period sc :month input-location output-location redact parsed-cached))
      
      (when (= (.toLowerCase (or (env :day) "false")) "true")
        (info "per day")
        (generate-per-period sc :day input-location output-location redact parsed-cached)))))