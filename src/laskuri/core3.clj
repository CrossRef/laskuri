(ns laskuri.core
  (:require [flambo.conf :as conf])
  (:require [flambo.api :as f])
  
  (:require [laskuri.util :as util])
  (:require [clj-time.core :as t])
  
  (:require [clojure.java.io :as io])
  (:use [clojure.tools.logging :only (info error)])
  (:gen-class))

(def c (-> (conf/spark-conf)
           (conf/master "local")
           (conf/app-name "laskuri")
           (conf/set "spark.driver.memory" "500m")
           (conf/set "spark.executor.memory" "500m")
           (conf/set "spark.kryoserializer.buffer.mb" "256") ; TODO tune
           ))

(def sc (f/spark-context c))



(defn format-doi-value
  [[doi date]]
  (format "%s\t%s" doi (str date)))

(def all-input "file:///Users/joe/data/doi-logs/doi_logs")
(def ten-thou-input "file:///Users/joe/data/doi-logs-small/access_log_201210_cr5-tenthou")
(def hundred-input "file:///Users/joe/data/doi-logs-small/access_log_201210_cr5-hundred")
(def ten-input "file:///Users/joe/data/doi-logs-small/access_log_201210_cr5-ten")
(def one-input "file:///Users/joe/data/doi-logs-small/access_log_201210_cr5-one")
(def single-input "file:///Users/joe/data/doi-logs-small/access_log_201210_cr5")

(def tmp-output-dir "/tmp/p")

(defn get-parsed-lines [ctx location]
  "Get a new input stream of parsed lines."
  (let [logfiles (f/text-file ctx location)
        parsed-lines (f/map logfiles (f/fn [s] (util/parse-line s)))
        parsed-lines (f/filter parsed-lines (f/fn [line] (not (nil? line))))]
    parsed-lines))

(defn swap
  "Swap keys and values of an K,V pair"
  [coll]
  (f/map coll (f/fn [[cnt k]] [k cnt])))

(defn count-by-key
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

;; The parts of the analysis are divided into all-time, per year, month and day. This is because each pipline involves (potentially) re-reading the input stream
;; and there seems to be a bug or something that crops up when rereading the stream multiple times. Still unsolved: http://stackoverflow.com/questions/27403732/kryoexception-buffer-overflow-with-very-small-input
;; This does involve re-generating the stream each time, and not caching it, which isn't optimal. 

(defn generate-all-time
  "Generate figures for all-time."
  [ctx input-location output-location]
  (let [parsed-lines (get-parsed-lines ctx input-location)
        
        ; doi -> date
        doi-date (f/map parsed-lines (f/fn [[date doi [subdomain domain tld]]]
                                            [doi date]))
        
        ; domain -> date
        domain-date (f/map parsed-lines (f/fn [[date doi [subdomain domain tld]]]
                                              [(str domain "." tld) date]))

        ; domain with subdomain -> doi
        subdomain-doi (f/map parsed-lines (f/fn [[date doi [subdomain domain tld]]]
                                                [(str subdomain "." domain "." tld) doi]))
        
        ; domain -> doi
        domain-doi (f/map parsed-lines (f/fn [[date doi [subdomain domain tld]]]
                                             [(str domain "." tld) doi]))
              
        ;; Outputs
        
        ; doi -> first date visited
        doi-first-date (f/reduce-by-key doi-date (f/fn [a b] (util/min-date a b)))

        ; doi -> count
        doi-count (count-by-key doi-date)
        
        ; domain -> count
        domain-count (count-by-key domain-date)
        ]
    
    ; (.saveAsTextFile (f/map doi-date format-doi-value) (str output-location "/ever/doi-date"))
    ; (.saveAsTextFile (f/map domain-date format-doi-value) (str output-location "/ever/domain-date"))
    ; (.saveAsTextFile (f/map subdomain-doi format-doi-value) (str output-location "/ever/subdomain-doi"))
    ; (.saveAsTextFile (f/map domain-doi format-doi-value) (str output-location "/ever/domain-doi")))
  

  
    (.saveAsTextFile (f/map doi-first-date format-doi-value) (str output-location "/ever/doi-first-date"))
    (.saveAsTextFile (f/map doi-count format-doi-value) (str output-location "/ever/doi-count"))
    (.saveAsTextFile (f/map domain-count format-doi-value) (str output-location "/ever/domain-count"))
    
    
    
    
  
  
  ))



(defn generate-per-period
  "Generate figures per-day."
  [ctx period input-location output-location]
  {:pre [(#{:year :month :day nil} period)]}
  (let [parsed-lines (get-parsed-lines ctx input-location)

        ; date truncated to period
        parsed-lines-period (f/map parsed-lines (f/fn [[date doi domain]]
                                                    [(condp = period
                                                     :year (util/truncate-year date)
                                                     :month (util/truncate-month date)
                                                     :day (util/truncate-day date)
                                                     nil date
                                                     date) doi domain]))
        ; doi -> period
        doi-period (f/map parsed-lines-period (f/fn [[date doi [subdomain domain tld]]]
                                            [doi date]))

                ; domain -> period
        domain-period (f/map parsed-lines-period (f/fn [[date doi [subdomain domain tld]]]
                                            [(str domain "." tld) date]))

        
        ; subdomain -> period
        subdomain-period (f/map parsed-lines-period (f/fn [[date doi [subdomain domain tld]]]
                                            [(str subdomain "." domain "." tld) date]))
        
                
        ; domain, period -> count
        domain-period-count (f/reduce-by-key
                              (f/map domain-period (f/fn [[domain period]] [[domain period] 1]))
                              (f/fn [a b] (+ a b)))
        
              
        ; domain, month -> count
        subdomain-period-count (f/reduce-by-key
                                (f/map subdomain-period (f/fn [[domain period]] [[domain period] 1]))
                                (f/fn [a b] (+ a b)))
        ]
    
    (.saveAsTextFile (f/map doi-period format-doi-value) (str output-location "/" (name period) "/doi"))
    (.saveAsTextFile (f/map domain-period format-doi-value) (str output-location "/" (name period) "/doi-value"))
    (.saveAsTextFile (f/map subdomain-period format-doi-value) (str output-location "/" (name period) "/subdomain"))
    (.saveAsTextFile (f/map domain-period-count format-doi-value) (str output-location "/" (name period) "/domain-count"))
    (.saveAsTextFile (f/map subdomain-period-count format-doi-value) (str output-location "/" (name period) "/subdomain-count"))
))


(defn -main
  [& args]
  (let [input-location hundred-input
        output-location tmp-output-dir]
    (generate-all-time sc input-location output-location)
    ; (generate-per-period sc :year input-location output-location)
    ; (generate-per-period sc :month input-location output-location)
    ; (generate-per-period sc :day input-location output-location)
    
    ))