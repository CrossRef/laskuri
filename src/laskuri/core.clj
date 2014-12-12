(ns laskuri.core
  (:require [flambo.conf :as conf])
  (:require [flambo.api :as f])
  (:require [laskuri.util :as util])
  (:require [clj-time.core :as t])
  (:require [clojure.string :as string])
  (:require [clojure.java.io :as io])
  (:use [clojure.tools.logging :only (info error)])
  (:require [environ.core :refer [env]])
  (:gen-class main true)) ;:gen-class :)


(defn format-kv
  "Format a Key, Value line into a tab separated string."
  [[k v]]
  (format "%s\t%s" k (str v)))

(defn format-ksv
  "Format a Key, Value line, where K is a vector, into a tab separated string."
  [[ks v]]
  (format "%s\t%s" (string/join "\t" ks) (str v)))

(defn get-parsed-lines [ctx location redact?]
  "Get a new input stream of parsed lines."
  (let [logfiles (f/text-file ctx location)
        parsed-lines (f/map logfiles (f/fn [s] (util/parse-line s)))
        parsed-lines (f/filter parsed-lines (f/fn [line] (not (nil? line))))]
    (if redact?
      (f/map parsed-lines (f/fn [[date doi domain]] [date doi (util/redact-domain domain)]))
      parsed-lines)))

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
  [ctx input-location output-location redact]
  (let [parsed-lines (get-parsed-lines ctx input-location redact)
        
        ; doi -> date
        doi-date (f/map parsed-lines (f/fn [[date doi [subdomain domain tld]]]
                                            [doi date]))
        
        ; domain -> date
        domain-date (f/map parsed-lines (f/fn [[date doi [subdomain domain tld]]]
                                              [(str domain "." tld) date]))

        ; [domain with subdomain, domain] -> doi
        subdomain-doi (f/map parsed-lines (f/fn [[date doi [subdomain domain tld]]]
                                                [[(str subdomain "." domain "." tld) domain] doi]))
        
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
        
        ; domain and subdomain, domain -> count
        ; including both subdomain and domain is necessary for the consumer of this dataset.
        subdomain-count (count-by-key subdomain-doi)]
      
  
    (.saveAsTextFile (f/map doi-first-date format-kv) (str output-location "/ever-doi-first-date"))
    (.saveAsTextFile (f/map doi-count format-kv) (str output-location "/ever-doi-count"))
    (.saveAsTextFile (f/map domain-count format-kv) (str output-location "/ever-domain-count"))
    (.saveAsTextFile (f/map subdomain-count format-ksv) (str output-location "/ever-subdomain-count"))))

(defn generate-per-period
  "Generate figures per-day."
  [ctx period input-location output-location redact]
  {:pre [(#{:year :month :day nil} period)]}
  (let [parsed-lines (get-parsed-lines ctx input-location redact)

        ; date truncated to period
        ; date represents the beginning of the period (i.e. first second of the day, month or year).
        parsed-lines-period (f/map parsed-lines (f/fn [[date doi domain]]
                                                    [(condp = period
                                                     :year (util/truncate-year date)
                                                     :month (util/truncate-month date)
                                                     :day (util/truncate-day date)
                                                     nil date
                                                     date) doi domain]))
        
        ; For the following, the period is included in the key because we're counting unique 'X per period'
        ; (e.g. '10.5555/12345678 per month').
        
        ; [doi period] -> date
        doi-period-date (f/map parsed-lines-period (f/fn [[date doi [subdomain domain tld]]]
                                            [[doi date] date]))

        ; [domain period] -> date
        domain-period-date (f/map parsed-lines-period (f/fn [[date doi [subdomain domain tld]]]
                                            [[(str domain "." tld) date] date]))

        
        ; [subdomain domain period] -> date
        subdomain-period-date (f/map parsed-lines-period (f/fn [[date doi [subdomain domain tld]]]
                                            [[(str subdomain "." domain "." tld) domain date] date]))

        ;; outputs
        
        ; doi -> count per period
        doi-period-count (count-by-key doi-period-date)
        
        ; domain -> count per period
        domain-period-count (count-by-key domain-period-date)
        
        ; subdomain -> count per period
        subdomain-period-count (count-by-key subdomain-period-date)]
    
    (.saveAsTextFile (f/map doi-period-count format-ksv) (str output-location "/" (name period) "-doi-period-count"))
    (.saveAsTextFile (f/map domain-period-count format-ksv) (str output-location "/" (name period) "-domain-period-count"))
    (.saveAsTextFile (f/map subdomain-period-count format-ksv) (str output-location "/" (name period) "-subdomain-period-count"))))


(defn -main
  [& args]
  (let [input-location (env :input-location)
        output-location (env :output-location)
        redact (= (.toLowerCase (or (env :redact) "false")) "true")
        dev-local (env :dev-local)
        
        ; If local, use this config. Otherwise empty, will be loaded from `spark-submit`. 
        conf (if dev-local
                (-> (conf/spark-conf)
                   (conf/master "local")
                   (conf/app-name "laskuri")
                   (conf/set "spark.driver.memory" "500m")
                   (conf/set "spark.executor.memory" "500m")
                   (conf/set "spark.kryoserializer.buffer.mb" "256"))
                (conf/spark-conf))
          sc (f/spark-context conf)]  
    (when (and input-location output-location)
      (info "Input" input-location)
      (info "Output" output-location)      
      (generate-all-time sc input-location output-location redact)
      (generate-per-period sc :year input-location output-location redact)
      (generate-per-period sc :month input-location output-location redact)
      (generate-per-period sc :day input-location output-location redact))
))