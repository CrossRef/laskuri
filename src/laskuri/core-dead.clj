(ns laskuri.core
  (:require [flambo.conf :as conf])
  (:require [flambo.api :as f])
  (:require [clj-time.core :as t])
  (:require [laskuri.util :as util])
  
  (:require [clojure.java.io :as io])
  (:use [clojure.tools.logging :only (info error)])
  (:gen-class))

(def c (-> (conf/spark-conf)
           (conf/master "local[*]")
           (conf/app-name "laskuri")
           (conf/set "spark.driver.memory" "5g")
           (conf/set "spark.executor.memory" "5g")
           (conf/set "spark.kryoserializer.buffer.mb" "512")
           ))

(def sc (f/spark-context c))

(defn domain-parts
  "Split a domain into its parts. If the domain is malformed, an empty vector."
  [^String domain]
  (try
    (clojure.string/split domain #"\.")
  (catch Exception _ [])))

(def one-input "file:///Users/joe/data/doi-logs-small/access_log_201210_cr5-one")


(defn -main
  [& args]
  
  (let [
        parsed-lines (f/parallelize sc [[(t/now) "abc" "x.y.z"]])
        parsed-lines (f/filter parsed-lines (f/fn [line] (not (nil? line))))
        
        ; ; parsed-lines with date truncated to year 
        parsed-lines-year parsed-lines
        ; parsed-lines with date truncated t month
        parsed-lines-month parsed-lines

        ; parsed-lines with date truncated t day
        parsed-lines-day parsed-lines
        
        ;; Date transformation
        
        ; doi -> date
        doi-date (f/map parsed-lines (f/fn [[date doi [subdomain domain tld]]]
                                      [doi date]))

        ; doi -> year
        doi-year (f/map parsed-lines-year (f/fn [[date doi [subdomain domain tld]]]
                                            [doi date]))

        ; doi -> month
        doi-month (f/map parsed-lines-month (f/fn [[date doi [subdomain domain tld]]]
                                            [doi date]))
        
        ; doi -> day
        doi-day (f/map parsed-lines-day (f/fn [[date doi [subdomain domain tld]]]
                                          [doi date]))
        
        ; domain -> date
        domain-date (f/map parsed-lines (f/fn [[date doi [subdomain domain tld]]]
                                      [(str domain "." tld) date]))

        ; domain -> year
        domain-year (f/map parsed-lines-year (f/fn [[date doi [subdomain domain tld]]]
                                            [(str domain "." tld) date]))

        ; domain -> month
        domain-month (f/map parsed-lines-month (f/fn [[date doi [subdomain domain tld]]]
                                            [(str domain "." tld) date]))
        
        ; domain -> day
        domain-day (f/map parsed-lines-day (f/fn [[date doi [subdomain domain tld]]]
                                          [(str domain "." tld) date]))
               
        ; subdomain -> year
        subdomain-year (f/map parsed-lines-year (f/fn [[date doi [subdomain domain tld]]]
                                            [(str subdomain "." domain "." tld) date]))

        ; subdomain -> month
        subdomain-month (f/map parsed-lines-month (f/fn [[date doi [subdomain domain tld]]]
                                            [(str subdomain "." domain "." tld) date]))        
        ; subdomain -> day
        subdomain-day (f/map parsed-lines-day (f/fn [[date doi [subdomain domain tld]]]
                                            [(str subdomain "." domain "." tld) date]))
        
        ;; Events for all-time
                
        ; doi -> first date visited
        doi-first-date (f/reduce-by-key doi-date (f/fn [a b] (util/min-date a b)))
        
        ;; Totals for all time
        
        ; doi -> total visits
        doi-total-visits (f/reduce-by-key
                          (f/map doi-date (f/fn [[doi date]] [doi 1]))
                          (f/fn [a b] (+ a b)))
        
        ; domain with subdomain -> doi
        subdomain-doi (f/map parsed-lines (f/fn [[date doi [subdomain domain tld]]]
                                        [(str subdomain "." domain "." tld) doi]))
        
        ; domain -> doi
        domain-doi (f/map parsed-lines (f/fn [[date doi [subdomain domain tld]]]
                                       [(str domain "." tld) doi]))]
    
          
    (.saveAsTextFile parsed-lines-year "/tmp/o/parsed-lines-year")
    (.saveAsTextFile parsed-lines-month "/tmp/o/parsed-lines-month")
    (.saveAsTextFile parsed-lines-day "/tmp/o/parsed-lines-day")
    (.saveAsTextFile domain-doi "/tmp/o/domain-doi")
    (.saveAsTextFile subdomain-doi "/tmp/o/subdomain-doi")
    (.saveAsTextFile domain-date "/tmp/o/domain-date")
    (.saveAsTextFile domain-year "/tmp/o/domain-year")
    (.saveAsTextFile domain-month "/tmp/o/domain-month")
    (.saveAsTextFile domain-day "/tmp/o/domain-day")))
  