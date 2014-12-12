(ns laskuri.core
  (:require [flambo.conf :as conf])
  (:require [flambo.api :as f]))

(def sc (f/spark-context (-> (conf/spark-conf)
           (conf/master "local")
           (conf/app-name "test")
           (conf/set "spark.driver.memory" "1g")
           (conf/set "spark.executor.memory" "1g")
           (conf/set "spark.local.dir" "/tmp/t")
           
            ; (conf/set "spark.kryoserializer.buffer.mb" "256")
            )))

(defn get-inputs []
  (f/text-file sc "file:///Users/joe/data/doi-logs-small/access_log_201210_cr5-one")
  )

(defn -main
  
  [& args]
  (let [logfile (get-inputs)
        a (f/map logfile (f/fn [u] nil))
        b (f/map logfile (f/fn [u] nil))
        c (f/map logfile (f/fn [u] nil))
        d (f/map logfile (f/fn [u] nil))
        e (f/map logfile (f/fn [u] nil))
        g (f/map logfile (f/fn [u] nil))
        h (f/map logfile (f/fn [u] nil))
        i (f/map logfile (f/fn [u] nil))
        ])
  
  (let [logfile (get-inputs)
        j (f/map logfile (f/fn [u] nil))
        k (f/map logfile (f/fn [u] nil))
        l (f/map logfile (f/fn [u] nil))
        m (f/map logfile (f/fn [u] nil))
        n (f/map logfile (f/fn [u] nil))  
        o (f/map logfile (f/fn [u] nil))
        p (f/map logfile (f/fn [u] nil))
        q (f/map logfile (f/fn [u] nil))
        r (f/map logfile (f/fn [u] nil))
        s (f/map logfile (f/fn [u] nil))
        t (f/map logfile (f/fn [u] nil))
]))
  
   