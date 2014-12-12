(defproject laskuri "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [yieldbot/flambo "0.4.0-SNAPSHOT"]
                 [clj-time "0.8.0"]
                 [org.clojure/data.json "0.2.3"]
                 [org.clojure/tools.logging "0.2.6"]
                 [environ "1.0.0"]]
  
  :main ^:skip-aot laskuri.core
  :profiles {:provided
             {:aot :all
              :dependencies
              [[org.apache.spark/spark-core_2.10 "1.1.1"]]}
  
             :dev {:aot :all}
             :uberjar {:aot :all}}
  
  :target-path "target/%s"
  :jvm-opts ["-Duser.timezone=UTC" "-Xmx5G" "-Xms2G"]  ; "-Dsun.io.serialization.extendedDebugInfo=true"
  
  )
