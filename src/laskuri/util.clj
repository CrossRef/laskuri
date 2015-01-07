(ns laskuri.util
    ; (:import (org.joda.time.DateTimeZone))
    ; (:import (org.joda.time.TimeZone))
    (:import (java.net URL))
    (:import (java.text.SimpleDateFormat))
    (:require [clojure.java.io :as io])
    (:use [clojure.tools.logging :only (info error)])
    (:require [clj-time.core :as time])
    (:require [clj-time.coerce :as coerce])
    (:require [clj-time.format :as format])
    (:require [clj-time.format :refer [parse formatter]]))

(def ^String line-re #"^([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\") ([^\"]{1,2}|[^\"][^ ]*[^\"]|\"[^\"]*\")$")
(def log-date-formatter (format/formatter (time/default-time-zone) "EEE MMM dd HH:mm:ss zzz yyyy" "EEE MMM dd HH:mm:ss ZZZ yyyy"))

;; Helper functions.
(defn domain-parts
  "Split a domain into its parts. If the domain is malformed, an empty vector."
  [^String domain]
  (try
    (clojure.string/split domain #"\.")
  (catch Exception _ [])))

;; Reading the eTLD file.
(defn etld-entry?
  "Is this a valid line in the eTLD file?"
  [^String line]
  (or (empty? line) (.startsWith line "//")))

(defn get-effective-tld-structure 
  "Load the set of effective TLDs into a trie."
  []
  (with-open [reader (clojure.java.io/reader (clojure.java.io/resource "effective_tld_names.dat"))]
    (let [lines (line-seq reader)
          components (map domain-parts (remove etld-entry? lines))
          tree (reduce #(assoc-in %1 (reverse %2) {}) {} components)]
      (do tree))))

(def etlds (get-effective-tld-structure))

;; Extracting domain info.
(defn get-host 
  "Extract the host from a URL string. If the scheme is missing, try adding http."
   [^String url]
   (try 
     (when (> (count url) 3)
       ; (println url)
       (.getHost (new URL url)))
    (catch Exception _ (try 
     (when (> (count url) 3)
       ; (println url)
       (.getHost (new URL (str "http://" url))))
    (catch Exception _ nil)))))

(defn get-main-domain
  "Extract the main (effective top-level domain, 'main domain' and subdomains) from a domain name. 'www.xxx.test.com' -> ['www.xxx' 'test' 'com'] . Return reversed vector of components."
  [domain etld-structure]

  ; Recurse to find the prefix that doesn't comprise a recognised eTLD.
  (defn find-tld-suffix [input-parts tree-parts]
    (let [input-head (first input-parts)
          input-tail (rest input-parts)
          
          ; Find children that match.
          tree-children (get tree-parts input-head)
          
          ; Or children that are wildcards.
          tree-children-wildcard (get tree-parts "*")]
      
        ; First try an exact match.
        (if (not (nil? tree-children))
          ; If found, recurse.
          (find-tld-suffix input-tail tree-children)
          
          ; If there isn't an exact match, see if wildcards are allowed.
          (if (not (nil? tree-children-wildcard))
            (find-tld-suffix input-tail tree-children-wildcard)
            input-parts))))
  
  (let [parts (domain-parts domain)
        reverse-parts (reverse parts)
        parts-length (count parts)
        non-etld-parts (find-tld-suffix reverse-parts etld-structure)
        etld-parts (drop (count non-etld-parts) parts)
        main-domain (first non-etld-parts)
        subdomains (reverse (rest non-etld-parts))]
        [(apply str (interpose "." subdomains)) (or main-domain "") (apply str (interpose "." etld-parts))]))

(defn convert-special-uri
  "For special uris, convert them into an HTTP host proxy form."
  [^String uri]
  (cond 
    
    ; Prefixes.
    (. uri startsWith "app:/ReadCube.swf") "http://readcube.special"
    (. uri startsWith "http://t.co") "http://api.twitter.com"
    
    ; Extra weird things.
    
    ; muc=1;Domain=t.co;Expires=Fri, 14-Aug-2015 16:48:09 GMT
    (. uri contains "Domain=t.co") "http://api.twitter.com"
      
    ; When there's no referrer, record that as a special value.
    (= uri "") "http://no-referrer"
    
    :else uri))

;; Misc parsing util

(defn strip-quotes 
  "Strip quotes from the start and end of the line if they exist."
  [^String inp]
  (if (= (first inp) (last inp) \")
    (subs inp 1 (dec (count inp))) inp))

;; Log parsing

(defn parse-line 
  "Parse a line from the log, return vector of [date, doi, domain-triple]."
  [^String line]
    (let [match (re-find (re-matcher line-re line))]
        (when (seq match)
            ; match is [ip, ?, date, ?, ?, ?, doi, ?, referrer]
            (let [;^String ip (match 1)
                  date-str (strip-quotes (match 3))
                  doi (match 7)
                  status (strip-quotes (match 8))
                  referrer-url (convert-special-uri (strip-quotes (match 9)))
                  the-date (format/parse log-date-formatter date-str)
                  domain-triple (get-main-domain (get-host referrer-url) etlds)]
                    [the-date doi domain-triple status]))))

;; Date bits
(defn min-date [a b]
  (if (and a b)
      (if (time/before? a b)
        a b)
      (if a a b)))

(defn truncate-day [date]
  (time/date-time (time/year date) (time/month date) (time/day date)))

(defn truncate-month [date]
  (time/date-time (time/year date) (time/month date)))

(defn truncate-year [date]
  (time/date-time (time/year date)))

;; Domain setlist

; Mapping of [domain tld] -> random guid.
(def blacklist (atom {}))

; Mapping of [subdomain domain tld] -> random guid.
; Stored value represents subdomain only. Storing the domain in the key gives needed context.
(def blacklist-subdomains (atom {}))

(defn get-domain-whitelist 
  "Load the whitelist file"
  []
  (with-open [reader (clojure.java.io/reader (clojure.java.io/resource "domain-whitelist.txt"))]
    (let [lines (line-seq reader)
          whitelist (into #{} lines)]
      whitelist)))

(def domain-whitelist (get-domain-whitelist))

(defn redact-domain
  [[subdomain domain tld]]
  "Redact a [subdomain domain tld] triple and return in same format.
  Returns consistent mapping of domains and subdomains to random value."
  (if (domain-whitelist domain)
    [subdomain domain tld]
    (let [domain-key [domain tld]
          subdomain-key [subdomain domain tld]
          
          orig-subdomain-redacted (@blacklist-subdomains subdomain-key)
          orig-domain-redacted (@blacklist domain-key)
          
          subdomain-redacted (or orig-subdomain-redacted (.substring (.replace (.toString (java.util.UUID/randomUUID)) "-" "") 0 4))
          domain-redacted (or orig-domain-redacted (.substring (.replace (.toString (java.util.UUID/randomUUID)) "-" "") 0 12))]
      
      ; If we had to generate, store.
      (when-not orig-subdomain-redacted (swap! blacklist assoc domain-key domain-redacted))
      (when-not orig-domain-redacted (swap! blacklist-subdomains assoc subdomain-key domain-redacted))
      [subdomain-redacted domain-redacted "redacted"])))