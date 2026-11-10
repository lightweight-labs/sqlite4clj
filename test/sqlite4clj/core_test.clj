(ns sqlite4clj.core-test
  (:require
   [clojure.java.io :as io]
   [clojure.test :refer [deftest is testing use-fixtures]]
   [sqlite4clj.core :as d]
   [sqlite4clj.impl.api :as api]
   [sqlite4clj.test-common :refer [test-db test-db-path test-fixture with-db]]))

(use-fixtures :once test-fixture)

(deftest pool-objects-are-references
  (testing "Ensure pool objects are references to connections."
    (with-db [db (test-db)]
      (d/with-conn [conn (:writer db)]
        (is (identical? conn (first (:connections (db :writer)))))))))

(deftest improper-access-of-connections-or-prepared-statements
  (testing "Does not cause segfault when connections are accessed from
            separate threads."

    (with-db [db (test-db)]

      (d/q (:writer db)
        ["CREATE TABLE IF NOT EXISTS foo(id INT PRIMARY KEY, data BLOB)"])

      (d/with-write-tx [tx (:writer db)]
        (d/q tx ["INSERT INTO foo (data) VALUES (?)" 100000000])
        (d/q tx ["INSERT INTO foo (data) VALUES (?)" 100000000])
        (d/q tx ["INSERT INTO foo (data) VALUES (?)" 100000000])
        (d/q tx ["INSERT INTO foo (data) VALUES (?)" 100000000])
        (d/q tx ["INSERT INTO foo (data) VALUES (?)" 100000000])
        (d/q tx ["INSERT INTO foo (data) VALUES (?)" 100000000])
        (d/q tx ["INSERT INTO foo (data) VALUES (?)" 100000000])
        (future (d/q tx ["select sum(data) from foo"])
                (d/q tx ["select sum(data) from foo"])
                (d/q tx ["select sum(data) from foo"])
                (d/q tx ["select sum(data) from foo"])
                (d/q tx ["select sum(data) from foo"])
                (d/q tx ["select sum(data) from foo"]))
        (d/q tx ["select sum(data) from foo"])
        (d/q tx ["select sum(data) from foo"])
        (d/q tx ["select sum(data) from foo"])
        (d/q tx ["select sum(data) from foo"])
        (d/q tx ["select sum(data) from foo"])
        (d/q tx ["select sum(data) from foo"])))

    ;; Didn't crash
    (is (= true true))))

(deftest transactions-handle-sqlite-exceptions

  (testing "Write sransactions rollback when a sqlite error happens.
            This also makes sure the connection is returned."

    (with-db [db (test-db)]
      (d/q (:writer db)
        ["CREATE TABLE IF NOT EXISTS bar(id INT PRIMARY KEY, data BLOB)"])

      (try
        (d/with-write-tx [tx (:writer db)]
          (d/q tx ["select count(*) from bar"])
          (d/q tx ["INSERT INTO bar (id, data) VALUES (?, ?)" 1 (bigdec 10.0)])
          (d/q tx ["INSERT INTO bar (id, data) VALUES (?, ?)" 1 (bigdec 10.0)]))
        (catch Throwable _))
      (try
        (d/with-write-tx [tx (:writer db)]
          (d/q tx ["select count(*) from bar"])
          (d/q tx ["INSERT INTO bar (id, data) VALUES (?, ?)" 1 {:a 4}])
          (d/q tx ["INSERT INTO bar (id, data) VALUES (?, ?)" 1 {:a 4}]))
        (catch Throwable _))

      (is (= 0 (first (d/q (:reader db) ["select count(*) from bar"])))))))

(deftest transactions-handle-java-exceptions

  (testing "Write transactions rollback when a java excepions happens.
            This also makes sure the connection is returned."

    (with-db [db (test-db)]
      (d/q (:writer db)
        ["CREATE TABLE IF NOT EXISTS bar(id INT PRIMARY KEY, data BLOB)"])

      (try
        (d/with-write-tx [tx (:writer db)]
          (d/q tx ["select count(*) from bar"])
          (d/q tx ["INSERT INTO bar (id, data) VALUES (?, ?)" 1 (bigdec 10.0)])
          (throw (ex-info "non-sql-exception" {})))
        (catch Throwable _))
      (try
        (d/with-write-tx [tx (:writer db)]
          (d/q tx ["select count(*) from bar"])
          (d/q tx ["INSERT INTO bar (id, data) VALUES (?, ?)" 2 {:a 4}])
          (throw (ex-info "non-sql-exception" {})))
        (catch Throwable _))

      (is (= 0 (first (d/q (:reader db) ["select count(*) from bar"])))))))

(deftest encoding-edn

  (testing "Handles *print-length* being set in user land."

    (with-db [db (test-db)]
      (d/q (:writer db)
        ["CREATE TABLE IF NOT EXISTS encoding(id INT PRIMARY KEY, data BLOB)"])

      (binding [*print-length* 1]
        (d/q (:writer db)
          ["INSERT INTO encoding (id, data) VALUES (?, ?)" 1
           {:id 0, :email "bob@foobar.com", :username "bob"}]))

      (is (= [{:id 0, :email "bob@foobar.com", :username "bob"}]
             (d/q (:reader db)
               ["select data from encoding where id = 1"]))))))

(deftest encoding-raw-byte-arrays
  (testing "Raw byte arrays round-trip unchanged and keep the prefixed blob format."
    (with-db [db (test-db)]
      (d/q (:writer db)
        ["CREATE TABLE IF NOT EXISTS raw_bytes(id INT PRIMARY KEY, data BLOB)"])
      (let [payload (byte-array [1 2 3 4 5])]
        (d/q (:writer db)
          ["INSERT INTO raw_bytes (id, data) VALUES (?, ?)" 1 payload])
        (let [[stored-bytes] (d/q (:reader db)
                              ["select data from raw_bytes where id = 1"])
              [stored-length] (d/q (:reader db)
                               ["select length(data) from raw_bytes where id = 1"])]
          (is (= (seq payload) (seq stored-bytes)))
          (is (= (inc (alength payload)) stored-length)))))))

(deftest limits-can-be-set-at-db-init
  
  (testing "Attached limit prevents write connections from attaching."
    (with-db [db (test-db {:limits {:attached 0}})]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
            #"too many attached databases - max 0"
            (d/q (:writer db)
              ["ATTACH DATABASE 'test-data/test.db' AS other"])))))
  
  (testing "Attached limit prevents read connections from attaching."
    (with-db [db (test-db {:limits {:attached 0}})]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
            #"too many attached databases - max 0"
            (d/q (:reader db)
              ["ATTACH DATABASE 'test-data/test.db' AS other"]))))))

(defn- live-statements [{:keys [pdb]}]
  (loop [stmt (api/next-stmt pdb nil)
         n    0]
    (if stmt
      (recur (api/next-stmt pdb stmt) (inc n))
      n)))

(deftest the-statement-cache-finalizes-what-it-evicts
  (with-db [db (test-db {:pool-size 1})]
    (dotimes [n 1000]
      (d/q (:writer db) [(str "SELECT " n)]))
    (is (<= (live-statements (first (:connections (:writer db)))) 512))))

(deftest eviction-leaves-statements-in-use
  (testing "Queries run on the same connection while another's results are
            read can evict its statement without finalizing it."
    (with-db [db (test-db {:pool-size 1})]
      (d/with-conn [conn (:writer db)]
        (let [many-queries #(dotimes [n 600] (d/q conn [(str "SELECT " n)]))]
          (is (= [1 2 3]
                (d/q conn ["WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM n WHERE x < 3) SELECT x FROM n"]
                  {:result-set-fn (fn [_ rows]
                                    (many-queries)
                                    (into [] (comp cat (map (fn [x] (many-queries) x))) rows))}))))))))

(deftest closing-a-db-closes-its-connections
  (let [db (test-db)]
    (d/q (:writer db) ["CREATE TABLE IF NOT EXISTS closing (x)"])
    (d/q (:reader db) ["SELECT * FROM closing"])
    ((:close (:reader db)))
    ((:close (:writer db)))
    (testing "The last connection to close removes the write-ahead log."
      (is (not (.exists (io/file (str test-db-path "-wal"))))))
    (testing "Closing again does nothing."
      ((:close (:writer db))))
    (testing "A closed connection refuses queries."
      (is (thrown? clojure.lang.ExceptionInfo (d/q (:writer db) ["SELECT 1"]))))))
