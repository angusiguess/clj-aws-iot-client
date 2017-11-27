(ns clj-aws-iot-client.core-test
  (:require [clojure.test :refer :all]
            [clj-aws-iot-client.core :refer :all])
  (:import [java.security KeyStore]))

(deftest client-instantiation
  (testing "wss with no token"
    (let [config {:client-endpoint "an-endpoint.iot.us-east-1.amazonaws.com"
                  :client-id "new client who this"
                  :aws-access-key-id "PROBABLYANAMAZONKEY"
                  :aws-secret-access-key "ProBably4secretKey"}
          client (mqtt-client :wss config)]
      (let [{:keys [clientId
                    clientEndpoint]} (bean client)]
        (is (= clientId (:client-id config)))
        (is (= clientEndpoint (:client-endpoint config))))))
  (testing "wss with token"
    (let [config {:client-endpoint "an-endpoint.iot.us-east-1.amazonaws.com"
                  :client-id "new client who this"
                  :aws-access-key-id "PROBABLYANAMAZONKEY"
                  :aws-secret-access-key "ProBably4secretKey"
                  :session-token "asessiontoken"}
          client (mqtt-client :wss config)]
      (let [{:keys [clientId
                    clientEndpoint]} (bean client)]
        (is (= clientId (:client-id config)))
        (is (= clientEndpoint (:client-endpoint config)))))))
