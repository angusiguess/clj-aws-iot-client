(ns clj-aws-iot-client.keystore
  (:import [java.security KeyStore]
           [java.io FileInputStream]))

(defn get-keystore [keystore-file keystore-password]
  (let [keystore (KeyStore/getInstance
                  (KeyStore/getDefaultType))
        file-stream (FileInputStream. keystore-file)]
    (.load keystore file-stream (char-array keystore-password))
    keystore))
