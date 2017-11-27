(ns clj-aws-iot-client.core
  (:require [clj-aws-iot-client.keystore :as keystore]
            [clojure.string :as str]
            [clojure.set :as set])
  (:import [com.amazonaws.services.iot.client AWSIotMqttClient
            AWSIotConnectionStatus
            AWSIotQos
            AWSIotTopic
            AWSIotMessage]
           [com.amazonaws.services.iot.client.core AwsIotConnectionType]))

;; # Lightweight Bindings for the AWS IOT SDK
;; This library aims to maintain parity with the existing Java SDK with a few
;; small changes to cooperate better with Clojure idioms.
;;
;; ## Design Decisions:

;; ### Prefer Keywords to Enums
;; Enums aren't super fun to work with in clojure, so we remap them to keywords.
;; The naming convention is as follows: We downcase the enum name and replace any
;; instances of `_` by `-` to. For example, `QOS0` becomes `qos0` and `MQTT_OVER_TLS`
;; becomes `:mqtt-over-tls`.

;; ### Anything that mutates client state returns the client.
;; In the underlying Java library, most setters return `void`, which in Clojure
;; becomes `nil`. Since in Clojure we like to use the threading macro, any function
;; that returns void is wrapped in a `doto` to return the client.

;; ## Enum conversions

(defn- enum-maps
  "Given an enum generate two lookup tables, one from keyword to enum value,
   the other from enum value to keyword"
  [enum]
  (let [enums->kws (into {}
                         (map (fn [e] [e
                                       (-> e
                                           str
                                           str/lower-case
                                           (str/replace #"_" "-")
                                           keyword)])
                              (.getEnumConstants enum)))
        kws->enums (set/map-invert enums->kws)]
    [enums->kws kws->enums]))

(defmacro defenumlookups
  "A helper macro that builds lookup functions for enums.
  all values of `enum` are turned into a map of keywords to enum values.
  This macro provides two functions: one named `from-name->to-name`
  and one named `to-name->from-name`, for example:
  `(defenumlookups AWSIotQos kw qos)` creates
  `(defn- kw->qos [kw])` which takes a keyword and returns an enum value, and
  `(defn- qos->kw [e])` which takes an enum value and returns a keyword."
  [enum from-name to-name]
  `(let [[to-kws# from-kws#] (enum-maps ~enum)]
     (defn- ~(symbol (str from-name "->" to-name))
       ~(str "Given a keyword, looks up the corresponding enum value in " enum)
       ([kw#] (get from-kws# kw#)))
     (defn- ~(symbol (str to-name "->" from-name))
       ~(str "Given an enum value in " enum " lookup the corresponding keyword")
       ([e#] (get to-kws# e#)))))

(defenumlookups AWSIotQos kw qos)

(defenumlookups AWSIotConnectionStatus kw connection-status)

(defenumlookups AwsIotConnectionType kw iot-connection-type)

;; ## Publishing
;; We can publish messages as a blocking or non-blocking operation.
;; Blocking makes use of `publish-blocking`, which just take options.
;; Non-blocking publishing requires that we build a publisher, which
;; contains callback functions for handling message success, failure
;; and timeouts.

(defn publish-blocking
  "Given an MQTT client `client`, a string `topic`, and, optionally,
  a keyword `qos` and a number `timeout`. Always returns the client"
  ([client topic payload {:keys [qos timeout] :as opts}]
   (let [qos (kw->qos qos)]
     (cond (and qos timeout) (doto client (.publish topic qos payload timeout))
           qos (doto client (.publish topic qos payload))
           timeout (doto client (.publish topic payload timeout))
           :no-opts (doto client (.publish topic payload)))))
  ([client topic payload]
   (publish-blocking client topic payload nil)))

(defn publisher
  "Takes three zero-arity functions, `on-success`, `on-failure`,
  and `on-timeout`, and returns a function that takes a string `topic`,
  keyword `qos`, and a string or byte array `payload`, this function can
  in turn construct a message which can be passed to `publish-async`"
  [on-success on-failure on-timeout]
  (fn [topic qos payload]
    (let [qos (kw->qos qos)]
     (proxy [AWSIotMessage] [topic qos payload]
       (onSuccess [] (on-success))
       (onFailure [] (on-failure))
       (onTimeout [] (on-timeout))))))

(defn publish-async
  "Given a `client` and a `message`, which is constructed by a publisher,
   asynchronously publish that message"
  [client message]
  (doto client (.publish message)))

;; ## Last will message
;; Optionally, a message can be attached to the client which will send if the
;; client terminates ungracefully. Note that this message must be set before
;; the client connects.

(defn get-will-message [client]
  (.getWillMessage client))

(defn set-will-message [client message]
  (doto client (.setWillMessage message)))


;; ## Subscribing

(defn subscription
  "Given a `topic` string, `qos` keyword, and
  a 1-arity function that takes a message, returns a
  subscription, which can be added to a client"
  [topic qos on-message]
  (let [qos (kw->qos qos)]
    (proxy [AWSIotTopic] [topic qos]
      (onMessage [message] (on-message message)))))

(defn subscribe
  "Applies a subscription to a client, firing the subscription's `on-message`
  whenever a message is received to `topic`"
  [client subscription]
  (doto client (.subscribe client subscription)))

;; # Client getters


(defn get-connection-status [client]
  (let [connection-status (.getConnectionStatus client)]
    (connection-status->kw connection-status)))

(defn get-connection-type [client]
  (let [connection-type (.getConnectionType client)]
    (iot-connection-type->kw connection-type)))

(defn get-base-retry-delay [client]
  (.getBaseRetryDelay client))

(defn get-connection-timeout [client]
  (.getConnectionTimeout client))

(defn get-keep-alive-interval [client]
  (.getKeepAliveInterval client))

(defn get-max-connection-retries [client]
  (.getMaxConnectionRetries client))

(defn get-max-offline-queue-size [client]
  (.getMaxOfflineQueueSize client))

(defn get-max-retry-delay [client]
  (.getMaxRetryDelay client))

(defn get-num-of-client-threads [client]
  (.getNumOfClientThreads client))

(defn get-server-ack-timeout [client]
  (.getServerAckTimeout client))

(defn get-client-endpoint [client]
  (.getClientEndpoint client))

(defn get-client-id [client]
  (.getClientId client))

(defn get-connection [client]
  (.getConnection client))

(defn get-devices [client]
  (.getDevices client))

(defn get-execution-service [client]
  (.getExecutionService client))

(defn get-subscriptions [client]
  (.getSubscriptions client))

(defn schedule-routine-task [client f initial-delay period]
  (doto client (.scheduleRoutineTask f initial-delay period)))

(defn schedule-timeout-task [client f timeout]
  (doto client
    (.scheduleTimeoutTask f timeout)))

;; # The client
;; Most functions in this library operate directly on the client. The client
;; can be authenticated in one of th

(defn mqtt-client
  "Returns an MQTT client, which handles connection to an MQTT endpoint
  authentication method can be either :tls or :wss
  :tls requires :client-endpoint, :client-id, :keystore, and :key-password
  :wss requires :client-id, :aws-access-key-id, and optionally :session-token"
  [authentication-method
   {:keys [client-endpoint
           client-id
           keystore
           aws-access-key-id
           aws-secret-access-key
           session-token
           key-password] :as config}]
  (cond (and (= :wss authentication-method)
             session-token)
        (AWSIotMqttClient. client-endpoint
                           client-id
                           aws-access-key-id
                           aws-secret-access-key
                           session-token)
        (= :wss authentication-method)
        (AWSIotMqttClient. client-endpoint
                           client-id
                           aws-access-key-id
                           aws-secret-access-key)
        (= :tls authentication-method)
        (AWSIotMqttClient. client-endpoint
                           client-id
                           keystore
                           key-password)))
