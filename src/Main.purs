module Main where

import Prelude
import Control.Monad.Aff
import Control.Monad.Eff
import Control.Monad.Eff.Class
import Control.Monad.Eff.Console (log, CONSOLE())
import Control.Monad.Eff.Exception
import Control.Monad.Error.Class
import Control.Monad.ST
import Data.Array
import Data.Date
import Data.Either
import Data.Foldable
import Data.Maybe
import Data.Monoid
import Data.Options
import Data.StrMap as StrMap
import Data.String (split, joinWith)
import Data.Foreign
import Data.Foreign.Class
import Data.Foreign.Index
import Data.Foreign.Keys
import Node.Encoding
import Node.HTTP
import Node.HTTP.Client as HTTP
import Node.Process
import Node.Stream

foreign import toISOString :: JSDate -> String

toISOString' :: Date -> String
toISOString' = toJSDate >>> toISOString

foreign import jsonStringify :: forall a. a -> String

affHttpRequestBody :: forall eff. Options HTTP.RequestOptions -> String -> Aff (http :: HTTP | eff) HTTP.Response
affHttpRequestBody requestOptions body =
    makeAff \_ success ->
        do
            req <- HTTP.request requestOptions success
            let requestStream = HTTP.requestAsStream req
            writeString requestStream UTF8 body $ pure unit
            end requestStream $ pure unit

affHttpRequest :: forall eff. Options HTTP.RequestOptions -> Aff(http :: HTTP | eff) HTTP.Response
affHttpRequest requestOptions =
    makeAff \_ success ->
        do
            req <- HTTP.request requestOptions success
            let requestStream = HTTP.requestAsStream req
            end requestStream $ pure unit

affHttpRequestFromURI :: forall eff. String -> Aff(http :: HTTP | eff) HTTP.Response
affHttpRequestFromURI uri =
    makeAff \_ success ->
        do
            req <- HTTP.requestFromURI uri success
            let requestStream = HTTP.requestAsStream req
            end requestStream $ pure unit

readStreamToString :: forall w s eff. Readable w (err :: EXCEPTION, st :: ST s | eff) -> Encoding -> Aff (err :: EXCEPTION, st :: ST s | eff) String
readStreamToString stream encoding =
    makeAff \_ success -> do
        buf <- newSTRef ""
        onDataString stream encoding \new ->
            do
                modifySTRef buf (\old -> old ++ new)
                return unit
        onEnd stream do
            body <- readSTRef buf
            success body

type MyResponse a =
    { response :: HTTP.Response
    , body :: a
    }

readResponseBody :: forall s eff. HTTP.Response -> Encoding -> Aff (http :: HTTP, err :: EXCEPTION, st :: ST s | eff) (MyResponse String)
readResponseBody response encoding =
    let responseStream = HTTP.responseAsStream response in
        do
            responseBody <- readStreamToString responseStream encoding
            pure { response: response, body: responseBody }

readResponseBodyJSON :: forall t s eff. (IsForeign t) => HTTP.Response -> Encoding -> Aff (http :: HTTP, err :: EXCEPTION, st :: ST s | eff) (MyResponse (F t))
readResponseBodyJSON =
    readResponseBody >>> \b ->
        b >>> flip (>>=) \resp ->
            let newBody = readJSON resp.body in
                pure { response: resp.response, body: newBody }

getLeadingMaster :: forall eff. String -> Aff (http :: HTTP | eff) String
getLeadingMaster initialMaster = do
    redirectResponse <- affHttpRequestFromURI $ initialMaster ++ "/master/redirect"
    leadingMaster <- case StrMap.lookup "location" $ HTTP.responseHeaders redirectResponse of
                        Just l -> pure l
                        Nothing -> throwError $ error "Couldn't get the leading master"
    return leadingMaster

getMetrics :: forall eff s. String -> Aff (err :: EXCEPTION, http :: HTTP, st :: ST s | eff) (F Foreign)
getMetrics baseURL = do
    metricsResponse <- affHttpRequestFromURI $ baseURL ++ "/metrics/snapshot"
    metricsF :: MyResponse (F Foreign) <- readResponseBodyJSON metricsResponse UTF8
    return metricsF.body

getConfigDefault :: forall eff. String -> String -> Eff (err :: EXCEPTION, process :: PROCESS | eff) String
getConfigDefault key defaultValue = (lookupEnv key) >>= ((fromMaybe defaultValue) >>> pure)

mesosToAionMetricName :: String -> String
mesosToAionMetricName mesosMetricName =
    let metricNamePath = split "/" mesosMetricName in
        joinWith "-" metricNamePath

type AionMetric =
    { name :: String
    , value :: Number
    }

getAionMetric :: String -> Foreign -> F (Array AionMetric)
getAionMetric mesosMetricName mesosMetrics =
    let aionMetricName = mesosToAionMetricName mesosMetricName in
        do
            fvalue <- prop mesosMetricName mesosMetrics
            value <- readNumber fvalue
            return $ singleton { name: aionMetricName, value: value }

writeAionMetric :: forall eff. String -> AionMetric -> Aff (now :: Now, http :: HTTP, console :: CONSOLE | eff) Unit
writeAionMetric baseURL aionMetric =
    do
        t <- liftEff now
        let tStr = toISOString' t
        let fullMetric =
                { metric: aionMetric.name
                , value: aionMetric.value
                , time: tStr
                }
        let content = jsonStringify fullMetric
        let requestOptions = (HTTP.protocol := "http:") <> (HTTP.hostname := baseURL) <> (HTTP.method := "POST") <> (HTTP.path := "/mesos_metrics") <> (HTTP.port := 31013)
        writeResponse <- affHttpRequestBody requestOptions content
        liftEff $ log $ show $ HTTP.statusCode writeResponse


main :: forall e s. Eff (now :: Now, st :: ST s, http :: HTTP, process :: PROCESS, err :: EXCEPTION, console :: CONSOLE | e) Unit
main = do
    masterBaseURL <- getConfigDefault "MASTER" "http://masters.mesos.truviewlive.com:5050"
    aionBaseURL <- getConfigDefault "AION_HOST" "169.254.255.254"
    launchAff do
        leadingMaster <- getLeadingMaster masterBaseURL
        let leadingMasterBaseURL = "http:" ++ leadingMaster
        metricsF <- getMetrics leadingMasterBaseURL
        let aionMetricsMap =
                do
                    metrics <- metricsF
                    mesosMetricsKeys <- keys metrics
                    let aionMetrics = (flip getAionMetric metrics) <$> mesosMetricsKeys
                    foldl (<>) (Right mempty) aionMetrics
        aionMetrics <-
                case aionMetricsMap of
                  Left _ -> throwError $ error "Error parsing metrics from mesos"
                  Right ms -> pure ms
        let aionMetricsLogs = (writeAionMetric aionBaseURL) <$> aionMetrics
        foldl (<>) mempty aionMetricsLogs
