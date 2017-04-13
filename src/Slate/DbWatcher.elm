module Slate.DbWatcher
    exposing
        ( Msg
        , Model
        , Config
        , init
        , start
        , stop
        , subscribe
        , unsubscribe
        , update
        , elmSubscriptions
        )

{-| Slate Database Watcher.

@docs Msg , Model , Config , init , start , stop , subscribe , unsubscribe , update , elmSubscriptions
-}

import Dict exposing (Dict)
import Time exposing (Time)
import Process
import Task exposing (Task)
import Json.Decode as JD exposing (..)
import Postgres exposing (..)
import StringUtils exposing (..)
import Retry exposing (..)
import Utils.Ops exposing (..)
import Utils.Error exposing (..)
import Utils.Log exposing (..)
import Utils.Json exposing ((<||))
import ParentChildUpdate exposing (..)
import Slate.Common.Db exposing (..)
import Slate.Common.Event exposing (..)
import Slate.Common.Entity exposing (..)
import Slate.DbWatcher.Common.Interface exposing (..)


type alias WatchedEntitiesDict comparable =
    Dict comparable (List EntityEventTypes)


type alias ListenPayload =
    { event : Event
    }


type alias Event =
    { entityName : String
    , target : String
    , operation : String
    , propertyName : Maybe String
    }


type ConnectCause
    = StartingCause
    | ReconnectCause


type DisconnectCause
    = StoppingCause
    | StopAfterConnectCause


channelName : String
channelName =
    "eventsinsert"


{-| Config
-}
type alias Config comparable msg =
    { pgReconnectDelayInterval : Time
    , stopDelayInterval : Time
    , validateId : comparable -> Bool
    , clientInterface : ClientInterface comparable Msg msg
    }


retryConfig : Retry.Config Msg
retryConfig =
    { retryMax = 3
    , delayNext = Retry.constantDelay 5000
    , routeToMeTagger = RetryModule
    }


{-| Msg
-}
type Msg
    = Nop
    | Stopped
    | DoCmd (Cmd Msg)
    | PGConnect ConnectCause Int
    | PGConnectError ConnectCause ( Int, String )
    | PGConnectionLost ( Int, String )
    | PGDisconnectError DisconnectCause ( Int, String )
    | PGDisconnect DisconnectCause Int
    | PGListenSuccess ( Int, String, ListenUnlisten )
    | PGListenError ( Int, String )
    | PGListenEvent ( Int, String, String )
    | RetryConnectCmd Int Msg (Cmd Msg)
    | RetryModule (Retry.Msg Msg)


{-| Model
-}
type alias Model comparable =
    { watchedEntities : WatchedEntitiesDict comparable
    , maybeDbConnectionInfo : Maybe DbConnectionInfo
    , running : Bool
    , pgListenConnectionId : Maybe Int
    , pgListenError : Bool
    , retryModel : Retry.Model Msg
    }


{-| init DbWatcher
-}
init : Config comparable msg -> ( Model comparable, Cmd msg )
init config =
    ({ watchedEntities = Dict.empty
     , maybeDbConnectionInfo = Nothing
     , running = False
     , pgListenConnectionId = Nothing
     , pgListenError = False
     , retryModel = Retry.initModel
     }
        ! []
    )



{-
   API
-}


{-| start DbWatcher
-}
start : Config comparable msg -> DbConnectionInfo -> Model comparable -> Result String ( Model comparable, Cmd msg )
start config dbConnectionInfo model =
    model.running
        ? ( Err "Already Started"
          , Ok
                (let
                    ( retryModel, cmd ) =
                        pgConnect config dbConnectionInfo model StartingCause
                 in
                    ( { model | running = True, watchedEntities = Dict.empty, retryModel = retryModel, maybeDbConnectionInfo = Just dbConnectionInfo }, Cmd.map config.clientInterface.routeToMeTagger cmd )
                )
          )


{-| stop DbWatcher
-}
stop : Config comparable msg -> Model comparable -> Result String ( Model comparable, Cmd msg )
stop config model =
    let
        cmd =
            let
                disconnectOrStop stopDelay =
                    Cmd.map config.clientInterface.routeToMeTagger <|
                        model.pgListenConnectionId
                            |?> (\pgConnectionId -> delayCmd (pgDisconnect model.pgListenConnectionId StoppingCause) stopDelay)
                            ?= delayUpdateMsg stopDelay Stopped

                logError errorType error =
                    config.clientInterface.errorTagger ( errorType, error )
                        |> delayUpdateMsg 0
            in
                model.running
                    ? ( disconnectOrStop config.stopDelayInterval
                      , (let
                            logCmd =
                                model.pgListenConnectionId
                                    |?> (\connectionId -> logError NonFatalError ("BUG -- Postgres connectionId:" +-+ connectionId +-+ " still exists when not running"))
                                    ?= Cmd.none
                         in
                            Cmd.batch [ disconnectOrStop 0, logCmd ]
                        )
                      )
    in
        Ok ({ model | running = False, pgListenConnectionId = Nothing, pgListenError = False, maybeDbConnectionInfo = Nothing } ! [ cmd ])


{-| subscribe to DbWatcher
-}
subscribe : Config comparable msg -> Model comparable -> List EntityEventTypes -> comparable -> SubscribeErrorTagger comparable msg -> Result (List String) ( Model comparable, Cmd msg )
subscribe config model entityEventTypesList comparable subscribeErrorTagger =
    createSubscriber config entityEventTypesList model comparable subscribeErrorTagger


{-| unsubscribe to DbWatcher
-}
unsubscribe : Config comparable msg -> Model comparable -> comparable -> UnsubscribeErrorTagger comparable msg -> Result (List String) ( Model comparable, Cmd msg )
unsubscribe config model comparable unsubscribeErrorTagger =
    destroySubscriber model comparable unsubscribeErrorTagger


{-| update
-}
update : Config comparable msg -> Msg -> Model comparable -> ( ( Model comparable, Cmd Msg ), List msg )
update config msg model =
    let
        logInfo message =
            config.clientInterface.logTagger ( LogLevelInfo, message )

        nonFatal error =
            config.clientInterface.errorTagger ( NonFatalError, error )

        dbConnectionInfo model =
            model.maybeDbConnectionInfo ?!= (\_ -> Debug.crash "BUG: model.maybeDbConnectionInfo cannot be Nothing")

        updateRetry =
            updateChildParent (Retry.update retryConfig) (update config) .retryModel retryConfig.routeToMeTagger (\model retryModel -> { model | retryModel = retryModel })
    in
        case msg of
            Nop ->
                ( model ! [], [] )

            DoCmd cmd ->
                ( model ! [ cmd ], [] )

            Stopped ->
                let
                    newModel =
                        { model | watchedEntities = Dict.empty }
                in
                    ( newModel ! [], logInfo ("DbWatcher Stopped.  Model:" +-+ newModel) :: [ config.clientInterface.stoppedMsg ] )

            PGConnect cause pgConnectionId ->
                let
                    ( newModel, cmd, appMsgs ) =
                        case cause of
                            StartingCause ->
                                ( { model | pgListenConnectionId = Just pgConnectionId, pgListenError = False }, Cmd.none, [ config.clientInterface.startedMsg ] )

                            ReconnectCause ->
                                model.running
                                    ? ( ( { model | pgListenConnectionId = Just pgConnectionId, pgListenError = False }, Cmd.none, [] )
                                      , ( model, (pgDisconnect model.pgListenConnectionId StopAfterConnectCause), [] )
                                      )

                    finalAppMsgs =
                        logInfo ("PGConnect:" +-+ pgConnectionId +-+ "Cause:" +-+ cause) :: appMsgs
                in
                    ( newModel ! [ cmd ], finalAppMsgs )

            PGConnectError cause ( _, pgError ) ->
                let
                    error =
                        "Cannot connect to Database after" +-+ retryConfig.retryMax +-+ "attempt(s)." +-+ pgError +-+ "Cause:" +-+ cause

                    ( newRetryModel, cmd ) =
                        model.running
                            ? ( let
                                    ( retryModel, cmd ) =
                                        pgConnect config (dbConnectionInfo model) model cause
                                in
                                    ( retryModel, delayCmd cmd config.pgReconnectDelayInterval )
                              , ( model.retryModel, Cmd.none )
                              )
                in
                    ( { model | pgListenConnectionId = Nothing, retryModel = newRetryModel } ! [ cmd ], [ nonFatal error ] )

            PGConnectionLost ( pgConnectionId, pgError ) ->
                let
                    error =
                        "PGConnectionLost:" +-+ ( pgConnectionId, pgError ) +-+ "Running:" +-+ model.running

                    ( retryModel, cmd ) =
                        model.running
                            ? ( pgConnect config (dbConnectionInfo model) model ReconnectCause
                              , ( model.retryModel, Cmd.none )
                              )
                in
                    ( { model | pgListenConnectionId = Nothing, retryModel = retryModel } ! [ cmd ], [ nonFatal error ] )

            PGDisconnectError cause ( pgConnectionId, pgError ) ->
                let
                    error =
                        "PGDisconnectError:" +-+ ( pgConnectionId, pgError ) +-+ "Cause:" +-+ cause
                in
                    case cause of
                        StoppingCause ->
                            update config Stopped { model | pgListenConnectionId = Nothing }

                        StopAfterConnectCause ->
                            ( model ! [], [ nonFatal error ] )

            PGDisconnect cause pgConnectionId ->
                let
                    message =
                        "PGDisconnect:" +-+ pgConnectionId +-+ "Cause:" +-+ cause
                in
                    case cause of
                        StoppingCause ->
                            update config Stopped { model | pgListenConnectionId = Nothing }

                        StopAfterConnectCause ->
                            ( model ! [], [ logInfo message ] )

            PGListenSuccess ( connectionId, channelName, listenUnlisten ) ->
                ( model ! [], [ logInfo <| "PGListenSuccess:" +-+ ( connectionId, channelName, listenUnlisten ) ] )

            PGListenError ( connectionId, pgError ) ->
                ( { model | pgListenError = True } ! [], [ nonFatal <| "PGListenError:" +-+ ( connectionId, pgError ) ] )

            PGListenEvent ( connectionId, channelName, pgMessage ) ->
                let
                    listenPayloadDecodedResult =
                        JD.decodeString listenPayloadDecoder pgMessage

                    ( entityName, refreshList, error ) =
                        model.running
                            ? ( listenPayloadDecodedResult
                                    |??>
                                        (\payload ->
                                            ( payload.event.entityName
                                            , createRefreshList config payload.event.entityName payload.event.target payload.event.operation payload.event.propertyName model.watchedEntities
                                            , ""
                                            )
                                        )
                                    ??= (\err -> ( "", [], "Listen payload event.entityName not found:" +-+ err ))
                              , listenPayloadDecodedResult
                                    |??>
                                        (\payload ->
                                            ( "", [], "DbWatcher not running, listen event" +-+ payload.event.entityName +-+ "not processed" )
                                        )
                                    ??= (\err ->
                                            ( "", [], "DbWatcher not running, listen event not processed.  Listen payload event.entityName not found:" +-+ err )
                                        )
                              )

                    msgs =
                        (String.length error > 0)
                            ? ( [ nonFatal error ]
                              , (List.length refreshList > 0)
                                    ? ( [ config.clientInterface.refreshTagger refreshList ], [] )
                              )
                in
                    ( model ! [], msgs )

            RetryConnectCmd retryCount failureMsg cmd ->
                let
                    parentMsg =
                        case failureMsg of
                            PGConnectError cause ( _, error ) ->
                                nonFatal ("Database Connnection Error:" +-+ "Error:" +-+ error +-+ "Connection Retry:" +-+ retryCount)

                            _ ->
                                Debug.crash "BUG -- Should never get here"
                in
                    ( model ! [ cmd ], [ parentMsg ] )

            RetryModule msg ->
                updateRetry msg model


{-|
    subscriptions
-}
elmSubscriptions : Config comparable msg -> Model comparable -> Sub msg
elmSubscriptions config model =
    let
        pgSub =
            model.pgListenConnectionId
                |?> (\pgConnectionId -> model.pgListenError ? ( Sub.none, Postgres.listen PGListenError PGListenSuccess PGListenEvent pgConnectionId channelName ))
                ?= Sub.none
    in
        model.running ? ( Sub.map config.clientInterface.routeToMeTagger pgSub, Sub.none )



{-
   Helpers
-}


createSubscriber : Config comparable msg -> List EntityEventTypes -> Model comparable -> comparable -> SubscribeErrorTagger comparable msg -> Result (List String) ( Model comparable, Cmd msg )
createSubscriber config entityEventTypesList model targetDbWatcherId subscribeErrorTagger =
    let
        initialErrors =
            List.isEmpty entityEventTypesList ? ( [ "no entityEventTypes exist" ], [] )

        validate ( entityName, eventTypes ) errors =
            let
                validations =
                    [ ( config.validateId targetDbWatcherId, "dbWatcherId is not valid:" +-+ targetDbWatcherId )
                    , ( List.isEmpty eventTypes, "no event types exist for" +-+ entityName )
                    ]
            in
                List.append
                    (validations
                        |> List.filter (\( condition, _ ) -> condition)
                        |> List.map (\( _, errorMessage ) -> errorMessage)
                    )
                    errors

        errors =
            List.foldl validate initialErrors entityEventTypesList
    in
        case errors of
            [] ->
                Ok ( { model | watchedEntities = Dict.insert targetDbWatcherId entityEventTypesList model.watchedEntities }, Cmd.none )

            _ :: _ ->
                Err errors


destroySubscriber : Model comparable -> comparable -> UnsubscribeErrorTagger comparable msg -> Result (List String) ( Model comparable, Cmd msg )
destroySubscriber model targetcomparable unsubscribeErrorTagger =
    case Dict.get targetcomparable model.watchedEntities of
        Just _ ->
            Ok ( { model | watchedEntities = Dict.remove targetcomparable model.watchedEntities }, Cmd.none )

        Nothing ->
            Err [ "Cannot unsubscribe watcher id" +-+ targetcomparable +-+ "that does not exist" ]


createRefreshList : Config comparable msg -> EntityName -> Target -> Operation -> Maybe PropertyName -> WatchedEntitiesDict comparable -> List comparable
createRefreshList config compareEntityName compareTarget compareOperation comparePropertyName watchedEntities =
    let
        compareEventType =
            ( compareTarget, compareOperation, comparePropertyName )

        selectEventTypes eventTypes =
            List.length (List.filter (\eventType -> eventType == compareEventType) eventTypes) > 0

        selectWatchedEntities entityEventTypesList =
            List.length
                (List.filter
                    (\( entityName, eventTypes ) ->
                        (entityName == compareEntityName) ? ( (selectEventTypes eventTypes), False )
                    )
                    entityEventTypesList
                )
                > 0

        l =
            Debug.log "In createRefreshList: event received from DB" ( compareEntityName, compareTarget, compareOperation, comparePropertyName )
    in
        Dict.filter (\_ entityEventTypesList -> selectWatchedEntities entityEventTypesList) watchedEntities
            |> Dict.keys


pgConnect : Config comparable msg -> DbConnectionInfo -> Model comparable -> ConnectCause -> ( Retry.Model Msg, Cmd Msg )
pgConnect config dbConnectionInfo model cause =
    Retry.retry retryConfig model.retryModel (PGConnectError cause) RetryConnectCmd (connectCmd dbConnectionInfo cause)


pgDisconnect : Maybe Int -> DisconnectCause -> Cmd Msg
pgDisconnect pgListenConnectionId cause =
    pgListenConnectionId
        |?> (\pgConnectionId -> Postgres.disconnect (PGDisconnectError cause) (PGDisconnect cause) pgConnectionId False)
        ?= Cmd.none


connectCmd : DbConnectionInfo -> ConnectCause -> FailureTagger ( ConnectionId, String ) Msg -> Cmd Msg
connectCmd dbConnectionInfo cause failureTagger =
    Postgres.connect failureTagger
        (PGConnect cause)
        PGConnectionLost
        dbConnectionInfo.timeout
        dbConnectionInfo.host
        dbConnectionInfo.port_
        dbConnectionInfo.database
        dbConnectionInfo.user
        dbConnectionInfo.password


delayUpdateMsg : Time -> msg -> Cmd msg
delayUpdateMsg delay msg =
    Task.perform (\_ -> msg) <| Process.sleep delay


delayCmd : Cmd Msg -> Time -> Cmd Msg
delayCmd cmd =
    (flip delayUpdateMsg) <| DoCmd cmd


listenPayloadDecoder : JD.Decoder ListenPayload
listenPayloadDecoder =
    JD.succeed ListenPayload
        <|| (field "event" eventDecoder)


eventDecoder : JD.Decoder Event
eventDecoder =
    JD.succeed Event
        <|| (field "entityName" JD.string)
        <|| (field "target" JD.string)
        <|| (field "operation" JD.string)
        <|| maybe (field "propertyName" JD.string)
