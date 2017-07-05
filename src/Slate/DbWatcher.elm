module Slate.DbWatcher
    exposing
        ( Msg
        , Model
        , Config
        , init
        , start
        , stop
        , update
        , subscribe
        , unsubscribe
        , unsubscribeAll
        , elmSubscriptions
        )

{-| Slate Database Watcher.

@docs Msg , Model , Config, Msg , Model , Config , init , start , stop , update , subscribe , unsubscribe , unsubscribeAll, elmSubscriptions
-}

import Task
import Dict exposing (Dict)
import Time exposing (Time)
import Json.Decode as JD exposing (..)
import Result.Extra as Result exposing (..)
import Postgres exposing (ConnectionId, ListenUnlisten)
import StringUtils exposing (..)
import Retry exposing (..)
import Utils.Ops exposing (..)
import Utils.Error exposing (..)
import Utils.Log exposing (..)
import ParentChildUpdate exposing (..)
import Slate.Common.Db as Db exposing (..)
import Slate.Common.Event exposing (..)
import Slate.Common.Entity exposing (..)
import Slate.Engine.Query exposing (..)
import Slate.Common.Taggers exposing (..)
import DebugF


type ConnectCause
    = StartingCause
    | ReconnectCause


type DisconnectCause
    = StoppingCause
    | StopAfterConnectCause


channelName : String
channelName =
    "eventsinsert"


{-| Referesh tagger
-}
type alias RefreshTagger msg =
    List QueryId -> msg


{-| Config
-}
type alias Config msg =
    { pgReconnectDelayInterval : Time
    , stopDelayInterval : Time
    , errorTagger : ErrorTagger String msg
    , logTagger : LogTagger String msg
    , routeToMeTagger : Msg -> msg
    , refreshTagger : RefreshTagger msg
    , debug : Bool
    }


retryConfig : DbConnectionInfo -> Retry.Config Msg
retryConfig dbConnectionInfo =
    { retryMax = 3000
    , delayNext = Retry.constantDelay 5000
    , routeToMeTagger = RetryMsg dbConnectionInfo
    }


type alias WatcherClientInfo =
    { entityEventTypes : List EntityEventTypes
    , dbConnectionInfo : DbConnectionInfo
    }


type alias WatchedEntitiesDict =
    Dict QueryId WatcherClientInfo


{-| Model
-}
type alias Model =
    { connectedDbConnections : Dict String { dbConnectionInfo : DbConnectionInfo, connectionId : ConnectionId }
    , watchedEntities : WatchedEntitiesDict
    , retryModels : Dict String (Retry.Model Msg)
    , running : Bool
    }


{-| init DbWatcher
-}
init : Config msg -> ( Model, Cmd msg )
init config =
    { connectedDbConnections = Dict.empty
    , watchedEntities = Dict.empty
    , retryModels = Dict.empty
    , running = False
    }
        ! []


{-| start watching
-}
start : Config msg -> Model -> List DbConnectionInfo -> ( Model, Cmd msg )
start config model dbConnectionInfos =
    not model.running
        ? ( dbConnectionInfos
                |> List.foldl
                    (\dbConnectionInfo ( model, cmds ) ->
                        pgConnect config dbConnectionInfo model StartingCause
                            |> (\( retryModel, retryCmd ) -> ( setRetryModel dbConnectionInfo model retryModel, Cmd.map config.routeToMeTagger retryCmd :: cmds ))
                    )
                    ( model, [] )
                |> (\( model, cmds ) -> { model | running = True } ! cmds)
          , model ! []
          )


{-| stop watching
-}
stop : Config msg -> Model -> ( Model, Cmd msg )
stop config model =
    model.running
        ? ( unsubscribeAll config model, model )
        |> (\model ->
                model.connectedDbConnections
                    |> Dict.values
                    |> List.map .connectionId
                    |> List.foldl
                        (\connectionId ( model, cmds ) ->
                            pgDisconnect model connectionId StoppingCause
                                |> (\cmd -> ( model, Cmd.map config.routeToMeTagger cmd :: cmds ))
                        )
                        ( model, [] )
                    |> (\( model, cmds ) -> { model | running = False } ! cmds)
           )


setRetryModel : DbConnectionInfo -> Model -> Retry.Model Msg -> Model
setRetryModel dbConnectionInfo model retryModel =
    { model | retryModels = Dict.insert (Db.makeComparable dbConnectionInfo) retryModel model.retryModels }


getRetryModel : DbConnectionInfo -> Model -> Retry.Model Msg
getRetryModel dbConnectionInfo model =
    Db.makeComparable dbConnectionInfo
        |> (\dbConnectionInfoStr ->
                Dict.get dbConnectionInfoStr model.retryModels
                    ?!= (\_ -> Debug.crash ("Cannot find retryModel for:" +-+ dbConnectionInfoStr +-+ "Retry models:" +-+ model.retryModels))
           )


{-| subscribe to DbWatcher
-}
subscribe : Config msg -> DbConnectionInfo -> Model -> List EntityEventTypes -> QueryId -> Result (List String) ( Model, Cmd msg )
subscribe config dbConnectionInfo model entityEventTypesList queryId =
    let
        initialErrors =
            List.isEmpty entityEventTypesList ? ( [ "no entityEventTypes exist" ], [] )

        validate ( entityName, eventTypes ) errors =
            let
                validations =
                    [ ( queryId < 0, "dbWatcherId is not valid:" +-+ queryId )
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
        Dict.insert queryId { entityEventTypes = entityEventTypesList, dbConnectionInfo = dbConnectionInfo } model.watchedEntities
            |> (\watchedEntities ->
                    config.debug
                        ? ( DebugF.log "*** DEBUG DbWatcher watchedEntities (post subscribe)" watchedEntities, Dict.empty )
                        |> always
                            ((errors == [])
                                ? ( Ok ( { model | watchedEntities = watchedEntities }, Cmd.none )
                                  , Err errors
                                  )
                            )
               )


{-| unsubscribe to DbWatcher
-}
unsubscribe : Config msg -> Model -> QueryId -> Result (List String) ( Model, Cmd msg )
unsubscribe config model queryId =
    Dict.get queryId model.watchedEntities
        |?> (\_ ->
                Dict.remove queryId model.watchedEntities
                    |> (\watchedEntities ->
                            config.debug
                                ? ( DebugF.log "*** DEBUG DbWatcher watchedEntities (post unsubscribe)" watchedEntities, Dict.empty )
                                |> always (Ok ( { model | watchedEntities = watchedEntities }, Cmd.none ))
                       )
            )
        ?= Err [ "Cannot unsubscribe watcher queryId:" +-+ queryId +-+ "that does not exist" ]


{-| unsubscribe all from DbWatcher
-}
unsubscribeAll : Config msg -> Model -> Model
unsubscribeAll config model =
    { model | watchedEntities = Dict.empty }


{-| Msg
-}
type Msg
    = Nop
    | Stopped
    | DoCmd (Cmd Msg)
    | PGConnect ConnectCause DbConnectionInfo ConnectionId
    | PGConnectError DbConnectionInfo ( ConnectionId, String )
    | PGConnectionLost DbConnectionInfo ( ConnectionId, String )
    | PGDisconnectError DisconnectCause DbConnectionInfo ( ConnectionId, String )
    | PGDisconnect DisconnectCause DbConnectionInfo ConnectionId
    | PGListenSuccess ( ConnectionId, String, ListenUnlisten )
    | PGListenError DbConnectionInfo ( ConnectionId, String )
    | PGListenEvent ( ConnectionId, String, String )
    | RetryConnectCmd DbConnectionInfo Int Msg (Cmd Msg)
    | RetryMsg DbConnectionInfo (Retry.Msg Msg)


{-| update
-}
update : Config msg -> Msg -> Model -> ( ( Model, Cmd Msg ), List msg )
update config msg model =
    let
        logInfo message =
            config.logTagger ( LogLevelInfo, message )

        logDebug message =
            config.logTagger ( LogLevelDebug, message )

        nonFatal error =
            config.errorTagger ( NonFatalError, error )

        fatal error =
            config.errorTagger ( FatalError, error )

        disconnected cause dbConnectionInfo pgConnectionId model disonnectMsgs =
            setUnconnected dbConnectionInfo pgConnectionId model
                |> (\model ->
                        case cause of
                            StoppingCause ->
                                ((List.length <| Dict.keys model.connectedDbConnections) == 0)
                                    ? ( update config Stopped model, ( model ! [], [] ) )
                                    |> (\( ( model, cmd ), msgs ) -> ( ( model, cmd ), List.append disonnectMsgs msgs ))

                            StopAfterConnectCause ->
                                ( model ! [], disonnectMsgs )
                   )

        setConnected dbConnectionInfo pgConnectionId model =
            { model | connectedDbConnections = Dict.insert (Db.makeComparable dbConnectionInfo) { connectionId = pgConnectionId, dbConnectionInfo = dbConnectionInfo } model.connectedDbConnections }

        setUnconnected dbConnectionInfo pgConnectionId model =
            { model | connectedDbConnections = Dict.remove (Db.makeComparable dbConnectionInfo) model.connectedDbConnections }

        updateRetry dbConnectionInfo =
            updateChildParent (Retry.update (retryConfig dbConnectionInfo)) (update config) (getRetryModel dbConnectionInfo) (retryConfig dbConnectionInfo).routeToMeTagger (setRetryModel dbConnectionInfo)
    in
        case msg of
            Nop ->
                ( model ! [], [] )

            DoCmd cmd ->
                ( model ! [ cmd ], [] )

            Stopped ->
                ( { model | watchedEntities = Dict.empty } ! [], [] )

            PGConnect cause dbConnectionInfo pgConnectionId ->
                { model | retryModels = Dict.remove (Db.makeComparable dbConnectionInfo) model.retryModels }
                    |> (\model ->
                            setConnected dbConnectionInfo pgConnectionId model
                                |> (\model ->
                                        (case cause of
                                            StartingCause ->
                                                ( Cmd.none, [] )

                                            ReconnectCause ->
                                                Db.makeComparable dbConnectionInfo
                                                    |> (\dbConnectionInfoStr ->
                                                            model.watchedEntities
                                                                |> Dict.filter (\_ { dbConnectionInfo } -> Db.makeComparable dbConnectionInfo == dbConnectionInfoStr)
                                                                |> Dict.keys
                                                                |> (\refreshList -> model.running ? ( ( Cmd.none, [ config.refreshTagger refreshList ] ), ( pgDisconnect model pgConnectionId StopAfterConnectCause, [] ) ))
                                                       )
                                        )
                                            |> (\( cmd, msgs ) -> ( model ! [ cmd ], List.append msgs [ logDebug ("PGConnect:" +-+ pgConnectionId +-+ "Cause:" +-+ cause) ] ))
                                   )
                       )

            PGConnectError dbConnectionInfo ( _, pgError ) ->
                { model | retryModels = Dict.remove (Db.makeComparable dbConnectionInfo) model.retryModels }
                    |> (\model -> ( model ! [], [ fatal ("Cannot connect to Database after" +-+ (retryConfig dbConnectionInfo).retryMax +-+ "attempt(s)." +-+ pgError) ] ))

            PGConnectionLost dbConnectionInfo ( pgConnectionId, pgError ) ->
                setUnconnected dbConnectionInfo pgConnectionId model
                    |> (\model ->
                            model.running
                                ? ( pgConnect config dbConnectionInfo model ReconnectCause
                                        |> (\( retryModel, cmd ) -> setRetryModel dbConnectionInfo model retryModel ! [ cmd ])
                                  , model ! []
                                  )
                       )
                    |> (\( model, cmd ) -> ( model ! [ cmd ], [ nonFatal <| "PGConnectionLost:" +-+ ( pgConnectionId, pgError ) ] ))

            PGDisconnectError cause dbConnectionInfo ( pgConnectionId, pgError ) ->
                ([ nonFatal <| "PGDisconnectError:" +-+ ( pgConnectionId, pgError ) +-+ "Cause:" +-+ cause ])
                    |> disconnected cause dbConnectionInfo pgConnectionId model

            PGDisconnect cause dbConnectionInfo pgConnectionId ->
                ([ logDebug <| "PGDisconnect:" +-+ pgConnectionId +-+ "Cause:" +-+ cause ])
                    |> disconnected cause dbConnectionInfo pgConnectionId model

            PGListenSuccess ( pgConnectionId, channelName, listenUnlisten ) ->
                ( model ! [], [ logDebug <| "PGListenSuccess:" +-+ ( pgConnectionId, channelName, listenUnlisten ) ] )

            PGListenError dbConnectionInfo ( pgConnectionId, pgError ) ->
                ( setUnconnected dbConnectionInfo pgConnectionId model ! [], [ nonFatal <| "PGListenError:" +-+ ( pgConnectionId, pgError ) ] )

            PGListenEvent ( pgConnectionId, channelName, pgMessage ) ->
                config.debug
                    ? ( Debug.log "*** DEBUG DbWatcher PGListenEvent" pgMessage, "" )
                    |> (always
                            ((\event ->
                                { entityName = getEntityName event
                                , target = getTarget event
                                , operation = getOperation event
                                , propertyName = getPropertyName event
                                }
                                    |> (\parsedEvent ->
                                            ( [ parsedEvent.entityName, parsedEvent.target, parsedEvent.operation ]
                                                |> List.filter isErr
                                                |> List.map (flip (??=) <| identity)
                                            , { entityName = parsedEvent.entityName ??= always ""
                                              , target = parsedEvent.target ??= always ""
                                              , operation = parsedEvent.operation ??= always ""
                                              , propertyName = parsedEvent.propertyName |??> Just ??= always Nothing
                                              }
                                            )
                                       )
                             )
                                |> (\parse ->
                                        (JD.decodeString (JD.at [ "event" ] eventDecoder) pgMessage)
                                            |??>
                                                (\event ->
                                                    case event of
                                                        Mutating mutatingEvent _ ->
                                                            parse event
                                                                |> (\( errors, parsedEvent ) ->
                                                                        (errors == [])
                                                                            ? ( model.running
                                                                                    ? ( ( createRefreshList config model pgConnectionId parsedEvent.entityName parsedEvent.target parsedEvent.operation parsedEvent.propertyName model.watchedEntities
                                                                                        , []
                                                                                        )
                                                                                      , ( [], [ "DbWatcher not running, listen event" +-+ parsedEvent.entityName +-+ "not processed" ] )
                                                                                      )
                                                                              , ( [], errors )
                                                                              )
                                                                   )

                                                        NonMutating _ _ ->
                                                            ( [], [] )
                                                )
                                            ??= (\err -> ( [], [ "Listen decoding error:" +-+ err ] ))
                                   )
                                |> (\( refreshList, errors ) ->
                                        (errors == [])
                                            ? ( ((List.length refreshList > 0) ? ( [ config.refreshTagger refreshList ], [] ))
                                              , [ nonFatal <| String.join "\n" errors ]
                                              )
                                            |> (\msgs -> ( model ! [], msgs ))
                                   )
                            )
                       )

            RetryConnectCmd dbConnectionInfo retryCount failureMsg cmd ->
                (case failureMsg of
                    PGConnectError _ ( _, error ) ->
                        nonFatal ("Database Connnection Error:" +-+ "Error:" +-+ error +-+ "Connection Retry:" +-+ retryCount +-+ "for connection:" +-+ { dbConnectionInfo | password = "" })

                    _ ->
                        Debug.crash "BUG -- Should never get here"
                )
                    |> (\msg -> ( model ! [ cmd ], [ msg ] ))

            RetryMsg dbConnectionInfo msg ->
                updateRetry dbConnectionInfo msg model


{-|
    subscriptions
-}
elmSubscriptions : Config msg -> Model -> Sub msg
elmSubscriptions config model =
    model.running
        ? ( model.connectedDbConnections
                |> Dict.values
                |> List.map (\{ dbConnectionInfo, connectionId } -> Postgres.listen (PGListenError dbConnectionInfo) PGListenSuccess PGListenEvent connectionId channelName)
                |> (List.map <| Sub.map config.routeToMeTagger)
                |> Sub.batch
          , Sub.none
          )



{-
   Helpers
-}


createRefreshList : Config msg -> Model -> ConnectionId -> EntityName -> Target -> Operation -> Maybe PropertyName -> WatchedEntitiesDict -> List QueryId
createRefreshList config model pgConnectionId compareEntityName compareTarget compareOperation compareMaybePropertyName watchedEntities =
    let
        compareEventType =
            Debug.log "compareEventType" ( compareTarget, compareOperation, compareMaybePropertyName )

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
    in
        (Dict.filter (\dbConnectionInfoStr { connectionId } -> pgConnectionId == connectionId) model.connectedDbConnections
            |> Dict.toList
            |> List.head
        )
            |?> (\( dbConnectionInfoStr, { dbConnectionInfo } ) ->
                    config.debug
                        ? ( DebugF.log "watchedEntities (createRefreshList)" watchedEntities, watchedEntities )
                        |> Dict.filter (\_ { entityEventTypes, dbConnectionInfo } -> Db.makeComparable dbConnectionInfo == dbConnectionInfoStr && selectWatchedEntities entityEventTypes)
                        |> Dict.keys
                )
            ?= []
            |> (\refreshList -> config.debug ? ( Debug.log "*** DEBUG DbWatcher Refresh List" refreshList, refreshList ))


msgToCmd : msg -> Cmd msg
msgToCmd msg =
    Task.perform (\_ -> msg) <| Task.succeed msg


pgConnect : Config msg -> DbConnectionInfo -> Model -> ConnectCause -> ( Retry.Model Msg, Cmd Msg )
pgConnect config dbConnectionInfo model cause =
    Retry.retry (retryConfig dbConnectionInfo) Retry.initModel (PGConnectError dbConnectionInfo) (RetryConnectCmd dbConnectionInfo) (connectCmd dbConnectionInfo cause)


pgDisconnect : Model -> ConnectionId -> DisconnectCause -> Cmd Msg
pgDisconnect model pgConnectionId cause =
    (Dict.filter (\_ { connectionId } -> pgConnectionId == connectionId) model.connectedDbConnections
        |> Dict.toList
        |> List.head
    )
        |?> (\( _, { dbConnectionInfo } ) -> dbConnectionInfo)
        ?!= (\_ -> Debug.crash ("BUG: Cannot find connectionId"))
        |> (\dbConnectionInfo ->
                Postgres.disconnect (PGDisconnectError cause dbConnectionInfo) (PGDisconnect cause dbConnectionInfo) pgConnectionId False
           )


connectCmd : DbConnectionInfo -> ConnectCause -> FailureTagger ( ConnectionId, String ) Msg -> Cmd Msg
connectCmd dbConnectionInfo cause failureTagger =
    Postgres.connect failureTagger
        (PGConnect cause dbConnectionInfo)
        (PGConnectionLost dbConnectionInfo)
        dbConnectionInfo.timeout
        dbConnectionInfo.host
        dbConnectionInfo.port_
        dbConnectionInfo.database
        dbConnectionInfo.user
        dbConnectionInfo.password
