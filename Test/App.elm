port module App exposing (..)

{- TODO remove this when compiler is fixed -}

import Json.Decode
import Dict exposing (Dict)
import Slate.DbWatcher as DbWatcher exposing (..)
import Time exposing (..)
import Task exposing (..)
import Process exposing (..)
import ParentChildUpdate exposing (..)
import DebugF
import Retry exposing (..)
import Postgres exposing (..)
import StringUtils exposing ((+-+))
import Slate.Common.Db exposing (..)
import Slate.Common.Event exposing (..)
import Utils.Error exposing (..)
import Utils.Log exposing (..)
import Utils.Ops exposing (..)
import Slate.Engine.Query exposing (..)


port exitApp : Float -> Cmd msg


port externalStop : (() -> msg) -> Sub msg


dbConnectionInfo : DbConnectionInfo
dbConnectionInfo =
    { host = "testDbWatcherServer"
    , port_ = 5432
    , database = "test_dbwatcher"
    , user = "postgres"
    , password = "password"
    , timeout = 15000
    }


dbConnectionInfo2 : DbConnectionInfo
dbConnectionInfo2 =
    { host = "testDbWatcherServer"
    , port_ = 5432
    , database = "test_dbwatcher"
    , user = "postgres"
    , password = "password"
    , timeout = 15000
    }


pgReconnectDelayInterval : Time
pgReconnectDelayInterval =
    10 * second


writeEventDelayInterval : Time
writeEventDelayInterval =
    5 * second


stopDelayInterval : Time
stopDelayInterval =
    5 * second


dbWatcherConfig : DbWatcher.Config Msg
dbWatcherConfig =
    { pgReconnectDelayInterval = pgReconnectDelayInterval
    , stopDelayInterval = stopDelayInterval
    , errorTagger = DbWatcherError
    , logTagger = DbWatcherLog
    , routeToMeTagger = DbWatcherMsg
    , refreshTagger = DbWatcherRefresh
    , debug = True
    }


retryConfig : Retry.Config Msg
retryConfig =
    { retryMax = 3000
    , delayNext = Retry.constantDelay 5000
    , routeToMeTagger = RetryMsg
    }


main : Program Never (Model) Msg
main =
    Platform.program
        { init = init
        , update = update
        , subscriptions = subscriptions
        }


type alias Model =
    { dbConnectionInfos : List DbConnectionInfo
    , running : Bool
    , retryModel : Retry.Model Msg
    , retryModel2 : Retry.Model Msg
    , connectionIds : Dict Int ConnectionId
    , countEvents : Int
    , countSubscribes : Int
    , writingEvents : Bool
    , dbWatcherModel : DbWatcher.Model
    , testIds : List QueryId
    , testIdIndex : Int
    }


init : ( Model, Cmd Msg )
init =
    DbWatcher.init dbWatcherConfig
        |> (\( dbWatcherModel, dbWatcherInitCmd ) ->
                { dbConnectionInfos = [ dbConnectionInfo, dbConnectionInfo2 ]
                , running = False
                , retryModel = Retry.initModel
                , retryModel2 = Retry.initModel
                , connectionIds = Dict.empty
                , countEvents = 0
                , countSubscribes = 0
                , writingEvents = False
                , dbWatcherModel = dbWatcherModel
                , testIds = [ 1, 2, 3, 4, 5, 6, 7, 8 ]
                , testIdIndex = 0
                }
                    |> (\model ->
                            Postgres.clientSideConfig ConfigError Configured BadResponse (Just "ws://localhost:8080/pgproxy") (Just "{\"sessionId\": \"1a787c0a-1215-4291-94cf-3bd016cf49a1\"}")
                                |> (\postgresCmd ->
                                        subscribeOrStop model
                                            |> (\( model, cmd ) -> model ! [ cmd, dbWatcherInitCmd, postgresCmd, Postgres.debug True ])
                                   )
                       )
           )


msgToCmd : msg -> Cmd msg
msgToCmd msg =
    Task.perform (\_ -> msg) <| Task.succeed msg


type Msg
    = DoCmd (Cmd Msg)
    | Stop ()
    | StartWriting
    | StopWriting
    | WriteEvent Int Int
    | WriteEventSuccess ( Int, List String )
    | WriteEventError ( Int, String )
    | PGConnect Int Int
    | PGConnectError ( Int, String )
    | PGConnectionLost ( Int, String )
    | PGDisconnect Int
    | PGDisconnectError ( Int, String )
    | RetryConnectCmd Int Msg (Cmd Msg)
    | RetryMsg (Retry.Msg Msg)
    | DbWatcherError ( ErrorType, String )
    | DbWatcherLog ( LogLevel, String )
    | DbWatcherMsg DbWatcher.Msg
    | DbWatcherRefresh (List QueryId)
    | ConfigError String
    | Configured ()
    | BadResponse ( String, String )


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    let
        updateDbWatcher =
            updateChildApp (DbWatcher.update dbWatcherConfig) update .dbWatcherModel DbWatcherMsg (\model dbWatcherModel -> { model | dbWatcherModel = dbWatcherModel })

        updateRetry =
            updateChildApp (Retry.update retryConfig) update .retryModel RetryMsg (\model retryModel -> { model | retryModel = retryModel })
    in
        case msg of
            DoCmd cmd ->
                (model ! [ cmd ])

            Stop _ ->
                let
                    l =
                        ( DebugF.log "App" "Stop", DebugF.log "Events Written" (toString model.countEvents) )
                in
                    model ! [ delayCmd (exitApp 0) stopDelayInterval ]

            StartWriting ->
                let
                    l =
                        DebugF.log "App " "StartWriting"

                    ( retryModel, cmd ) =
                        pgConnect 1 model

                    ( retryModel2, cmd2 ) =
                        pgConnect 2 model
                in
                    ({ model | retryModel = retryModel, retryModel2 = retryModel2, writingEvents = True } ! [ cmd, cmd2 ])

            StopWriting ->
                let
                    l =
                        DebugF.log "App " "StopWriting"
                in
                    DbWatcher.stop dbWatcherConfig model.dbWatcherModel
                        |> (\( dbWatcherModel, cmd ) -> ({ model | dbWatcherModel = dbWatcherModel, writingEvents = False } ! [ cmd ]))

            WriteEvent which connectionId ->
                let
                    status =
                        model.writingEvents
                            ? ( "      Event #" +-+ (model.countEvents + 1)
                              , "      Writing Events Stopped"
                              )

                    l =
                        Debug.log "App WriteEvent" ("connectionId" +-+ connectionId +-+ "which:" +-+ which +-+ status)

                    insertStatement =
                        getInsertStatement model.countEvents insertStatements

                    cmd =
                        model.writingEvents
                            ? ( Postgres.query WriteEventError WriteEventSuccess connectionId insertStatement 10
                              , delayUpdateMsg (Stop ()) stopDelayInterval
                              )
                in
                    model ! [ cmd ]

            WriteEventSuccess ( connectionId, rowStrs ) ->
                let
                    l =
                        Debug.log "App WriteEventSuccess (connectionId, rows returned)" ( connectionId, rowStrs )

                    countEvents =
                        model.countEvents + 1

                    which =
                        (countEvents % 2 == 0) ? ( 1, 2 )

                    nextConnectionId =
                        Dict.get which model.connectionIds
                            ?!= (\_ -> Debug.crash "Missing Connection... Perhaps the server exited/crashed.")

                    cmd =
                        (countEvents < List.length insertStatements)
                            ? ( delayUpdateMsg (WriteEvent which nextConnectionId) writeEventDelayInterval
                              , delayUpdateMsg (Stop ()) stopDelayInterval
                              )
                in
                    ({ model | countEvents = countEvents } ! [ cmd ])

            WriteEventError ( connectionId, error ) ->
                let
                    l =
                        Debug.log "App WriteEventError" ( connectionId, error )
                in
                    model ! [ exitApp 1 ]

            PGConnect which connectionId ->
                let
                    -- crash =
                    --     Debug.crash "!!!"
                    l =
                        DebugF.log "App PGConnect" ("PGConnect:" +-+ connectionId)

                    cmd =
                        (which == 1) ? ( delayUpdateMsg (WriteEvent which connectionId) writeEventDelayInterval, Cmd.none )
                in
                    ({ model | connectionIds = Dict.insert which connectionId model.connectionIds } ! [ cmd ])

            PGConnectError ( connectionId, error ) ->
                model ! []

            PGConnectionLost ( connectionId, error ) ->
                model ! []

            PGDisconnect connectionId ->
                model ! []

            PGDisconnectError ( connectionId, error ) ->
                model ! []

            RetryConnectCmd retryCount failureMsg cmd ->
                let
                    l =
                        case failureMsg of
                            PGConnectError ( _, error ) ->
                                DebugF.log "App RetryConnectCmd" ("Database Connnection Error:" +-+ "Error:" +-+ error +-+ "Connection Retry:" +-+ retryCount)

                            _ ->
                                Debug.crash "BUG -- Should never get here"
                in
                    (model ! [ cmd ])

            RetryMsg msg ->
                updateRetry msg model

            DbWatcherError ( errorType, details ) ->
                let
                    l =
                        case errorType of
                            NonFatalError ->
                                DebugF.log ("DbWatcherError" +-+ errorType) details

                            _ ->
                                Debug.crash details
                in
                    (model ! [])

            DbWatcherLog ( logLevel, details ) ->
                let
                    l =
                        DebugF.log ("DbWatcherLog" +-+ logLevel) details
                in
                    (model ! [])

            DbWatcherRefresh dbWatcherId ->
                let
                    l =
                        DebugF.log "DbWatcher " ("DbWatcherRefresh" +-+ dbWatcherId)
                in
                    subscribeOrStop model

            DbWatcherMsg msg ->
                updateDbWatcher msg model

            ConfigError error ->
                Debug.crash ("ConfigError:" +-+ error)
                    |> always (model ! [])

            Configured () ->
                Debug.log "Configured" ()
                    |> (\_ ->
                            DbWatcher.start dbWatcherConfig model.dbWatcherModel model.dbConnectionInfos
                                |> (\( dbWatcherModel, dbWatcherStartCmd ) ->
                                        { model | dbWatcherModel = dbWatcherModel } ! [ msgToCmd StartWriting, dbWatcherStartCmd ]
                                   )
                       )

            BadResponse ( response, error ) ->
                Debug.log "BadResponse:" ( response, error )
                    |> always (model ! [])


subscriptions : Model -> Sub Msg
subscriptions model =
    let
        stopApp =
            externalStop Stop

        dbWatcherSub =
            DbWatcher.elmSubscriptions dbWatcherConfig model.dbWatcherModel
    in
        Sub.batch [ stopApp, dbWatcherSub ]


subscribeOrStop : Model -> ( Model, Cmd Msg )
subscribeOrStop model =
    (((model.countSubscribes % 2 == 0) ? ( 1, 2 ))
        |> (\which ->
                (model.dbConnectionInfos
                    |> (List.drop <| which - 1)
                    |> List.head
                )
           )
    )
        |?> (\dbConnectionInfo ->
                createEntityEventTypes model.testIdIndex
                    |> (\entityEventTypes ->
                            ( List.head model.testIds, List.drop 1 model.testIds )
                                |> (\( maybeTestId, testIds ) ->
                                        { model | testIds = testIds, testIdIndex = model.testIdIndex + 1, countSubscribes = model.countSubscribes + 1 }
                                            |> (\model ->
                                                    maybeTestId
                                                        |?> (\testId ->
                                                                DbWatcher.subscribe dbWatcherConfig dbConnectionInfo model.dbWatcherModel entityEventTypes (Debug.log "DbWatcher subscribing testId" testId)
                                                                    |??> (\( dbWatcherModel, cmd ) -> { model | dbWatcherModel = dbWatcherModel } ! [ cmd ])
                                                                    ??= (\errors -> model ! [ msgToCmd <| DbWatcherError ( NonFatalError, (String.join "," errors) ) ])
                                                            )
                                                        ?= (DbWatcher.unsubscribeAll dbWatcherConfig model.dbWatcherModel
                                                                |> (\dbWatcherModel -> { model | dbWatcherModel = dbWatcherModel } ! [ msgToCmd StopWriting ])
                                                           )
                                               )
                                   )
                       )
            )
        ?!= (\_ -> Debug.crash "BUG")


createEntityEventTypes : Int -> List EntityEventTypes
createEntityEventTypes count =
    case count of
        0 ->
            [ ( "Person", [ ( "entity", "created", Nothing ), ( "property", "added", Just "name" ) ] ) ]

        1 ->
            [ ( "User", [ ( "entity", "created", Nothing ), ( "property", "added", Just "authenticationMethod" ) ] ) ]

        2 ->
            [ ( "Person", [ ( "relationship", "added", Just "address" ), ( "relationship", "removed", Just "address" ) ] ) ]

        3 ->
            [ ( "Person", [ ( "entity", "destroyed", Nothing ) ] ) ]

        4 ->
            [ ( "User", [ ( "entity", "created", Nothing ), ( "entity", "destroyed", Nothing ) ] ) ]

        5 ->
            [ ( "Person", [ ( "relationship", "added", Just "address" ) ] ) ]

        6 ->
            [ ( "BadEntity", [] ) ]

        7 ->
            []

        _ ->
            [ ( "Person", [ ( "entity", "created", Nothing ), ( "property", "added", Just "name" ) ] ) ]


pgConnect : Int -> Model -> ( Retry.Model Msg, Cmd Msg )
pgConnect which model =
    (model.dbConnectionInfos
        |> (List.drop <| which - 1)
        |> List.head
    )
        |?> (\dbConnectionInfo -> Retry.retry retryConfig model.retryModel PGConnectError RetryConnectCmd (connectCmd which dbConnectionInfo))
        ?!= (\_ -> Debug.crash "BUG")


pgDisconnect : Maybe Int -> Cmd Msg
pgDisconnect connectionId =
    connectionId
        |?> (\pgConnectionId -> Postgres.disconnect PGDisconnectError PGDisconnect pgConnectionId False)
        ?= Cmd.none


connectCmd : Int -> DbConnectionInfo -> FailureTagger ( ConnectionId, String ) Msg -> Cmd Msg
connectCmd which dbConnectionInfo failureTagger =
    Postgres.connect failureTagger
        (PGConnect which)
        PGConnectionLost
        dbConnectionInfo.timeout
        dbConnectionInfo.host
        dbConnectionInfo.port_
        dbConnectionInfo.database
        dbConnectionInfo.user
        dbConnectionInfo.password


delayUpdateMsg : Msg -> Time -> Cmd Msg
delayUpdateMsg msg delay =
    Task.perform (\_ -> msg) <| Process.sleep delay


delayCmd : Cmd Msg -> Time -> Cmd Msg
delayCmd cmd =
    delayUpdateMsg <| DoCmd cmd


insertStatements : List String
insertStatements =
    [ "SELECT insert_events($$($1[1], $2, '{\"target\": \"entity\",\"entityId\": \"123\",\"metadata\": {\"command\": \"asOneCmd\", \"initiatorId\": \"999888777\" },\"operation\": \"created\",\"entityName\": \"Person\"}')$$);"
    , "SELECT insert_events($$($1[1], $2, '{\"target\": \"entity\",\"entityId\": \"234\",\"metadata\": {\"command\": \"asOneCmd\", \"initiatorId\": \"999888777\" },\"operation\": \"created\",\"entityName\": \"Person\"}')$$);"
    , "SELECT insert_events($$($1[1], $2, '{\"target\": \"entity\",\"entityId\": \"234\",\"metadata\": {\"command\": \"asOneCmd\", \"initiatorId\": \"999888777\" },\"operation\": \"created\",\"entityName\": \"Person\"}')$$);"
    , "SELECT insert_events($$($1[1], $2, '{\"target\": \"entity\",\"entityId\": \"234\",\"metadata\": {\"command\": \"asOneCmd\", \"initiatorId\": \"999888777\" },\"operation\": \"created\",\"entityName\": \"Person\"}')$$);"
    , "SELECT insert_events($$($1[1], $2, '{\"target\": \"entity\",\"entityId\": \"234\",\"metadata\": {\"command\": \"asOneCmd\", \"initiatorId\": \"999888777\" },\"operation\": \"created\",\"entityName\": \"Person\"}')$$);"
    , "SELECT insert_events($$($1[1], $2, '{\"target\": \"relationship\",  \"entityId\": \"123\", \"metadata\": {\"command\": \"asOneCmd\", \"initiatorId\": \"999888777\"}, \"operation\": \"added\", \"entityName\": \"Person\", \"propertyName\": \"address\"}' )$$);"
    , "SELECT insert_events($$($1[1], $2, '{\"target\": \"relationship\",  \"entityId\": \"123\", \"metadata\": {\"command\": \"asOneCmd\", \"initiatorId\": \"999888777\"}, \"operation\": \"added\", \"entityName\": \"Person\", \"propertyName\": \"address\"}' )$$);"
    , "SELECT insert_events($$($1[1], $2, '{\"target\": \"relationship\",  \"entityId\": \"123\", \"metadata\": {\"command\": \"asOneCmd\", \"initiatorId\": \"999888777\"}, \"operation\": \"added\", \"entityName\": \"Person\", \"propertyName\": \"address\"}' )$$);"
    , "SELECT insert_events($$($1[1], $2, '{\"target\": \"relationship\",  \"entityId\": \"123\", \"metadata\": {\"command\": \"asOneCmd\", \"initiatorId\": \"999888777\"}, \"operation\": \"added\", \"entityName\": \"Person\", \"propertyName\": \"address\"}' )$$);"
    , "SELECT insert_events($$($1[1], $2, '{\"target\": \"relationship\",  \"entityId\": \"123\", \"metadata\": {\"command\": \"asOneCmd\", \"initiatorId\": \"999888777\"}, \"operation\": \"added\", \"entityName\": \"Person\", \"propertyName\": \"address\"}' )$$);"
    , "SELECT insert_events($$($1[1], $2, '{\"target\": \"relationship\",  \"entityId\": \"123\", \"metadata\": {\"command\": \"asOneCmd\", \"initiatorId\": \"999888777\"}, \"operation\": \"added\", \"entityName\": \"Person\", \"propertyName\": \"address\"}' )$$);"
    , "SELECT insert_events($$($1[1], $2, '{\"target\": \"relationship\",\"entityId\": \"123\",\"metadata\": {\"command\": \"asOneCmd\", \"initiatorId\": \"999888777\" },\"operation\": \"added\",\"propertyName\": \"address\",\"entityName\": \"Person\"}')$$);"
    , "SELECT insert_events($$($1[1], $2, '{\"target\": \"entity\",\"entityId\": \"234\",\"metadata\": {\"command\": \"asOneCmd\", \"initiatorId\": \"999888777\" },\"operation\": \"destroyed\",\"entityName\": \"Person\"}')$$);"
    , "SELECT insert_events($$($1[1], $2, '{\"target\": \"entity\",\"entityId\": \"123\",\"metadata\": {\"command\": \"asOneCmd\", \"initiatorId\": \"999888777\" },\"operation\": \"created\",\"entityName\": \"User\"}')$$);\n"
    , "SELECT insert_events($$($1[1], $2, '{\"target\": \"entity\",\"entityId\": \"234\",\"metadata\": {\"command\": \"asOneCmd\", \"initiatorId\": \"999888777\" },\"operation\": \"destroyed\",\"entityName\": \"Person\"}')$$);"
    , "SELECT insert_events($$($1[1], $2, '{\"target\": \"entity\",\"entityId\": \"123\",\"metadata\": {\"command\": \"asOneCmd\", \"initiatorId\": \"999888777\" },\"operation\": \"created\",\"entityName\": \"User\"}')$$);"
    , "SELECT insert_events($$($1[1], $2, '{\"target\": \"entity\",\"entityId\": \"234\",\"metadata\": {\"command\": \"asOneCmd\", \"initiatorId\": \"999888777\" },\"operation\": \"created\",\"entityName\": \"Person\"}')$$);"
    , "SELECT insert_events($$($1[1], $2, '{\"target\": \"entity\",\"entityId\": \"123\",\"metadata\": {\"command\": \"asOneCmd\", \"initiatorId\": \"999888777\" },\"operation\": \"created\",\"propertyName\": \"address\",\"entityName\": \"User\"}')$$);"
    ]


getInsertStatement : Int -> List String -> String
getInsertStatement targetIdx statements =
    (List.indexedMap (\idx statement -> (idx == targetIdx) ? ( statement, "" )) statements)
        |> (\newStatements -> List.filter (\statement -> not <| String.isEmpty statement) newStatements)
        |> (\filteredStatements ->
                case filteredStatements of
                    statement :: [] ->
                        statement

                    _ :: _ ->
                        Debug.crash ("BUG:  There should be only one statement in filteredStatements" +-+ filteredStatements)

                    [] ->
                        Debug.crash ("BUG:  There should be at least one statement in filteredStatements" +-+ filteredStatements)
           )
