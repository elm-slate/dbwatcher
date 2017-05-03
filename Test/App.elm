port module App exposing (..)

{- TODO remove this when compiler is fixed -}

import Json.Decode
import Client exposing (..)
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


pgReconnectDelayInterval : Time
pgReconnectDelayInterval =
    10 * second


writeEventDelayInterval : Time
writeEventDelayInterval =
    5 * second


stopDelayInterval : Time
stopDelayInterval =
    5 * second


clientConfig : Client.Config (DbWatcher.Config QueryId (Client.Msg QueryId DbWatcher.Msg)) (DbWatcher.Model QueryId) QueryId DbWatcher.Msg Msg
clientConfig =
    { dbWatcherConfig = dbWatcherConfig
    , interface = DbWatcher.interface
    , routeToMeTagger = ClientMsg
    , startWritingMsg = StartWriting
    , stopWritingMsg = StopWriting
    , stopMsg = ClientStopped
    }


dbWatcherConfig : DbWatcher.Config QueryId (Client.Msg QueryId DbWatcher.Msg)
dbWatcherConfig =
    { pgReconnectDelayInterval = pgReconnectDelayInterval
    , stopDelayInterval = stopDelayInterval
    , invalidId = (\queryId -> queryId < 0)
    , clientInterface = Client.clientInterface
    }


retryConfig : Retry.Config Msg
retryConfig =
    { retryMax = 3
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


type Msg
    = ClientError ( ErrorType, String )
    | ClientLog ( LogLevel, String )
    | ClientMsg (Client.Msg QueryId DbWatcher.Msg)
    | ClientStop
    | DoCmd (Cmd Msg)
    | Stop ()
    | ClientStopped
    | StartWriting
    | StopWriting
    | WriteEvent Int
    | WriteEventSuccess ( Int, List String )
    | WriteEventError ( Int, String )
    | PGConnect Int
    | PGConnectError ( Int, String )
    | PGConnectionLost ( Int, String )
    | PGDisconnect Int
    | PGDisconnectError ( Int, String )
    | RetryConnectCmd Int Msg (Cmd Msg)
    | RetryMsg (Retry.Msg Msg)


type alias Model =
    { running : Bool
    , clientModel : Client.Model QueryId (DbWatcher.Model QueryId)
    , retryModel : Retry.Model Msg
    , maybeConnectionId : Maybe Int
    , countEvents : Int
    , writingEvents : Bool
    }


initModel : ( Model, Cmd Msg )
initModel =
    let
        ( clientModel, cmd ) =
            Client.init clientConfig dbConnectionInfo [ 1, 2, 3, 4, 5, 6, 7, 8 ]
    in
        ( { running = False
          , clientModel = clientModel
          , retryModel = Retry.initModel
          , maybeConnectionId = Nothing
          , countEvents = 0
          , writingEvents = False
          }
        , cmd
        )


init : ( Model, Cmd Msg )
init =
    initModel


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    let
        updateClient =
            updateChildApp (Client.update clientConfig) update .clientModel ClientMsg (\model clientModel -> { model | clientModel = clientModel })

        updateRetry =
            updateChildApp (Retry.update retryConfig) update .retryModel RetryMsg (\model retryModel -> { model | retryModel = retryModel })
    in
        case msg of
            ClientError ( errorType, details ) ->
                let
                    l =
                        case errorType of
                            NonFatalError ->
                                DebugF.log ("App ClientError" +-+ errorType) details

                            _ ->
                                Debug.crash <| details
                in
                    model ! []

            ClientLog ( logLevel, details ) ->
                let
                    l =
                        DebugF.log ("App ClientLog" +-+ logLevel) details
                in
                    model ! []

            ClientStop ->
                let
                    l =
                        DebugF.log "App " "ClientStop"
                in
                    model ! []

            ClientStopped ->
                let
                    l =
                        DebugF.log "App " "ClientStopped"
                in
                    ({ model | writingEvents = False } ! [])

            DoCmd cmd ->
                (model ! [ cmd ])

            Stop _ ->
                let
                    l =
                        ( DebugF.log "App" "Stop", DebugF.log "Events Written" (toString model.countEvents) )
                in
                    model ! [ delayCmd (exitApp 0) stopDelayInterval ]

            ClientMsg msg ->
                updateClient msg model

            StartWriting ->
                let
                    l =
                        DebugF.log "App " "StartWriting"

                    ( retryModel, cmd ) =
                        pgConnect dbConnectionInfo model
                in
                    ({ model | retryModel = retryModel, writingEvents = True } ! [ cmd ])

            StopWriting ->
                let
                    l =
                        DebugF.log "App " "StopWriting"
                in
                    ({ model | writingEvents = False } ! [])

            WriteEvent connectionId ->
                let
                    status =
                        model.writingEvents
                            ? ( "      Event #" +-+ (model.countEvents + 1)
                              , "      Writing Events Stopped"
                              )

                    l =
                        Debug.log "App WriteEvent" ("connectionId" +-+ connectionId +-+ status)

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

                    cmd =
                        (countEvents < List.length insertStatements)
                            ? ( delayUpdateMsg (WriteEvent connectionId) writeEventDelayInterval
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

            PGConnect connectionId ->
                let
                    l =
                        DebugF.log "App PGConnect" ("PGConnect:" +-+ connectionId)
                in
                    ({ model | maybeConnectionId = Just connectionId } ! [ delayUpdateMsg (WriteEvent connectionId) writeEventDelayInterval ])

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


subscriptions : Model -> Sub Msg
subscriptions model =
    let
        stopApp =
            externalStop Stop

        clientSub =
            Client.subscriptions clientConfig model.clientModel
    in
        Sub.batch [ stopApp, clientSub ]


pgConnect : DbConnectionInfo -> Model -> ( Retry.Model Msg, Cmd Msg )
pgConnect dbConnectionInfo model =
    Retry.retry retryConfig model.retryModel PGConnectError RetryConnectCmd (connectCmd dbConnectionInfo)


pgDisconnect : Maybe Int -> Cmd Msg
pgDisconnect connectionId =
    connectionId
        |?> (\pgConnectionId -> Postgres.disconnect PGDisconnectError PGDisconnect pgConnectionId False)
        ?= Cmd.none


connectCmd : DbConnectionInfo -> FailureTagger ( ConnectionId, String ) Msg -> Cmd Msg
connectCmd dbConnectionInfo failureTagger =
    Postgres.connect failureTagger
        PGConnect
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
    [ "SELECT insert_events($$($1[1], $2, '{\"target\": \"entity\",\"version\": 0,\"entityId\": \"123\",\"metadata\": {\"command\": \"asOneCmd\", \"initiatorId\": \"999888777\" },\"operation\": \"created\",\"entityName\": \"Person\"}')$$);"
    , "SELECT insert_events($$($1[1], $2, '{\"target\": \"entity\",\"version\": 0,\"entityId\": \"234\",\"metadata\": {\"command\": \"asOneCmd\", \"initiatorId\": \"999888777\" },\"operation\": \"created\",\"entityName\": \"Person\"}')$$);"
    , "SELECT insert_events($$($1[1], $2, '{\"target\": \"entity\",\"version\": 0,\"entityId\": \"234\",\"metadata\": {\"command\": \"asOneCmd\", \"initiatorId\": \"999888777\" },\"operation\": \"created\",\"entityName\": \"Person\"}')$$);"
    , "SELECT insert_events($$($1[1], $2, '{\"target\": \"entity\",\"version\": 0,\"entityId\": \"234\",\"metadata\": {\"command\": \"asOneCmd\", \"initiatorId\": \"999888777\" },\"operation\": \"created\",\"entityName\": \"Person\"}')$$);"
    , "SELECT insert_events($$($1[1], $2, '{\"target\": \"entity\",\"version\": 0,\"entityId\": \"234\",\"metadata\": {\"command\": \"asOneCmd\", \"initiatorId\": \"999888777\" },\"operation\": \"created\",\"entityName\": \"Person\"}')$$);"
    , "SELECT insert_events($$($1[1], $2, '{\"target\": \"relationship\", \"version\": 0, \"entityId\": \"123\", \"metadata\": {\"command\": \"asOneCmd\", \"initiatorId\": \"999888777\"}, \"operation\": \"added\", \"entityName\": \"Person\", \"propertyName\": \"address\"}' )$$);"
    , "SELECT insert_events($$($1[1], $2, '{\"target\": \"relationship\",\"version\": 0,\"entityId\": \"123\",\"metadata\": {\"command\": \"asOneCmd\", \"initiatorId\": \"999888777\" },\"operation\": \"added\",\"propertyName\": \"address\",\"entityName\": \"Person\"}')$$);"
    , "SELECT insert_events($$($1[1], $2, '{\"target\": \"entity\",\"version\": 0,\"entityId\": \"234\",\"metadata\": {\"command\": \"asOneCmd\", \"initiatorId\": \"999888777\" },\"operation\": \"destroyed\",\"entityName\": \"Person\"}')$$);"
    , "SELECT insert_events($$($1[1], $2, '{\"target\": \"entity\",\"version\": 0,\"entityId\": \"123\",\"metadata\": {\"command\": \"asOneCmd\", \"initiatorId\": \"999888777\" },\"operation\": \"created\",\"entityName\": \"User\"}')$$);\n"
    , "SELECT insert_events($$($1[1], $2, '{\"target\": \"entity\",\"version\": 0,\"entityId\": \"234\",\"metadata\": {\"command\": \"asOneCmd\", \"initiatorId\": \"999888777\" },\"operation\": \"destroyed\",\"entityName\": \"Person\"}')$$);"
    , "SELECT insert_events($$($1[1], $2, '{\"target\": \"entity\",\"version\": 0,\"entityId\": \"123\",\"metadata\": {\"command\": \"asOneCmd\", \"initiatorId\": \"999888777\" },\"operation\": \"created\",\"entityName\": \"User\"}')$$);"
    , "SELECT insert_events($$($1[1], $2, '{\"target\": \"entity\",\"version\": 0,\"entityId\": \"234\",\"metadata\": {\"command\": \"asOneCmd\", \"initiatorId\": \"999888777\" },\"operation\": \"created\",\"entityName\": \"Person\"}')$$);"
    , "SELECT insert_events($$($1[1], $2, '{\"target\": \"entity\",\"version\": 0,\"entityId\": \"123\",\"metadata\": {\"command\": \"asOneCmd\", \"initiatorId\": \"999888777\" },\"operation\": \"created\",\"propertyName\": \"address\",\"entityName\": \"User\"}')$$);"
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
