port module Test.App exposing (..)

{- TODO remove this when compiler is fixed -}

import Json.Decode
import DbWatcher exposing (..)
import Time exposing (..)
import Task exposing (..)
import Process exposing (..)
import DbWatcher exposing (..)
import ParentChildUpdate exposing (..)
import DebugF
import StringUtils exposing ((+-+))
import Slate.Common.Db exposing (..)
import Utils.Error exposing (..)
import Utils.Ops exposing (..)
import Utils.Log exposing (..)
import Dict
import Slate.Engine.Query exposing (..)
import Slate.DbWatcher.Common.Interface exposing (..)


port exitApp : Float -> Cmd msg


port externalStop : (() -> msg) -> Sub msg



-- dbConnectionInfo : DbConnectionInfo
-- dbConnectionInfo =
--     { host = "testDbWatcherServer"
--     , port_ = 5432
--     , database = "test_dbwatcher"
--     , user = "postgres"
--     , password = "password"
--     , timeout = 15000
--     }


dbConnectionInfo : DbConnectionInfo
dbConnectionInfo =
    { host = "localPGDbServer"
    , port_ = 5432
    , database = "parallelsTest"
    , user = "parallels"
    , password = "parallelspw"
    , timeout = 15000
    }


pgReconnectDelayInterval : Time
pgReconnectDelayInterval =
    10 * second


stopDelayInterval : Time
stopDelayInterval =
    5 * second


main : Program Never Model Msg
main =
    Platform.program
        { init = init
        , update = update
        , subscriptions = subscriptions
        }


type alias Config comparable msg =
    { pgReconnectDelayInterval : Time
    , stopDelayInterval : Time
    , validateId : comparable -> Bool
    , clientInterface : ClientInterface comparable Msg msg
    }


dbWatcherConfig : DbWatcher.Config QueryId Msg
dbWatcherConfig =
    { pgReconnectDelayInterval = pgReconnectDelayInterval
    , stopDelayInterval = stopDelayInterval
    , validateId = (\queryId -> queryId < 0)
    , clientInterface =
        { errorTagger = DbWatcherError
        , logTagger = DbWatcherLog
        , routeToMeTagger = DbWatcherModule
        , refreshTagger = DbWatcherRefresh
        , startedMsg = DbWatcherStarted
        , stoppedMsg = DbWatcherStopped
        }
    }


type Msg
    = Nop
    | DbWatcherError ( ErrorType, String )
    | DbWatcherLog ( LogLevel, String )
    | DbWatcherModule DbWatcher.Msg
    | DbWatcherRefresh (List QueryId)
    | DbWatcherStarted
    | DbWatcherStop
    | SubscribeError ( QueryId, String )
    | Stop ()
    | DbWatcherStopped


type alias Model =
    { running : Bool
    , dbWatcherModel : DbWatcher.Model QueryId
    }


initModel : ( Model, Cmd Msg )
initModel =
    let
        ( dbWatcherModel1, initCmd ) =
            DbWatcher.init dbWatcherConfig

        ( dbWatcherModel2, startCmd ) =
            DbWatcher.start dbWatcherConfig dbConnectionInfo dbWatcherModel1 ??= (\error -> ( dbWatcherModel1, delayUpdateMsg (DbWatcherError ( FatalError, error )) 0 ))
    in
        ( { running = False
          , dbWatcherModel = dbWatcherModel2
          }
        , Cmd.batch [ startCmd, initCmd ]
        )


init : ( Model, Cmd Msg )
init =
    initModel


delayUpdateMsg : Msg -> Time -> Cmd Msg
delayUpdateMsg msg delay =
    Task.perform (\_ -> msg) <| Process.sleep delay


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    let
        updatePubSub =
            ParentChildUpdate.updateChildApp (DbWatcher.update dbWatcherConfig) update .dbWatcherModel DbWatcherModule (\model dbWatcherModel -> { model | dbWatcherModel = dbWatcherModel })
    in
        case msg of
            Nop ->
                model ! []

            DbWatcherError ( errorType, details ) ->
                let
                    l =
                        case errorType of
                            NonFatalError ->
                                DebugF.log "DbWatcherError" details

                            _ ->
                                Debug.crash <| toString details
                in
                    model ! []

            DbWatcherLog ( logLevel, details ) ->
                let
                    l =
                        DebugF.log "DbWatcherLog" (toString logLevel ++ ":" +-+ details)
                in
                    model ! []

            DbWatcherStarted ->
                let
                    entityEventTypes =
                        [ ( "Person", [ ( "entity", "created", Nothing ), ( "property", "added", Just "name" ) ] ) ]

                    ( dbWatcherModel, cmd ) =
                        DbWatcher.subscribe dbWatcherConfig model.dbWatcherModel entityEventTypes 1 SubscribeError
                            ??= (\errors -> ( model.dbWatcherModel, delayUpdateMsg (DbWatcherError ( FatalError, (String.join "," errors) )) 0 ))

                    l =
                        DebugF.log "App" "DbWatcher Started"

                    ll =
                        DebugF.log "DbWatcher Dict" (Dict.toList dbWatcherModel.watchedEntities)
                in
                    ({ model | running = True } ! [ cmd ])

            DbWatcherStop ->
                let
                    l =
                        DebugF.log "App " "DbWatcher Stop"
                in
                    model ! []

            DbWatcherStopped ->
                let
                    l =
                        DebugF.log "App " "DbWatcher Stopped"
                in
                    model ! []

            DbWatcherRefresh queryIds ->
                let
                    l =
                        DebugF.log "App " ("DbWatcher Refresh" +-+ queryIds)
                in
                    model ! []

            SubscribeError ( queryId, error ) ->
                let
                    l =
                        DebugF.log "SubscribeError" ("queryId:" +-+ queryId +-+ "error:" +-+ error)
                in
                    model ! [ exitApp 0 ]

            Stop _ ->
                let
                    l =
                        DebugF.log "App " "Stop"
                in
                    model ! [ exitApp 0 ]

            DbWatcherModule msg ->
                updatePubSub msg model


subscriptions : Model -> Sub Msg
subscriptions model =
    let
        stopApp =
            externalStop Stop

        dbWatcherSub =
            DbWatcher.elmSubscriptions dbWatcherConfig model.dbWatcherModel
    in
        model.running ? ( Sub.batch [ stopApp, dbWatcherSub ], stopApp )
