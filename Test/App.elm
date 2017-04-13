port module Test.App exposing (..)

{- TODO remove this when compiler is fixed -}

import Json.Decode
import Slate.DbWatcher as DbWatcher exposing (..)
import Time exposing (..)
import Task exposing (..)
import Process exposing (..)
import ParentChildUpdate exposing (..)
import DebugF
import StringUtils exposing ((+-+))
import Slate.Common.Db exposing (..)
import Utils.Error exposing (..)
import Utils.Ops exposing (..)
import Utils.Log exposing (..)
import Dict
import Slate.Engine.Query exposing (..)
import Slate.Common.Event exposing (..)


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


dbWatcherConfig : DbWatcher.Config QueryId Msg
dbWatcherConfig =
    { pgReconnectDelayInterval = pgReconnectDelayInterval
    , stopDelayInterval = stopDelayInterval
    , invalidId = (\queryId -> queryId < 0)
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
    | Stop ()
    | DbWatcherStopped


type alias Model =
    { running : Bool
    , countSubscribed : Int
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
          , countSubscribed = 0
          , dbWatcherModel = dbWatcherModel2
          }
        , Cmd.batch [ startCmd, initCmd ]
        )


init : ( Model, Cmd Msg )
init =
    initModel


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
                        DbWatcher.subscribe dbWatcherConfig model.dbWatcherModel entityEventTypes (model.countSubscribed + 1)
                            ??= (\errors -> ( model.dbWatcherModel, delayUpdateMsg (DbWatcherError ( FatalError, (String.join "," errors) )) 0 ))

                    l =
                        DebugF.log "App" "DbWatcher Started"
                in
                    ({ model | running = True, dbWatcherModel = dbWatcherModel, countSubscribed = model.countSubscribed + 1 } ! [ cmd ])

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
                    model ! [ exitApp 0 ]

            DbWatcherRefresh queryIds ->
                let
                    ( dbWatcherModel, cmd ) =
                        (model.countSubscribed < 8)
                            ? ( DbWatcher.subscribe dbWatcherConfig model.dbWatcherModel (createEntityEventTypes (model.countSubscribed + 1)) (model.countSubscribed + 1)
                                    ??= (\errors -> ( model.dbWatcherModel, delayUpdateMsg (DbWatcherError ( NonFatalError, (String.join "," errors) )) 0 ))
                              , (model.countSubscribed == 8)
                                    ? ( DbWatcher.unsubscribe dbWatcherConfig model.dbWatcherModel 4
                                            ??= (\errors -> ( model.dbWatcherModel, delayUpdateMsg (DbWatcherError ( NonFatalError, (String.join "," errors) )) 0 ))
                                      , DbWatcher.stop dbWatcherConfig model.dbWatcherModel
                                            ??= (\error -> ( model.dbWatcherModel, delayUpdateMsg (DbWatcherError ( NonFatalError, error )) 0 ))
                                      )
                              )

                    l =
                        ( DebugF.log "App " ("DbWatcher Refresh" +-+ queryIds)
                        , DebugF.log "App DbWatcher WatchedEntities" (Dict.toList dbWatcherModel.watchedEntities)
                        )
                in
                    ({ model | dbWatcherModel = dbWatcherModel, countSubscribed = model.countSubscribed + 1 }) ! [ cmd ]

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


delayUpdateMsg : Msg -> Time -> Cmd Msg
delayUpdateMsg msg delay =
    Task.perform (\_ -> msg) <| Process.sleep delay


createEntityEventTypes : Int -> List EntityEventTypes
createEntityEventTypes queryId =
    case queryId % 8 of
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
            Debug.crash "This should never happen"
