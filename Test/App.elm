port module Test.App exposing (..)

{- TODO remove this when compiler is fixed -}

import Json.Decode
import Client exposing (..)
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
    { host = "localpgdbserver"
    , port_ = 5432
    , database = "parallelsTest"
    , user = "parallels"
    , password = "parallelspw"
    , timeout = 15000
    }



-- dbConnectionInfo : DbConnectionInfo
-- dbConnectionInfo =
--     { host = "testDbWatcherServer"
--     , port_ = 5432
--     , database = "test_dbwatcher"
--     , user = "postgres"
--     , password = "password"
--     , timeout = 15000
--     }


pgReconnectDelayInterval : Time
pgReconnectDelayInterval =
    10 * second


stopDelayInterval : Time
stopDelayInterval =
    5 * second


clientConfig =
    { dbWatcherConfig = dbWatcherConfig
    , interface = DbWatcher.interface
    , routeToMeTagger = ClientModule
    }


dbWatcherConfig =
    { pgReconnectDelayInterval = pgReconnectDelayInterval
    , stopDelayInterval = stopDelayInterval
    , invalidId = (\queryId -> queryId < 0)
    , clientInterface = Client.clientInterface
    }


main : Program Never (Model) Msg
main =
    Platform.program
        { init = init
        , update = update
        , subscriptions = subscriptions
        }


type Msg
    = Nop
    | ClientError ( ErrorType, String )
    | ClientLog ( LogLevel, String )
    | ClientModule (Client.Msg DbWatcher.Msg)
    | ClientRefresh (List QueryId)
    | ClientStarted
    | ClientStop
    | Stop ()
    | ClientStopped


type alias Model =
    { running : Bool
    , clientModel : Client.Model (DbWatcher.Model QueryId)
    }


initModel : ( Model, Cmd Msg )
initModel =
    let
        ( clientModel, cmd ) =
            Client.init clientConfig

        ( clientModel2, cmd2 ) =
            Client.start clientConfig dbConnectionInfo clientModel
    in
        ( { running = False
          , clientModel = clientModel
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
            ParentChildUpdate.updateChildApp (Client.update clientConfig) update .clientModel ClientModule (\model clientModel -> { model | clientModel = clientModel })
    in
        case msg of
            Nop ->
                model ! []

            ClientError ( errorType, details ) ->
                let
                    l =
                        case errorType of
                            NonFatalError ->
                                DebugF.log ("ClientError" +-+ errorType) details

                            _ ->
                                Debug.crash <| toString details
                in
                    model ! []

            ClientLog ( logLevel, details ) ->
                let
                    l =
                        DebugF.log ("ClientLog" +-+ logLevel) details
                in
                    model ! []

            ClientStarted ->
                -- let
                --     entityEventTypes =
                --         [ ( "Person", [ ( "entity", "created", Nothing ), ( "property", "added", Just "name" ) ] ) ]
                --
                --     ( dbWatcherModel, cmd ) =
                --         DbWatcher.subscribe dbWatcherConfig model.dbWatcherModel entityEventTypes (model.countSubscribed + 1)
                --             ??= (\errors -> ( model.dbWatcherModel, delayUpdateMsg (DbWatcherError ( FatalError, (String.join "," errors) )) 0 ))
                --
                --     l =
                --         DebugF.log "App" "Client Started"
                -- in
                --     ({ model | running = True, dbWatcherModel = dbWatcherModel, countSubscribed = model.countSubscribed + 1 } ! [ cmd ])
                let
                    l =
                        DebugF.log "App" "ClientStarted"
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
                    model ! [ exitApp 0 ]

            ClientRefresh queryIds ->
                -- let
                --     ( dbWatcherModel, cmd ) =
                --         (model.countSubscribed < 8)
                --             ? ( interface.subscribe dbWatcherConfig model.dbWatcherModel (createEntityEventTypes (model.countSubscribed + 1)) (model.countSubscribed + 1)
                --                     ??= (\errors -> ( model.dbWatcherModel, delayUpdateMsg (DbWatcherError ( NonFatalError, (String.join "," errors) )) 0 ))
                --               , (model.countSubscribed == 8)
                --                     ? ( DbWatcher.unsubscribe dbWatcherConfig model.dbWatcherModel 4
                --                             ??= (\errors -> ( model.dbWatcherModel, delayUpdateMsg (DbWatcherError ( NonFatalError, (String.join "," errors) )) 0 ))
                --                       , DbWatcher.stop dbWatcherConfig model.dbWatcherModel
                --                             ??= (\error -> ( model.dbWatcherModel, delayUpdateMsg (DbWatcherError ( NonFatalError, error )) 0 ))
                --                       )
                --               )
                --
                --     l =
                --         ( DebugF.log "App " ("DbWatcher Refresh" +-+ queryIds)
                --         , DebugF.log "App DbWatcher WatchedEntities" (Dict.toList dbWatcherModel.watchedEntities)
                --         )
                -- in
                --     ({ model | dbWatcherModel = dbWatcherModel, countSubscribed = model.countSubscribed + 1 }) ! [ cmd ]
                let
                    l =
                        DebugF.log "App" "ClientRefresh"
                in
                    model ! []

            Stop _ ->
                let
                    l =
                        DebugF.log "App" "Stop"
                in
                    model ! [ exitApp 0 ]

            ClientModule msg ->
                let
                    l =
                        DebugF.log "App" "ClientModule"
                in
                    updateClient msg model


subscriptions : Model -> Sub Msg
subscriptions model =
    let
        stopApp =
            externalStop Stop

        -- dbWatcherSub =
        --     interface.elmSubscriptions dbWatcherConfig model.dbWatcherModel
    in
        Sub.none



-- model.running ? ( Sub.batch [ stopApp, dbWatcherSub ], stopApp )


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
