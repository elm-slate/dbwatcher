module Client exposing (..)

import Utils.Log exposing (..)
import Utils.Error exposing (..)
import Utils.Ops exposing (..)
import StringUtils exposing ((+-+))
import DebugF
import ParentChildUpdate exposing (..)
import Slate.Engine.Query exposing (..)
import Slate.Common.Db exposing (..)
import Slate.Common.Event exposing (..)
import Slate.DbWatcher.Common.Interface exposing (..)


type alias Config config model queryId dbWatcherMsg msg =
    { dbWatcherConfig : config
    , interface : Interface config model queryId dbWatcherMsg msg
    , routeToMeTagger : Msg -> msg
    }


clientInterface =
    { errorTagger = DbWatcherError
    , logTagger = DbWatcherLog
    , routeToMeTagger = DbWatcherModule
    , refreshTagger = DbWatcherRefresh
    , startedMsg = DbWatcherStarted
    , stoppedMsg = DbWatcherStopped
    }


type Msg msg
    = Nop
    | DbWatcherError ( ErrorType, String )
    | DbWatcherLog ( LogLevel, String )
    | DbWatcherModule msg
    | DbWatcherRefresh (List QueryId)
    | DbWatcherStarted
    | DbWatcherStopped


type alias Model dbWatcherModel =
    { running : Bool
    , dbWatcherModel : dbWatcherModel
    }


init config =
    let
        ( dbWatcherModel, cmd ) =
            config.interface.init config.dbWatcherConfig
    in
        ({ running = False
         , dbWatcherModel = dbWatcherModel
         }
            ! [ Cmd.map config.routeToMeTagger cmd ]
        )


start config dbConnectionInfo model =
    config.interface.start config.dbWatcherConfig dbConnectionInfo model.dbWatcherModel


subscribe config model entityEventTypesList queryId =
    config.interface.subscribe config.dbWatcherConfig model.dbWatcherModel entityEventTypesList queryId


unsubscribe config model queryId =
    config.interface.subscribe config.dbWatcherConfig model.dbWatcherModel queryId


update config msg model =
    let
        updateDbWatcher =
            updateChildParent (config.interface.update config.dbWatcherConfig) (update config) .dbWatcherModel DbWatcherModule (\model dbWatcherModel -> { model | dbWatcherModel = dbWatcherModel })
    in
        case msg of
            Nop ->
                ( model ! [], [] )

            DbWatcherError ( errorType, details ) ->
                let
                    l =
                        case errorType of
                            NonFatalError ->
                                DebugF.log ("DbWatcherError" +-+ errorType) details

                            _ ->
                                Debug.crash <| toString details
                in
                    ( model ! [], [] )

            DbWatcherLog ( logLevel, details ) ->
                let
                    l =
                        DebugF.log ("DbWatcherLog" +-+ logLevel) details
                in
                    ( model ! [], [] )

            DbWatcherStarted ->
                -- let
                --     entityEventTypes =
                --         [ ( "Person", [ ( "entity", "created", Nothing ), ( "property", "added", Just "name" ) ] ) ]
                --
                --     ( dbWatcherModel, cmd ) =
                --         DbWatcher.subscribe dbWatcherConfig model.dbWatcherModel entityEventTypes (model.countSubscribed + 1)
                --             ??= (\errors -> ( model.dbWatcherModel, delayUpdateMsg (DbWatcherError ( FatalError, (String.join "," errors) )) 0 ))
                --
                --     l =
                --         DebugF.log "App" "DbWatcher Started"
                -- in
                --     ({ model | running = True, dbWatcherModel = dbWatcherModel, countSubscribed = model.countSubscribed + 1 } ! [ cmd ])
                ( model ! [], [] )

            DbWatcherStopped ->
                let
                    l =
                        DebugF.log "App " "DbWatcher Stopped"
                in
                    ( model ! [], [] )

            --
            DbWatcherRefresh queryIds ->
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
                ( model ! [], [] )

            DbWatcherModule msg ->
                updateDbWatcher msg model
