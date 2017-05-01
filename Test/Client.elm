module Client exposing (..)

import Time exposing (..)
import Task exposing (..)
import Process exposing (..)
import Utils.Log exposing (..)
import Utils.Error exposing (..)
import Utils.Ops exposing (..)
import StringUtils exposing ((+-+))
import DebugF
import ParentChildUpdate exposing (..)
import Slate.Common.Db exposing (..)
import Slate.DbWatcher.Common.Interface exposing (..)


type alias Config dbWatcherConfig dbWatcherModel dbWatcherId dbWatcherMsg msg =
    { dbWatcherConfig : dbWatcherConfig
    , interface : Interface dbWatcherConfig dbWatcherModel dbWatcherId dbWatcherMsg (Msg dbWatcherId dbWatcherMsg)
    , routeToMeTagger : Msg dbWatcherId dbWatcherMsg -> msg
    , startWritingMsg : msg
    }


clientInterface : ClientInterface dbWatcherId dbWatcherMsg (Msg dbWatcherId dbWatcherMsg)
clientInterface =
    { errorTagger = DbWatcherError
    , logTagger = DbWatcherLog
    , routeToMeTagger = DbWatcherMsg
    , refreshTagger = DbWatcherRefresh
    , startedMsg = DbWatcherStarted
    , stoppedMsg = DbWatcherStopped
    }


type Msg dbWatcherId dbWatcherMsg
    = DbWatcherError ( ErrorType, String )
    | DbWatcherLog ( LogLevel, String )
    | DbWatcherMsg dbWatcherMsg
    | DbWatcherRefresh (List dbWatcherId)
    | DbWatcherStarted
    | DbWatcherStopped


type alias Model dbWatcherModel =
    { running : Bool
    , dbWatcherModel : dbWatcherModel
    , countSubscribed : Int
    }


init : Config dbWatcherConfig dbWatcherModel dbWatcherId dbWatcherMsg msg -> DbConnectionInfo -> ( Model dbWatcherModel, Cmd msg )
init config dbConnectionInfo =
    config.interface.init config.dbWatcherConfig
        |> (\( dbWatcherModel, initCmd ) ->
                let
                    model =
                        { running = False, dbWatcherModel = dbWatcherModel, countSubscribed = 0 }
                in
                    config.interface.start config.dbWatcherConfig dbConnectionInfo model.dbWatcherModel
                        |??> (\( dbWatcherModel, startCmd ) -> ( { model | dbWatcherModel = dbWatcherModel }, Cmd.map config.routeToMeTagger <| Cmd.batch [ startCmd, initCmd ] ))
                        ??= (\error -> ( { model | dbWatcherModel = dbWatcherModel }, Cmd.map config.routeToMeTagger initCmd ))
           )


update : Config dbWatcherConfig dbWatcherModel dbWatcherId dbWatcherMsg msg -> Msg dbWatcherId dbWatcherMsg -> Model dbWatcherModel -> ( ( Model dbWatcherModel, Cmd (Msg dbWatcherId dbWatcherMsg) ), List msg )
update config msg model =
    let
        updateDbWatcher =
            updateChildParent (config.interface.update config.dbWatcherConfig) (update config) .dbWatcherModel DbWatcherMsg (\model dbWatcherModel -> { model | dbWatcherModel = dbWatcherModel })
    in
        case msg of
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
                let
                    entityEventTypes =
                        [ ( "Person", [ ( "entity", "created", Nothing ), ( "property", "added", Just "name" ) ] ) ]

                    l =
                        DebugF.log "Client" "DbWatcherStarted"
                in
                    -- TODO need to fix dbWatcherId value in subscribe
                    -- config.interface.subscribe config.dbWatcherConfig model.dbWatcherModel entityEventTypes (model.countSubscribed + 1)
                    --     ??= (\errors -> ( model.dbWatcherModel, delayUpdateMsg (DbWatcherError ( FatalError, (String.join "," errors) )) 0 ))
                    --     |> (\( dbWatcherModel, cmd ) -> ( { model | running = True, dbWatcherModel = dbWatcherModel, countSubscribed = model.countSubscribed + 1 } ! [], [] ))
                    ( model ! [], [] )

            DbWatcherStopped ->
                let
                    l =
                        DebugF.log "Client " "DbWatcherStopped"
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

            DbWatcherMsg msg ->
                let
                    l =
                        DebugF.log "Client " ("DbWatcherMsg" +-+ msg)
                in
                    updateDbWatcher msg model


subscriptions : Model dbWatcherModel -> Sub Msg
subscriptions model =
    -- let
    --     -- dbWatcherSub =
    --     --     interface.elmSubscriptions dbWatcherConfig model.dbWatcherModel
    -- in
    Sub.none


delayUpdateMsg : Msg dbWatcherId dbWatcherIdMsg -> Time -> Cmd (Msg dbWatcherId dbWatcherIdMsg)
delayUpdateMsg msg delay =
    Task.perform (\_ -> msg) <| Process.sleep delay
