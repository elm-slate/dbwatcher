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
import Slate.Common.Event exposing (..)
import Slate.DbWatcher.Common.Interface exposing (..)


type alias Config dbWatcherConfig dbWatcherModel dbWatcherId dbWatcherMsg msg =
    { dbWatcherConfig : dbWatcherConfig
    , interface : Interface dbWatcherConfig dbWatcherModel dbWatcherId dbWatcherMsg (Msg dbWatcherId dbWatcherMsg)
    , routeToMeTagger : Msg dbWatcherId dbWatcherMsg -> msg
    , startWritingMsg : msg
    , stopWritingMsg : msg
    , stopMsg : msg
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


type alias Model dbWatcherId dbWatcherModel =
    { running : Bool
    , dbWatcherModel : dbWatcherModel
    , testIds : List dbWatcherId
    , countSubscribedOrStopped : Int
    }


init : Config dbWatcherConfig dbWatcherModel dbWatcherId dbWatcherMsg msg -> DbConnectionInfo -> List dbWatcherId -> ( Model dbWatcherId dbWatcherModel, Cmd msg )
init config dbConnectionInfo testIds =
    config.interface.init config.dbWatcherConfig
        |> (\( dbWatcherModel, initCmd ) ->
                let
                    model =
                        { running = False, dbWatcherModel = dbWatcherModel, testIds = testIds, countSubscribedOrStopped = 0 }
                in
                    config.interface.start config.dbWatcherConfig dbConnectionInfo model.dbWatcherModel
                        |??> (\( dbWatcherModel, startCmd ) -> ( { model | dbWatcherModel = dbWatcherModel }, Cmd.map config.routeToMeTagger <| Cmd.batch [ startCmd, initCmd ] ))
                        ??= (\error -> ( { model | dbWatcherModel = dbWatcherModel }, Cmd.map config.routeToMeTagger initCmd ))
           )


update : Config dbWatcherConfig dbWatcherModel dbWatcherId dbWatcherMsg msg -> Msg dbWatcherId dbWatcherMsg -> Model dbWatcherId dbWatcherModel -> ( ( Model dbWatcherId dbWatcherModel, Cmd (Msg dbWatcherId dbWatcherMsg) ), List msg )
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
                let
                    l =
                        DebugF.log "Client" "DbWatcherStarted"
                in
                    subscribeOrStop config model

            DbWatcherStopped ->
                let
                    l =
                        DebugF.log "Client" "DbWatcherStopped"
                in
                    ( model ! [], [ config.stopMsg ] )

            DbWatcherRefresh queryIds ->
                let
                    l =
                        DebugF.log "Client " ("DbWatcherRefresh" +-+ queryIds)
                in
                    subscribeOrStop config model

            DbWatcherMsg msg ->
                updateDbWatcher msg model


subscriptions : Config dbWatcherConfig dbWatcherModel dbWatcherId dbWatcherMsg msg -> Model dbWatcherId dbWatcherModel -> Sub msg
subscriptions config model =
    Sub.map config.routeToMeTagger <| config.interface.elmSubscriptions config.dbWatcherConfig model.dbWatcherModel


subscribeOrStop : Config dbWatcherConfig dbWatcherModel dbWatcherId dbWatcherMsg msg -> Model dbWatcherId dbWatcherModel -> ( ( Model dbWatcherId dbWatcherModel, Cmd (Msg dbWatcherId dbWatcherMsg) ), List msg )
subscribeOrStop config model =
    model.countSubscribedOrStopped
        + 1
        |> (\countSubscribedOrStopped ->
                case model.testIds of
                    [] ->
                        ( Nothing, [], (createEntityEventTypes model.countSubscribedOrStopped), countSubscribedOrStopped )

                    testId :: _ ->
                        ( List.head model.testIds, List.drop 1 model.testIds, (createEntityEventTypes model.countSubscribedOrStopped), countSubscribedOrStopped )
           )
        |> (\( maybeTestId, testIds, entityEventTypes, countSubscribedOrStopped ) ->
                maybeTestId
                    |?> (\testId ->
                            config.interface.subscribe config.dbWatcherConfig model.dbWatcherModel entityEventTypes testId
                                |??> (\( dbWatcherModel, cmd ) -> ( ( { model | running = True, dbWatcherModel = dbWatcherModel, testIds = testIds, countSubscribedOrStopped = countSubscribedOrStopped }, cmd ), (model.countSubscribedOrStopped == 0) ? ( [ config.startWritingMsg ], [] ) ))
                                ??= (\errors -> ( ( { model | testIds = testIds, countSubscribedOrStopped = countSubscribedOrStopped }, delayUpdateMsg (DbWatcherError ( NonFatalError, (String.join "," errors) )) 0 ), [] ))
                        )
                    ?= (config.interface.stop config.dbWatcherConfig model.dbWatcherModel
                            |??> (\( dbWatcherModel, cmd ) -> ( ( { model | running = False, dbWatcherModel = dbWatcherModel, testIds = testIds, countSubscribedOrStopped = countSubscribedOrStopped }, cmd ), [ config.stopWritingMsg ] ))
                            ??= (\error -> ( ( { model | running = False, countSubscribedOrStopped = countSubscribedOrStopped, testIds = testIds }, delayUpdateMsg (DbWatcherError ( NonFatalError, error )) 0 ), [] ))
                       )
           )


delayUpdateMsg : Msg dbWatcherId dbWatcherIdMsg -> Time -> Cmd (Msg dbWatcherId dbWatcherIdMsg)
delayUpdateMsg msg delay =
    Task.perform (\_ -> msg) <| Process.sleep delay


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
