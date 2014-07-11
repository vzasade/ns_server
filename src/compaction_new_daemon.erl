-module(compaction_new_daemon).

-behaviour(gen_server).

-include("ns_common.hrl").
-include("couch_db.hrl").

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-record(compaction_state, {buckets_to_compact :: [binary()],
                           compactor_pid :: pid(),
                           scheduler,
                           compactor_fun :: fun(),
                           compactor_config}).

-record(state, {kv_compaction :: #compaction_state{},
                views_compaction :: #compaction_state{},
                %% mapping from compactor pid to #forced_compaction{}
                running_forced_compactions,
                %% reverse mapping from #forced_compaction{} to compactor pid
                forced_compaction_pids}).

-record(config, {db_fragmentation,
                 view_fragmentation,
                 allowed_period,
                 parallel_view_compact = false,
                 do_purge = false,
                 daemon}).

-record(daemon_config, {check_interval,
                        min_file_size}).

-record(period, {from_hour,
                 from_minute,
                 to_hour,
                 to_minute,
                 abort_outside}).

-record(forced_compaction, {type :: bucket | bucket_purge | db | view,
                            name :: binary()}).

%% API
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


get_last_rebalance_or_failover_timestamp() ->
    case ns_config:search_raw(ns_config:get(), counters) of
        {value, [{'_vclock', VClock} | _]} ->
            GregorianSecondsTS = vclock:get_latest_timestamp(VClock),
            GregorianSecondsTS - calendar:datetime_to_gregorian_seconds({{1970, 1, 1}, {0, 0, 0}});
        _ -> 0
    end.

%% gen_server callbacks
init([]) ->
    process_flag(trap_exit, true),

    Self = self(),

    ns_pubsub:subscribe_link(
      ns_config_events,
      fun ({buckets, _}) ->
              Self ! config_changed;
          ({autocompaction, _}) ->
              Self ! config_changed;
          ({{node, Node, compaction_daemon}, _}) when node() =:= Node ->
              Self ! config_changed;
          (_) ->
              ok
      end),

    CheckInterval = get_check_interval(ns_config:get()),

    {ok, #state{kv_compaction =
                    init_scheduled_compaction(CheckInterval, compact_kv,
                                              fun spawn_scheduled_kv_compactor/2),
                views_compaction =
                    init_scheduled_compaction(CheckInterval, compact_views,
                                              fun spawn_scheduled_views_compactor/2),
                running_forced_compactions=dict:new(),
                forced_compaction_pids=dict:new()}}.

handle_cast(Message, State) ->
    ?log_warning("Got unexpected cast ~p:~n~p", [Message, State]),
    {noreply, State}.

handle_call({force_compact_bucket, Bucket}, _From, State) ->
    Compaction = #forced_compaction{type=bucket, name=Bucket},

    NewState =
        case is_already_being_compacted(Compaction, State) of
            true ->
                State;
            false ->
                Configs = compaction_config(Bucket),
                Pid = spawn_forced_bucket_compactor(Bucket, Configs),
                register_forced_compaction(Pid, Compaction, State)
        end,
    {reply, ok, NewState};
handle_call({force_purge_compact_bucket, Bucket}, _From, State) ->
    Compaction = #forced_compaction{type=bucket_purge, name=Bucket},

    NewState =
        case is_already_being_compacted(Compaction, State) of
            true ->
                State;
            false ->
                {Config, Props} = compaction_config(Bucket),
                Pid = spawn_forced_bucket_compactor(Bucket,
                                                    {Config#config{do_purge = true}, Props}),
                State1 = register_forced_compaction(Pid, Compaction, State),
                ale:info(?USER_LOGGER,
                         "Started deletion purge compaction for bucket `~s`",
                         [Bucket]),
                State1
        end,
    {reply, ok, NewState};
handle_call({force_compact_db_files, Bucket}, _From, State) ->
    Compaction = #forced_compaction{type=db, name=Bucket},

    NewState =
        case is_already_being_compacted(Compaction, State) of
            true ->
                State;
            false ->
                {Config, _} = compaction_config(Bucket),
                OriginalTarget = {[{type, db}]},
                Pid = spawn_dbs_compactor(Bucket, Config, true, OriginalTarget),
                register_forced_compaction(Pid, Compaction, State)
        end,
    {reply, ok, NewState};
handle_call({force_compact_view, Bucket, DDocId}, _From, State) ->
    Name = <<Bucket/binary, $/, DDocId/binary>>,
    Compaction = #forced_compaction{type=view, name=Name},

    NewState =
        case is_already_being_compacted(Compaction, State) of
            true ->
                State;
            false ->
                {Config, _} = compaction_config(Bucket),
                OriginalTarget = {[{type, view},
                                   {name, DDocId}]},
                Pid = spawn_view_compactor(Bucket, DDocId, Config, true,
                                           OriginalTarget),
                register_forced_compaction(Pid, Compaction, State)
        end,
    {reply, ok, NewState};
handle_call({cancel_forced_bucket_compaction, Bucket}, _From, State) ->
    Compaction = #forced_compaction{type=bucket, name=Bucket},
    {reply, ok, maybe_cancel_compaction(Compaction, State)};
handle_call({cancel_forced_db_compaction, Bucket}, _From, State) ->
    Compaction = #forced_compaction{type=db, name=Bucket},
    {reply, ok, maybe_cancel_compaction(Compaction, State)};
handle_call({cancel_forced_view_compaction, Bucket, DDocId}, _From, State) ->
    Name = <<Bucket/binary, $/, DDocId/binary>>,
    Compaction = #forced_compaction{type=view, name=Name},
    {reply, ok, maybe_cancel_compaction(Compaction, State)};
handle_call(Event, _From, State) ->
    ?log_warning("Got unexpected call ~p:~n~p", [Event, State]),
    {reply, ok, State}.

handle_info(compact_kv, #state{kv_compaction=CompactionState} = State) ->
    {noreply, State#state{kv_compaction=
                              process_scheduler_message(CompactionState)}};
handle_info(compact_views, #state{views_compaction=CompactionState} = State) ->
    {noreply, State#state{views_compaction=
                              process_scheduler_message(CompactionState)}};
handle_info({'EXIT', Compactor, Reason},
            #state{kv_compaction=#compaction_state{compactor_pid=Compactor}=
                       CompactorState} = State) ->
    {noreply, State#state{kv_compaction=
                              process_compactors_exit(Reason, CompactorState)}};
handle_info({'EXIT', Compactor, Reason},
            #state{views_compaction=#compaction_state{compactor_pid=Compactor}=
                       CompactorState} = State) ->
    {noreply, State#state{views_compaction=
                              process_compactors_exit(Reason, CompactorState)}};
handle_info({'EXIT', Pid, Reason},
            #state{running_forced_compactions=Compactions} = State) ->
    case dict:find(Pid, Compactions) of
        {ok, #forced_compaction{type=Type, name=Name}} ->
            case Reason of
                normal ->
                    case Type of
                        bucket_purge ->
                            ale:info(?USER_LOGGER,
                                     "Purge deletion compaction of bucket `~s` completed",
                                     [Name]);
                        _ ->
                            ale:info(?USER_LOGGER,
                                     "User-triggered compaction of ~p `~s` completed.",
                                     [Type, Name])
                    end;
                _ ->
                    case Type of
                        bucket_purge ->
                            ale:info(?USER_LOGGER,
                                     "Purge deletion compaction of bucket `~s` failed: ~p. "
                                     "See logs for detailed reason.",
                                     [Name, Reason]);
                        _ ->
                            ale:error(?USER_LOGGER,
                                      "User-triggered compaction of ~p `~s` failed: ~p. "
                                      "See logs for detailed reason.",
                                      [Type, Name, Reason])
                    end
            end,

            {noreply, unregister_forced_compaction(Pid, State)};
        error ->
            ?log_error("Unexpected process termination ~p: ~p. Dying.",
                       [Pid, Reason]),
            {stop, Reason, State}
    end;
handle_info(config_changed,
            #state{kv_compaction = KVCompaction,
                   views_compaction = ViewsCompaction} = State) ->
    misc:flush(config_changed),
    Config = ns_config:get(),

    {noreply, State#state{kv_compaction = process_config_change(Config, KVCompaction),
                          views_compaction = process_config_change(Config, ViewsCompaction)}};
handle_info(Info, State) ->
    ?log_warning("Got unexpected message ~p:~n~p", [Info, State]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

get_check_interval(Config) ->
    #daemon_config{check_interval=CheckInterval} = compaction_daemon_config(Config),
    CheckInterval.

%% Internal functions
get_dbs_compactor(BucketName, Config, Force) ->
    OriginalTarget = {[{type, bucket}]},
    [{type, database},
     {important, true},
     {name, BucketName},
     {fa, {fun spawn_dbs_compactor/4,
           [BucketName, Config, Force, OriginalTarget]}}].

-spec spawn_scheduled_kv_compactor(binary(), {#config{}, list()}) -> pid().
spawn_scheduled_kv_compactor(BucketName, {Config, ConfigProps}) ->
    proc_lib:spawn_link(
      fun () ->
              check_period_and_maybe_exit(Config),
              check_bucket_exists_and_maybe_exit(BucketName),

              ?log_info("Start compaction of vbuckets for bucket ~s with config: ~n~p",
                        [BucketName, ConfigProps]),

              Compactor = get_dbs_compactor(BucketName, Config, false),
              misc:wait_for_process(chain_compactors([Compactor]), infinity)
      end).

get_view_compactors(BucketName, DDocNames, Config, Force) ->
    OriginalTarget = {[{type, bucket}]},
    [ [{type, view},
       {name, <<BucketName/binary, $/, DDocId/binary>>},
       {important, false},
       {fa, {fun spawn_view_compactor/5,
             [BucketName, DDocId, Config, Force =:= true, OriginalTarget]}}] ||
        DDocId <- DDocNames ].

-spec spawn_scheduled_views_compactor(binary(), {#config{}, list()}) -> pid().
spawn_scheduled_views_compactor(BucketName, {Config, ConfigProps}) ->
    proc_lib:spawn_link(
      fun () ->
              check_period_and_maybe_exit(Config),
              check_bucket_exists_and_maybe_exit(BucketName),

              try_to_cleanup_indexes(BucketName),

              ?log_info("Start compaction of indexes for bucket ~s with config: ~n~p",
                        [BucketName, ConfigProps]),

              DDocNames = ddoc_names(BucketName),
              DDocNames =/= [] orelse exit(normal),

              Compactors = get_view_compactors(BucketName, DDocNames, Config, false),
              misc:wait_for_process(chain_compactors(Compactors), infinity)
      end).

-spec spawn_forced_bucket_compactor(binary(), {#config{}, list()}) -> pid().
spawn_forced_bucket_compactor(BucketName, {Config, ConfigProps}) ->
    proc_lib:spawn_link(
      fun () ->
              check_bucket_exists_and_maybe_exit(BucketName),

              try_to_cleanup_indexes(BucketName),

              ?log_info("Start forced compaction of bucket ~s with config: ~n~p",
                        [BucketName, ConfigProps]),

              DDocNames = ddoc_names(BucketName),

              DbsCompactor = get_dbs_compactor(BucketName, Config, true),
              DDocCompactors = get_view_compactors(BucketName, DDocNames, Config, true),

              case Config#config.parallel_view_compact of
                  true ->
                      DbsPid = chain_compactors([DbsCompactor]),
                      ViewsPid = chain_compactors(DDocCompactors),

                      misc:wait_for_process(DbsPid, infinity),
                      misc:wait_for_process(ViewsPid, infinity);
                  false ->
                      AllCompactors = [DbsCompactor | DDocCompactors],
                      Pid = chain_compactors(AllCompactors),
                      misc:wait_for_process(Pid, infinity)
              end
      end).

try_to_cleanup_indexes(BucketName) ->
    ?log_info("Cleaning up indexes for bucket `~s`", [BucketName]),

    try
        couch_set_view:cleanup_index_files(mapreduce_view, BucketName)
    catch SetViewT:SetViewE ->
            ?log_error("Error while doing cleanup of old "
                       "index files for bucket `~s`: ~p~n~p",
                       [BucketName, {SetViewT, SetViewE}, erlang:get_stacktrace()])
    end,

    try
        capi_spatial:cleanup_spatial_index_files(BucketName)
    catch SpatialT:SpatialE ->
            ?log_error("Error while doing cleanup of old "
                       "spatial index files for bucket `~s`: ~p~n~p",
                       [BucketName, {SpatialT, SpatialE}, erlang:get_stacktrace()])
    end,

    %% we still use ordinary views for development subset
    MasterDbName = db_name(BucketName, <<"master">>),
    case couch_db:open_int(MasterDbName, []) of
        {ok, Db} ->
            try
                couch_view:cleanup_index_files(Db)
            catch
                ViewE:ViewT ->
                    ?log_error(
                       "Error while doing cleanup of old "
                       "index files for database `~s`: ~p~n~p",
                       [MasterDbName, {ViewT, ViewE}, erlang:get_stacktrace()])
            after
                couch_db:close(Db)
            end;
        Error ->
            ?log_error("Failed to open database `~s`: ~p",
                       [MasterDbName, Error])
    end.

chain_compactors(Compactors) ->
    Parent = self(),

    proc_lib:spawn_link(
      fun () ->
              process_flag(trap_exit, true),
              do_chain_compactors(Parent, Compactors)
      end).

do_chain_compactors(_Parent, []) ->
    ok;
do_chain_compactors(Parent, [Compactor | Compactors]) ->
    {Fun, Args} = proplists:get_value(fa, Compactor),
    Important = proplists:get_value(important, Compactor, true),
    Pid = erlang:apply(Fun, Args),

    receive
        {'EXIT', Parent, Reason} = Exit ->
            ?log_debug("Got exit signal from parent: ~p", [Exit]),
            exit(Pid, Reason),
            misc:wait_for_process(Pid, infinity),
            exit(Reason);
        {'EXIT', Pid, normal} ->
            case proplists:get_value(on_success, Compactor) of
                undefined ->
                    ok;
                {SuccessFun, SuccessArgs} ->
                    erlang:apply(SuccessFun, SuccessArgs)
            end,

            do_chain_compactors(Parent, Compactors);
        {'EXIT', Pid, Reason} ->
            Type = proplists:get_value(type, Compactor),
            Name = proplists:get_value(name, Compactor),

            case Important of
                true ->
                    ?log_warning("Compactor for ~p `~s` (pid ~p) terminated "
                                 "unexpectedly: ~p",
                                 [Type, Name, Compactor, Reason]),
                    exit(Reason);
                false ->
                    ?log_warning("Compactor for ~p `~s` (pid ~p) terminated "
                                 "unexpectedly (ignoring this): ~p",
                                 [Type, Name, Compactor, Reason])
            end
    end.

spawn_dbs_compactor(BucketName, Config, Force, OriginalTarget) ->
    Parent = self(),

    proc_lib:spawn_link(
      fun () ->
              VBucketDbs = all_bucket_dbs(BucketName),
              Total = length(VBucketDbs),

              DoCompact =
                  case Force of
                      true ->
                          ?log_info("Forceful compaction of bucket ~s requested",
                                    [BucketName]),
                          true;
                      false ->
                          bucket_needs_compaction(BucketName, Total - 1, Config)
                  end,

              DoCompact orelse exit(normal),

              ?log_info("Compacting databases for bucket ~s", [BucketName]),

              TriggerType = case Force of
                                true ->
                                    manual;
                                false ->
                                    scheduled
                            end,

              ok = couch_task_status:add_task(
                     [{type, bucket_compaction},
                      {original_target, OriginalTarget},
                      {trigger_type, TriggerType},
                      {bucket, BucketName},
                      {vbuckets_done, 0},
                      {total_vbuckets, Total},
                      {progress, 0}]),

              {Options, SafeViewSeqs} =
                  case Config#config.do_purge of
                      true ->
                          {{0, 0, true}, []};
                      _ ->
                          {NowMegaSec, NowSec, _} = os:timestamp(),
                          NowEpoch = NowMegaSec * 1000000 + NowSec,

                          Interval0 = compaction_api:get_purge_interval(BucketName) * 3600 * 24,
                          Interval = erlang:round(Interval0),

                          PurgeTS0 = NowEpoch - Interval,

                          RebTS = get_last_rebalance_or_failover_timestamp(),

                          PurgeTS = case RebTS > PurgeTS0 of
                                        true ->
                                            RebTS - Interval;
                                        false ->
                                            PurgeTS0
                                    end,

                          {{PurgeTS, 0, false}, capi_set_view_manager:get_safe_purge_seqs(BucketName)}
                  end,



              Compactors =
                  [ [{type, vbucket},
                     {name, element(2, VBucketAndDbName)},
                     {important, false},
                     {fa, {fun spawn_vbucket_compactor/5,
                           [BucketName, VBucketAndDbName, Config, Force,
                            make_per_vbucket_compaction_options(Options, VBucketAndDbName, SafeViewSeqs)]}},
                     {on_success,
                      {fun update_bucket_compaction_progress/2, [Ix, Total]}}] ||
                      {Ix, VBucketAndDbName} <- misc:enumerate(VBucketDbs) ],

              process_flag(trap_exit, true),
              do_chain_compactors(Parent, Compactors),

              ?log_info("Finished compaction of databases for bucket ~s",
                        [BucketName])
      end).

make_per_vbucket_compaction_options({TS, 0, DD} = GeneralOption, {Vb, _}, SafeViewSeqs) ->
    case lists:keyfind(Vb, 1, SafeViewSeqs) of
        false ->
            GeneralOption;
        {_, Seq} ->
            {TS, Seq, DD}
    end.

update_bucket_compaction_progress(Ix, Total) ->
    Progress = (Ix * 100) div Total,
    ok = couch_task_status:update(
           [{vbuckets_done, Ix},
            {progress, Progress}]).

open_db(BucketName, {VBucket, DbName}) ->
    case couch_db:open_int(DbName, []) of
        {ok, Db} ->
            {ok, Db};
        {not_found, no_db_file} = NotFoundError ->
            %% It can be a real error which we don't want to hide. But at the
            %% same time it can be a result of, for instance, rebalance. In
            %% the second case we can expect many databases to be missing. So
            %% we don't want all the resulting harmless errors be spamming the
            %% log. To handle this we will check if the vbucket corresponding
            %% to the database still belongs to our node according to vbucket
            %% map. And if it's not the case we'll indicate that the error can
            %% be ignored.
            case is_integer(VBucket) of
                true ->
                    BucketConfig = get_bucket(BucketName),
                    NodeVBuckets = ns_bucket:all_node_vbuckets(BucketConfig),

                    case ordsets:is_element(VBucket, NodeVBuckets) of
                        true ->
                            %% this is a real error we want to report
                            NotFoundError;
                        false ->
                            vbucket_moved
                    end;
                false ->
                    NotFoundError
            end;
         Error ->
            Error
    end.

open_db_or_fail(BucketName, {_, DbName} = VBucketAndDbName) ->
    case open_db(BucketName, VBucketAndDbName) of
        {ok, Db} ->
            Db;
        vbucket_moved ->
            exit(normal);
        Error ->
            ?log_error("Failed to open database `~s`: ~p", [DbName, Error]),
            exit({open_db_failed, Error})
    end.

vbucket_needs_compaction({DataSize, FileSize}, Config) ->
    #config{daemon=#daemon_config{min_file_size=MinFileSize},
            db_fragmentation=FragThreshold} = Config,

    file_needs_compaction(DataSize, FileSize, FragThreshold, MinFileSize).

get_master_db_size_info(BucketName, VBucketAndDb) ->
    Db = open_db_or_fail(BucketName, VBucketAndDb),

    try
        {ok, DbInfo} = couch_db:get_db_info(Db),

        {proplists:get_value(data_size, DbInfo, 0),
         proplists:get_value(disk_size, DbInfo)}
    after
        couch_db:close(Db)
    end.

get_db_size_info(Bucket, VBucket) ->
    {ok, Props} = ns_memcached:get_vbucket_details_stats(Bucket, VBucket),

    {list_to_integer(proplists:get_value("db_data_size", Props)),
     list_to_integer(proplists:get_value("db_file_size", Props))}.

maybe_compact_vbucket(BucketName, {VBucket, DbName} = VBucketAndDb,
                      Config, Force, Options) ->
    Bucket = binary_to_list(BucketName),

    SizeInfo = case VBucket of
                   master ->
                       get_master_db_size_info(BucketName, VBucketAndDb);
                   _ ->
                       get_db_size_info(Bucket, VBucket)
               end,

    case Force orelse vbucket_needs_compaction(SizeInfo, Config) of
        true ->
            ok;
        false ->
            exit(normal)
    end,

    %% effectful
    ensure_can_db_compact(DbName, SizeInfo),

    ?log_info("Compacting `~s' (~p)", [DbName, Options]),
    Ret = case VBucket of
              master ->
                  compact_master_vbucket(BucketName, DbName);
              _ ->
                  ns_memcached:compact_vbucket(Bucket, VBucket, Options)
          end,
    ?log_info("Compaction of ~p has finished with ~p", [DbName, Ret]),
    Ret.

spawn_vbucket_compactor(BucketName, VBucketAndDb, Config, Force, Options) ->
    proc_lib:spawn_link(
      fun () ->
              case maybe_compact_vbucket(BucketName, VBucketAndDb, Config, Force, Options) of
                  ok ->
                      ok;
                  Result ->
                      exit(Result)
              end
      end).

compact_master_vbucket(BucketName, DbName) ->
    Db = open_db_or_fail(BucketName, {master, DbName}),
    process_flag(trap_exit, true),

    {ok, Compactor} = couch_db:start_compact(Db, [dropdeletes]),

    CompactorRef = erlang:monitor(process, Compactor),

    receive
        {'EXIT', _Parent, Reason} = Exit ->
            ?log_debug("Got exit signal from parent: ~p", [Exit]),

            erlang:demonitor(CompactorRef),
            ok = couch_db:cancel_compact(Db),
            Reason;
        {'DOWN', CompactorRef, process, Compactor, normal} ->
            ok;
        {'DOWN', CompactorRef, process, Compactor, noproc} ->
            %% compactor died before we managed to monitor it;
            %% we will treat this as an error
            {db_compactor_died_too_soon, DbName};
        {'DOWN', CompactorRef, process, Compactor, Reason} ->
            Reason
    end.

spawn_view_compactor(BucketName, DDocId, Config, Force, OriginalTarget) ->
    Parent = self(),

    proc_lib:spawn_link(
      fun () ->
              process_flag(trap_exit, true),

              Compactors =
                  [ [{type, view},
                     {important, true},
                     {name, view_name(BucketName, DDocId, Type)},
                     {fa, {fun spawn_view_index_compactor/6,
                           [BucketName, DDocId,
                            Type, Config, Force, OriginalTarget]}}] ||
                      Type <- [main, replica] ],

              do_chain_compactors(Parent, Compactors)
      end).

view_name(BucketName, DDocId, Type) ->
    <<BucketName/binary, $/, DDocId/binary, $/,
      (atom_to_binary(Type, latin1))/binary>>.

spawn_view_index_compactor(BucketName, DDocId,
                           Type, Config, Force, OriginalTarget) ->
    Parent = self(),

    proc_lib:spawn_link(
      fun () ->
              ViewName = view_name(BucketName, DDocId, Type),

              DoCompact =
                  case Force of
                      true ->
                          ?log_info("Forceful compaction of view ~s requested",
                                    [ViewName]),
                          true;
                      false ->
                          view_needs_compaction(BucketName, DDocId, Type, Config)
                  end,

              DoCompact orelse exit(normal),

              %% effectful
              ensure_can_view_compact(BucketName, DDocId, Type),

              ?log_info("Compacting indexes for ~s", [ViewName]),
              process_flag(trap_exit, true),

              TriggerType = case Force of
                                true ->
                                    manual;
                                false ->
                                    scheduled
                            end,

              InitialStatus = [{original_target, OriginalTarget},
                               {trigger_type, TriggerType}],

              do_spawn_view_index_compactor(Parent, BucketName,
                                            DDocId, Type, InitialStatus),

              ?log_info("Finished compacting indexes for ~s", [ViewName])
      end).

do_spawn_view_index_compactor(Parent, BucketName, DDocId, Type, InitialStatus) ->
    Compactor = start_view_index_compactor(BucketName, DDocId,
                                           Type, InitialStatus),
    CompactorRef = erlang:monitor(process, Compactor),

    receive
        {'EXIT', Parent, Reason} = Exit ->
            ?log_debug("Got exit signal from parent: ~p", [Exit]),

            erlang:demonitor(CompactorRef),
            ok = couch_set_view_compactor:cancel_compact(mapreduce_view,
                                                         BucketName, DDocId,
                                                         Type, prod),
            exit(Reason);
        {'DOWN', CompactorRef, process, Compactor, normal} ->
            ok;
        {'DOWN', CompactorRef, process, Compactor, noproc} ->
            exit({index_compactor_died_too_soon,
                  BucketName, DDocId, Type});
        {'DOWN', CompactorRef, process, Compactor, Reason}
          when Reason =:= shutdown;
               Reason =:= {updater_died, shutdown};
               Reason =:= {updated_died, noproc};
               Reason =:= {updater_died, {updater_error, shutdown}} ->
            do_spawn_view_index_compactor(Parent, BucketName,
                                          DDocId, Type, InitialStatus);
        {'DOWN', CompactorRef, process, Compactor, Reason} ->
            exit(Reason)
    end.

start_view_index_compactor(BucketName, DDocId, Type, InitialStatus) ->
    case couch_set_view_compactor:start_compact(mapreduce_view, BucketName,
                                                DDocId, Type, prod,
                                                InitialStatus) of
        {ok, Pid} ->
            Pid;
        {error, initial_build} ->
            exit(normal)
    end.

bucket_needs_compaction(BucketName, NumVBuckets,
                        #config{daemon=#daemon_config{min_file_size=MinFileSize},
                                db_fragmentation=FragThreshold}) ->
    {DataSize, FileSize} = aggregated_size_info(binary_to_list(BucketName)),

    ?log_debug("`~s` data size is ~p, disk size is ~p",
               [BucketName, DataSize, FileSize]),

    file_needs_compaction(DataSize, FileSize,
                          FragThreshold, MinFileSize * NumVBuckets).

file_needs_compaction(DataSize, FileSize, FragThreshold, MinFileSize) ->
    %% NOTE: If there are no vbuckets on this node MinFileSize will be
    %% 0 so less then or _equals_ is really important to avoid
    %% division by zero
    case FileSize =< MinFileSize of
        true ->
            false;
        false ->
            FragSize = FileSize - DataSize,
            Frag = round((FragSize / FileSize) * 100),

            check_fragmentation(FragThreshold, Frag, FragSize)
    end.

aggregated_size_info(Bucket) ->
    {ok, {DS, FS}} =
        ns_memcached:raw_stats(node(), Bucket, <<"diskinfo">>,
                               fun (<<"ep_db_file_size">>, V, {DataSize, _}) ->
                                       {DataSize, V};
                                   (<<"ep_db_data_size">>, V, {_, FileSize}) ->
                                       {V, FileSize};
                                   (_, _, Tuple) ->
                                       Tuple
                               end, {<<"0">>, <<"0">>}),
    {list_to_integer(binary_to_list(DS)),
     list_to_integer(binary_to_list(FS))}.

check_fragmentation({FragLimit, FragSizeLimit}, Frag, FragSize) ->
    true = is_integer(FragLimit),
    true = is_integer(FragSizeLimit),

    (Frag >= FragLimit) orelse (FragSize >= FragSizeLimit).

ensure_can_db_compact(DbName, {DataSize, _}) ->
    SpaceRequired = space_required(DataSize),

    {ok, DbDir} = ns_storage_conf:this_node_dbdir(),
    Free = free_space(DbDir),

    case Free >= SpaceRequired of
        true ->
            ok;
        false ->
            ale:error(?USER_LOGGER,
                      "Cannot compact database `~s`: "
                      "the estimated necessary disk space is about ~p bytes "
                      "but the currently available disk space is ~p bytes.",
                      [DbName, SpaceRequired, Free]),
            exit({not_enough_space, DbName, SpaceRequired, Free})
    end.

space_required(DataSize) ->
    round(DataSize * 2.0).

free_space(Path) ->
    Stats = ns_disksup:get_disk_data(),
    {ok, RealPath} = misc:realpath(Path, "/"),
    {ok, {_, Total, Usage}} =
        ns_storage_conf:extract_disk_stats_for_path(Stats, RealPath),
    trunc(Total - (Total * (Usage / 100))) * 1024.

view_needs_compaction(BucketName, DDocId, Type,
                      #config{view_fragmentation=FragThreshold,
                              daemon=#daemon_config{min_file_size=MinFileSize}}) ->
    Info = get_group_data_info(BucketName, DDocId, Type),

    case Info of
        disabled ->
            %% replica index can be disabled; so we'll skip it here instead of
            %% spamming logs with irrelevant crash reports
            false;
        _ ->
            InitialBuild = proplists:get_value(initial_build, Info),
            case InitialBuild of
                true ->
                    false;
                false ->
                    FileSize = proplists:get_value(disk_size, Info),
                    DataSize = proplists:get_value(data_size, Info, 0),

                    ?log_debug("`~s/~s/~s` data_size is ~p, disk_size is ~p",
                               [BucketName, DDocId, Type, DataSize, FileSize]),

                    file_needs_compaction(DataSize, FileSize,
                                          FragThreshold, MinFileSize)
            end
    end.

ensure_can_view_compact(BucketName, DDocId, Type) ->
    Info = get_group_data_info(BucketName, DDocId, Type),

    case Info of
        disabled ->
            exit(normal);
        _ ->
            DataSize = proplists:get_value(data_size, Info, 0),
            SpaceRequired = space_required(DataSize),

            {ok, ViewsDir} = ns_storage_conf:this_node_ixdir(),
            Free = free_space(ViewsDir),

            case Free >= SpaceRequired of
                true ->
                    ok;
                false ->
                    ale:error(?USER_LOGGER,
                              "Cannot compact view ~s/~s/~p: "
                              "the estimated necessary disk space is about ~p bytes "
                              "but the currently available disk space is ~p bytes.",
                              [BucketName, DDocId, Type, SpaceRequired, Free]),
                    exit({not_enough_space,
                          BucketName, DDocId, SpaceRequired, Free})
            end
    end.

get_group_data_info(BucketName, DDocId, main) ->
    {ok, Info} = couch_set_view:get_group_data_size(mapreduce_view,
                                                    BucketName, DDocId),
    Info;
get_group_data_info(BucketName, DDocId, replica) ->
    MainInfo = get_group_data_info(BucketName, DDocId, main),
    proplists:get_value(replica_group_info, MainInfo, disabled).

ddoc_names(BucketName) ->
    capi_ddoc_replication_srv:fetch_ddoc_ids(BucketName).

search_node_default(Config, Key, Default) ->
    case ns_config:search_node(Config, Key) of
        false ->
            Default;
        {value, Value} ->
            Value
    end.

compaction_daemon_config(Config) ->
    Props = search_node_default(Config, compaction_daemon, []),
    daemon_config_to_record(Props).

compaction_config_props(Config, BucketName) ->
    Global = search_node_default(Config, autocompaction, []),
    BucketConfig = get_bucket(BucketName, Config),
    PerBucket = case proplists:get_value(autocompaction, BucketConfig, []) of
                    false -> [];
                    SomeValue -> SomeValue
                end,

    lists:foldl(
      fun ({Key, _Value} = KV, Acc) ->
              [KV | proplists:delete(Key, Acc)]
      end, Global, PerBucket).

compaction_config(BucketName) ->
    compaction_config(ns_config:get(), BucketName).

compaction_config(Config, BucketName) ->
    ConfigProps = compaction_config_props(Config, BucketName),
    DaemonConfig = compaction_daemon_config(Config),
    ConfigRecord = config_to_record(ConfigProps, DaemonConfig),

    {ConfigRecord, ConfigProps}.

config_to_record(Config, DaemonConfig) ->
    do_config_to_record(Config, #config{daemon=DaemonConfig}).

do_config_to_record([], Acc) ->
    Acc;
do_config_to_record([{database_fragmentation_threshold, V} | Rest], Acc) ->
   do_config_to_record(
     Rest, Acc#config{db_fragmentation=normalize_fragmentation(V)});
do_config_to_record([{view_fragmentation_threshold, V} | Rest], Acc) ->
    do_config_to_record(
      Rest, Acc#config{view_fragmentation=normalize_fragmentation(V)});
do_config_to_record([{parallel_db_and_view_compaction, V} | Rest], Acc) ->
    do_config_to_record(Rest, Acc#config{parallel_view_compact=V});
do_config_to_record([{allowed_time_period, V} | Rest], Acc) ->
    do_config_to_record(Rest, Acc#config{allowed_period=allowed_period_record(V)});
do_config_to_record([{_OtherKey, _V} | Rest], Acc) ->
    %% NOTE !!!: releases before 2.2.0 raised error here
    do_config_to_record(Rest, Acc).

normalize_fragmentation({Percentage, Size}) ->
    NormPercencentage =
        case Percentage of
            undefined ->
                100;
            _ when is_integer(Percentage) ->
                Percentage
        end,

    NormSize =
        case Size of
            undefined ->
                %% just set it to something really big (to simplify further
                %% logic a little bit)
                1 bsl 64;
            _ when is_integer(Size) ->
                Size
        end,

    {NormPercencentage, NormSize}.


daemon_config_to_record(Config) ->
    do_daemon_config_to_record(Config, #daemon_config{}).

do_daemon_config_to_record([], Acc) ->
    Acc;
do_daemon_config_to_record([{min_file_size, V} | Rest], Acc) ->
    do_daemon_config_to_record(Rest, Acc#daemon_config{min_file_size=V});
do_daemon_config_to_record([{check_interval, V} | Rest], Acc) ->
    do_daemon_config_to_record(Rest, Acc#daemon_config{check_interval=V}).

allowed_period_record(Config) ->
    do_allowed_period_record(Config, #period{}).

do_allowed_period_record([], Acc) ->
    Acc;
do_allowed_period_record([{from_hour, V} | Rest], Acc) ->
    do_allowed_period_record(Rest, Acc#period{from_hour=V});
do_allowed_period_record([{from_minute, V} | Rest], Acc) ->
    do_allowed_period_record(Rest, Acc#period{from_minute=V});
do_allowed_period_record([{to_hour, V} | Rest], Acc) ->
    do_allowed_period_record(Rest, Acc#period{to_hour=V});
do_allowed_period_record([{to_minute, V} | Rest], Acc) ->
    do_allowed_period_record(Rest, Acc#period{to_minute=V});
do_allowed_period_record([{abort_outside, V} | Rest], Acc) ->
    do_allowed_period_record(Rest, Acc#period{abort_outside=V});
do_allowed_period_record([{OtherKey, _V} | _], _Acc) ->
    %% should not happen
    exit({invalid_period_key_bug, OtherKey}).

-spec check_period_and_maybe_exit(#config{}) -> ok.
check_period_and_maybe_exit(#config{allowed_period=undefined}) ->
    ok;
check_period_and_maybe_exit(#config{allowed_period=Period}) ->
    do_check_period_and_maybe_exit(Period).

do_check_period_and_maybe_exit(#period{from_hour=FromHour,
                                       from_minute=FromMinute,
                                       to_hour=ToHour,
                                       to_minute=ToMinute,
                                       abort_outside=AbortOutside}) ->
    {HH, MM, _} = erlang:time(),

    Now0 = time_to_minutes(HH, MM),
    From = time_to_minutes(FromHour, FromMinute),
    To0 = time_to_minutes(ToHour, ToMinute),

    To = case To0 < From of
             true ->
                 To0 + 24 * 60;
             false ->
                 To0
         end,

    Now = case Now0 < From of
              true ->
                  Now0 + 24 * 60;
              false ->
                  Now0
          end,

    CanStart = (Now >= From) andalso (Now < To),
    MinutesLeft = To - Now,

    case CanStart of
        true ->
            case AbortOutside of
                true ->
                    shutdown_compactor_after(MinutesLeft),
                    ok;
                false ->
                    ok
            end;
        false ->
            exit(normal)
    end.

time_to_minutes(HH, MM) ->
    HH * 60 + MM.

shutdown_compactor_after(MinutesLeft) ->
    Compactor = self(),

    MillisLeft = MinutesLeft * 60 * 1000,
    ?log_debug("Scheduling a timer to shut "
               "ourselves down in ~p ms", [MillisLeft]),

    spawn_link(
      fun () ->
              process_flag(trap_exit, true),

              receive
                  {'EXIT', Compactor, _Reason} ->
                      ok;
                  Msg ->
                      exit({unexpected_message_bug, Msg})
              after
                  MillisLeft ->
                      ?log_info("Shutting compactor ~p down "
                                "not to abuse allowed time period", [Compactor]),
                      exit(Compactor, shutdown)
              end
      end).

init_scheduled_compaction(CheckInterval, Message, Fun) ->
    self() ! Message,
    #compaction_state{buckets_to_compact=[],
                      compactor_pid=undefined,
                      scheduler=compaction_scheduler:init(CheckInterval, Message),
                      compactor_fun = Fun}.

process_scheduler_message(#compaction_state{buckets_to_compact=Buckets0,
                                            scheduler=Scheduler,
                                            compactor_pid=undefined} = State) ->
    Buckets =
        case Buckets0 of
            [] ->
                lists:map(fun list_to_binary/1,
                          ns_bucket:node_bucket_names_of_type(node(), membase));
            _ ->
                Buckets0
        end,

    case Buckets of
        [] ->
            ?log_debug("No buckets to compact. Rescheduling compaction."),
            State#compaction_state{scheduler=compaction_scheduler:schedule_next(Scheduler)};
        _ ->
            ?log_debug("Starting compaction for the following buckets: ~n~p",
                       [Buckets]),
            NewState1 = State#compaction_state{buckets_to_compact=Buckets,
                                               scheduler=
                                                   compaction_scheduler:start_now(Scheduler)},
            compact_next_bucket(NewState1)
    end.

process_compactors_exit(Reason, #compaction_state{compactor_pid=Compactor,
                                                  buckets_to_compact=Buckets,
                                                  scheduler=Scheduler} = State) ->
    true = Buckets =/= [],

    case Reason of
        normal ->
            ok;
        shutdown ->
            ?log_debug("Compactor ~p exited with reason ~p", [Compactor, Reason]);
        _ ->
            ?log_error("Compactor ~p exited unexpectedly: ~p. "
                       "Moving to the next bucket.", [Compactor, Reason])
    end,

    NewBuckets =
        case Reason of
            shutdown ->
                %% this can happen either on config change or if compaction
                %% has been stopped due to end of allowed time period; in the
                %% latter case there's no need to compact bucket again; but it
                %% won't hurt: compaction shall be terminated rightaway with
                %% reason normal
                Buckets;
            _ ->
                tl(Buckets)
        end,

    NewState = State#compaction_state{compactor_pid=undefined,
                                      compactor_config=undefined,
                                      buckets_to_compact=NewBuckets},
    case NewBuckets of
        [] ->
            ?log_debug("Finished compaction iteration."),
            NewState#compaction_state{scheduler =
                                          compaction_scheduler:schedule_next(Scheduler)};
        _ ->
            compact_next_bucket(NewState)
    end.

process_config_change(Config, #compaction_state{scheduler = Scheduler} = State) ->
    CheckInterval = get_check_interval(Config),
    NewScheduler = compaction_scheduler:set_interval(CheckInterval, Scheduler),

    maybe_restart_compactor(Config, State),
    State#compaction_state{scheduler = NewScheduler}.

maybe_restart_compactor(_NewConfig, #compaction_state{compactor_pid=undefined}) ->
    ok;
maybe_restart_compactor(_NewConfig, #compaction_state{buckets_to_compact=[]}) ->
    ok;
maybe_restart_compactor(NewConfig, #compaction_state{compactor_pid=Compactor,
                                                     buckets_to_compact=[CompactedBucket|_],
                                                     compactor_config=Config}) ->
    {MaybeNewConfig, _} = compaction_config(NewConfig, CompactedBucket),
    case MaybeNewConfig =:= Config of
        true ->
            ok;
        false ->
            ?log_info("Restarting compactor ~p since "
                      "autocompaction config has changed", [Compactor]),
            exit(Compactor, shutdown)
    end.

-spec compact_next_bucket(#compaction_state{}) -> #compaction_state{}.
compact_next_bucket(#compaction_state{buckets_to_compact=Buckets,
                                      compactor_pid=undefined,
                                      compactor_fun=Fun} = State) ->
    true = [] =/= Buckets,

    NextBucket = hd(Buckets),

    {InitialConfig, _ConfigProps} = Configs = compaction_config(NextBucket),

    Compactor = Fun(NextBucket, Configs),
    State#compaction_state{compactor_pid = Compactor,
                           compactor_config = InitialConfig}.

db_name(BucketName, SubName) when is_list(SubName) ->
    db_name(BucketName, list_to_binary(SubName));
db_name(BucketName, VBucket) when is_integer(VBucket) ->
    db_name(BucketName, integer_to_list(VBucket));
db_name(BucketName, SubName) when is_binary(SubName) ->
    true = is_binary(BucketName),

    <<BucketName/binary, $/, SubName/binary>>.

all_bucket_dbs(BucketName) ->
    BucketConfig = get_bucket(BucketName),
    NodeVBuckets = ns_bucket:all_node_vbuckets(BucketConfig),
    VBucketDbs = [{V, db_name(BucketName, V)} || V <- NodeVBuckets],

    [{master, db_name(BucketName, "master")} | VBucketDbs].

get_bucket(BucketName) ->
    get_bucket(BucketName, ns_config:get()).

get_bucket(BucketName, Config) ->
    BucketNameStr = binary_to_list(BucketName),

    case ns_bucket:get_bucket(BucketNameStr, Config) of
        not_present ->
            exit({bucket_not_present, BucketName});
        {ok, BucketConfig} ->
            BucketConfig
    end.

bucket_exists(BucketName) ->
    case ns_bucket:get_bucket_light(binary_to_list(BucketName)) of
        {ok, Config} ->
            (ns_bucket:bucket_type(Config) =:= membase) andalso
                lists:member(node(), ns_bucket:bucket_nodes(Config));
        not_present ->
            false
    end.

check_bucket_exists_and_maybe_exit(BucketName) ->
    case bucket_exists(BucketName) of
        true ->
            ok;
        false ->
            exit(normal)
    end.

-spec register_forced_compaction(pid(), #forced_compaction{},
                                 #state{}) -> #state{}.
register_forced_compaction(Pid, Compaction,
                    #state{running_forced_compactions=Compactions,
                           forced_compaction_pids=CompactionPids} = State) ->
    error = dict:find(Compaction, CompactionPids),

    NewCompactions = dict:store(Pid, Compaction, Compactions),
    NewCompactionPids = dict:store(Compaction, Pid, CompactionPids),

    State#state{running_forced_compactions=NewCompactions,
                forced_compaction_pids=NewCompactionPids}.

-spec unregister_forced_compaction(pid(), #state{}) -> #state{}.
unregister_forced_compaction(Pid,
                             #state{running_forced_compactions=Compactions,
                                    forced_compaction_pids=CompactionPids} = State) ->
    Compaction = dict:fetch(Pid, Compactions),

    NewCompactions = dict:erase(Pid, Compactions),
    NewCompactionPids = dict:erase(Compaction, CompactionPids),

    State#state{running_forced_compactions=NewCompactions,
                forced_compaction_pids=NewCompactionPids}.

-spec is_already_being_compacted(#forced_compaction{}, #state{}) -> boolean().
is_already_being_compacted(Compaction,
                           #state{forced_compaction_pids=CompactionPids}) ->
    dict:find(Compaction, CompactionPids) =/= error.

-spec maybe_cancel_compaction(#forced_compaction{}, #state{}) -> #state{}.
maybe_cancel_compaction(Compaction,
                        #state{forced_compaction_pids=CompactionPids} = State) ->
    case dict:find(Compaction, CompactionPids) of
        error ->
            State;
        {ok, Pid} ->
            exit(Pid, shutdown),
            receive
                %% let all the other exit messages be handled by corresponding
                %% handle_info clause so that error message is logged
                {'EXIT', Pid, shutdown} ->
                    unregister_forced_compaction(Pid, State);
                {'EXIT', Pid, _Reason} = Exit ->
                    {noreply, NewState} = handle_info(Exit, State),
                    NewState
            end
    end.
