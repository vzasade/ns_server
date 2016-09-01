%% @author Northscale <info@northscale.com>
%% @copyright 2009 NorthScale, Inc.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%      http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%
%% @doc Implementation of diagnostics web requests
-module(diag_handler).
-author('NorthScale <info@northscale.com>').

-include("ns_common.hrl").
-include("ns_log.hrl").
-include_lib("kernel/include/file.hrl").

-include_lib("eunit/include/eunit.hrl").
-include("remote_clusters_info.hrl").

-export([do_diag_per_node_binary/0,
         handle_diag/1,
         handle_sasl_logs/1, handle_sasl_logs/2,
         handle_diag_ale/1,
         handle_diag_eval/1,
         handle_diag_master_events/1,
         handle_diag_vbuckets/1,
         handle_diag_get_password/1,
         arm_timeout/2, arm_timeout/1, disarm_timeout/1,
         grab_process_info/1, manifest/0,
         grab_all_tap_and_checkpoint_stats/0,
         grab_all_tap_and_checkpoint_stats/1,
         log_all_tap_and_checkpoint_stats/0,
         diagnosing_timeouts/1,
         %% rpc-ed to grab babysitter and couchdb processes
         grab_process_infos/0,
         %% rpc-ed to grab couchdb ets_tables
         grab_all_ets_tables/0]).

%% Read the manifest.xml file
manifest() ->
    case file:read_file(filename:join(path_config:component_path(bin, ".."), "manifest.xml")) of
        {ok, C} ->
            string:tokens(binary_to_list(C), "\n");
        _ -> []
    end.

%% works like lists:foldl(Fun, Acc, binary:split(Binary, Separator, [global]))
%%
%% But without problems of binary:split. See MB-9534
split_fold_incremental(Binary, Separator, Fun, Acc) ->
    CP = binary:compile_pattern(Separator),
    Len = erlang:size(Binary),
    split_fold_incremental_loop(Binary, CP, Len, Fun, Acc, 0).

split_fold_incremental_loop(_Binary, _CP, Len, _Fun, Acc, Start) when Start > Len ->
    Acc;
split_fold_incremental_loop(Binary, CP, Len, Fun, Acc, Start) ->
    {MatchPos, MatchLen} =
        case binary:match(Binary, CP, [{scope, {Start, Len - Start}}]) of
            nomatch ->
                %% NOTE: 1 here will move Start _past_ Len on next
                %% loop iteration
                {Len, 1};
            MatchPair ->
                MatchPair
        end,
    NewPiece = binary:part(Binary, Start, MatchPos - Start),
    NewAcc = Fun(NewPiece, Acc),
    split_fold_incremental_loop(Binary, CP, Len, Fun, NewAcc, MatchPos + MatchLen).

-spec sanitize_backtrace(binary()) -> [binary()].
sanitize_backtrace(Backtrace) ->
    R = split_fold_incremental(
          Backtrace, <<"\n">>,
          fun (X, Acc) ->
                  if
                      size(X) =< 120 ->
                          [binary:copy(X) | Acc];
                      true ->
                          [binary:copy(binary:part(X, 1, 120)) | Acc]
                  end
          end, []),
    lists:reverse(R).

massage_messages(Messages) ->
    [massage_message(M) || M <- lists:sublist(Messages, 10)].

massage_message(Message) ->
    iolist_to_binary(io_lib:format("~W", [Message, 25])).

grab_process_info(Pid) ->
    PureInfo = erlang:process_info(Pid,
                                   [registered_name,
                                    status,
                                    initial_call,
                                    backtrace,
                                    error_handler,
                                    garbage_collection,
                                    heap_size,
                                    total_heap_size,
                                    links,
                                    monitors,
                                    monitored_by,
                                    memory,
                                    messages,
                                    message_queue_len,
                                    reductions,
                                    trap_exit,
                                    current_location,
                                    dictionary]),

    Backtrace = proplists:get_value(backtrace, PureInfo),
    NewBacktrace = sanitize_backtrace(Backtrace),

    Messages = proplists:get_value(messages, PureInfo),
    NewMessages = massage_messages(Messages),

    Info0 = lists:keyreplace(backtrace, 1, PureInfo, {backtrace, NewBacktrace}),
    lists:keyreplace(messages, 1, Info0, {messages, NewMessages}).

grab_all_tap_and_checkpoint_stats() ->
    grab_all_tap_and_checkpoint_stats(15000).

grab_all_tap_and_checkpoint_stats(Timeout) ->
    ActiveBuckets = ns_memcached:active_buckets(),
    ThisNodeBuckets = ns_bucket:node_bucket_names_of_type(node(), membase),
    InterestingBuckets = ordsets:intersection(lists:sort(ActiveBuckets),
                                              lists:sort(ThisNodeBuckets)),
    WorkItems = [{Bucket, Type} || Bucket <- InterestingBuckets,
                                   Type <- [<<"tap">>, <<"checkpoint">>, <<"dcp">>]],
    Results = misc:parallel_map(
                fun ({Bucket, Type}) ->
                        {ok, _} = timer2:kill_after(Timeout),
                        case get_dcp_ckpt_tap_stats(Bucket, Type) of
                            {ok, V} -> V;
                            Crap -> Crap
                        end
                end, WorkItems, infinity),
    {WiB, WiT} = lists:unzip(WorkItems),
    lists:zip3(WiB, WiT, Results).

get_dcp_ckpt_tap_stats(Bucket, Type) ->
    ns_memcached:raw_stats(
      node(), Bucket, Type,
      fun(K = <<"eq_dcpq:replication:", _/binary>>, V, Acc) ->
              process_dcp_ckpt_tap_stats(K, V, Acc);
         (<<"eq_dcpq:", _/binary>>, _V, Acc) ->
              %% Drop all other "eq_dcpq" stats
              Acc;
         (K, V, Acc) ->
              process_dcp_ckpt_tap_stats(K, V, Acc)
      end, []).

process_dcp_ckpt_tap_stats(K, V, []) ->
    <<K/binary, ": ", V/binary>>;
process_dcp_ckpt_tap_stats(K, V, Acc) ->
    <<Acc/binary, ",", $\n, K/binary, ": ", V/binary>>.

log_all_tap_and_checkpoint_stats() ->
    ?log_info("logging tap & checkpoint stats"),
    [begin
         ?log_info("~s:~s:~n[~s]",[Type, Bucket, Values])
     end || {Bucket, Type, Values} <- grab_all_tap_and_checkpoint_stats()],
    ?log_info("end of logging tap & checkpoint stats").

task_status_all() ->
    local_tasks:all() ++ ns_couchdb_api:get_tasks().

do_diag_per_node_binary() ->
    work_queue:submit_sync_work(
      diag_handler_worker,
      fun () ->
              (catch collect_diag_per_node_binary(40000))
      end).

collect_diag_per_node_binary(Timeout) ->
    ReplyRef = make_ref(),
    Parent = self(),
    {ChildPid, ChildRef} =
        spawn_monitor(
          fun () ->
                  Reply = fun (Key, Value) ->
                                  Parent ! {ReplyRef, {Key, Value}},
                                  ?log_debug("Collected data for key ~p", [Key])
                          end,

                  ChildPid = self(),
                  proc_lib:spawn_link(
                    fun () ->
                            erlang:monitor(process, Parent),
                            erlang:monitor(process, ChildPid),

                            receive
                                {'DOWN', _, _, _, Reason} ->
                                    exit(ChildPid, Reason)
                            end
                    end),

                  try
                      collect_diag_per_node_binary_body(Reply)
                  catch
                      T:E ->
                          Reply(partial_results_reason,
                                {process_died, {T, E, erlang:get_stacktrace()}})
                  end
          end),

    TRef = erlang:send_after(Timeout, self(), timeout),

    try
        RV = collect_diag_per_node_binary_loop(ReplyRef, ChildRef, []),
        term_to_binary(RV)
    after
        erlang:cancel_timer(TRef),
        receive
            timeout -> ok
        after
            0 -> ok
        end,

        erlang:demonitor(ChildRef, [flush]),
        exit(ChildPid, kill),

        flush_leftover_replies(ReplyRef)
    end.

flush_leftover_replies(ReplyRef) ->
    receive
        {ReplyRef, _} ->
            flush_leftover_replies(ReplyRef)
    after
        0 -> ok
    end.

collect_diag_per_node_binary_loop(ReplyRef, ChildRef, Results) ->
    receive
        {ReplyRef, Item} ->
            collect_diag_per_node_binary_loop(ReplyRef, ChildRef, [Item | Results]);
        timeout ->
            [{partial_results_reason, timeout} | Results];
        {'DOWN', ChildRef, process, _, _} ->
            Results
    end.

collect_diag_per_node_binary_body(Reply) ->
    ?log_debug("Start collecting diagnostic data"),
    ActiveBuckets = ns_memcached:active_buckets(),
    PersistentBuckets = [B || B <- ActiveBuckets, ns_bucket:is_persistent(B)],

    Reply(processes, grab_process_infos()),
    Reply(babysitter_processes, (catch grab_babysitter_process_infos())),
    Reply(couchdb_processes, (catch grab_couchdb_process_infos())),
    Reply(version, ns_info:version()),
    Reply(manifest, manifest()),
    Reply(config, ns_config_log:sanitize(ns_config:get_kv_list())),
    Reply(basic_info, element(2, ns_info:basic_info())),
    Reply(memory, memsup:get_memory_data()),
    Reply(disk, (catch ns_disksup:get_disk_data())),
    Reply(active_tasks, task_status_all()),
    Reply(ns_server_stats, (catch system_stats_collector:get_ns_server_stats())),
    Reply(active_buckets, ActiveBuckets),
    Reply(replication_docs, (catch xdc_rdoc_api:find_all_replication_docs(5000))),
    Reply(design_docs, [{Bucket, (catch capi_utils:full_live_ddocs(Bucket, 2000))} ||
                           Bucket <- PersistentBuckets]),
    Reply(ets_tables, (catch grab_all_ets_tables())),
    Reply(couchdb_ets_tables, (catch grab_couchdb_ets_tables())),
    Reply(internal_settings, (catch menelaus_web:build_internal_settings_kvs())),
    Reply(logging, (catch ale:capture_logging_diagnostics())),
    Reply(system_info, (catch grab_system_info())).

grab_babysitter_process_infos() ->
    rpc:call(ns_server:get_babysitter_node(), ?MODULE, grab_process_infos, [], 5000).

grab_couchdb_process_infos() ->
    rpc:call(ns_node_disco:couchdb_node(), ?MODULE, grab_process_infos, [], 5000).

grab_process_infos() ->
    grab_process_infos_loop(erlang:processes(), []).

grab_process_infos_loop([], Acc) ->
    Acc;
grab_process_infos_loop([P | RestPids], Acc) ->
    NewAcc = [{P, (catch grab_process_info(P))} | Acc],
    grab_process_infos_loop(RestPids, NewAcc).

grab_couchdb_ets_tables() ->
    rpc:call(ns_node_disco:couchdb_node(), ?MODULE, grab_all_ets_tables, [], 5000).

prepare_ets_table(_Table, failed) ->
    [];
prepare_ets_table(Table, {Info, Content}) ->
    [{{Table, Info}, sanitize_ets_table(Table, Info, Content)}].

sanitize_ets_table(ui_auth_by_token, _Info, _Content) ->
    ["not printed"];
sanitize_ets_table(ui_auth_by_expiration, _Info, _Content) ->
    ["not printed"];
sanitize_ets_table(xdcr_stats, _Info, Content) ->
    xdc_rep_utils:sanitize_state(Content);
sanitize_ets_table(remote_clusters_info, _Info, Content) ->
    sanitize_remote_clusters_info(Content);
sanitize_ets_table(ns_config_ets_dup, _Info, Content) ->
    ns_config_log:sanitize(Content);
sanitize_ets_table(_, Info, Content) ->
    case proplists:get_value(name, Info) of
        ssl_otp_pem_cache ->
            ["not printed"];
        _ ->
            Content
    end.

sanitize_remote_clusters_info(Content) ->
    misc:rewrite_tuples(
      fun (#remote_bucket{capi_vbucket_map = CapiVbucketMap} = RemoteBucket) ->
              {stop, RemoteBucket#remote_bucket{
                       password = <<"******">>,
                       capi_vbucket_map =
                           dict:map(fun (_Key, Urls) ->
                                            [misc:sanitize_url(Url) || Url <- Urls]
                                    end, CapiVbucketMap)
                      }
              };
          (T) ->
              {continue, T}
      end, Content).

grab_all_ets_tables() ->
    lists:flatmap(
      fun (T) ->
              InfoAndContent =
                  try
                      {ets:info(T), ets:tab2list(T)}
                  catch
                      _:_ -> failed
                  end,
              prepare_ets_table(T, InfoAndContent)
      end, ets:all()).

diag_format_timestamp(EpochMilliseconds) ->
    SecondsRaw = trunc(EpochMilliseconds/1000),
    MicroSecs = (EpochMilliseconds rem 1000) * 1000,
    AsNow = {SecondsRaw div 1000000, SecondsRaw rem 1000000, MicroSecs},
    ale_default_formatter:format_time(AsNow).

generate_diag_filename() ->
    {{YYYY, MM, DD}, {Hour, Min, Sec}} = calendar:now_to_local_time(now()),
    io_lib:format("ns-diag-~4.4.0w~2.2.0w~2.2.0w~2.2.0w~2.2.0w~2.2.0w.txt",
                  [YYYY, MM, DD, Hour, Min, Sec]).

diag_format_log_entry(Type, Code, Module, Node, TStamp, ShortText, Text) ->
    FormattedTStamp = diag_format_timestamp(TStamp),
    io_lib:format("~s ~s:~B:~s:~s(~s) - ~s~n",
                  [FormattedTStamp, Module, Code, Type, ShortText, Node, Text]).

handle_diag(Req) ->
    Params = Req:parse_qs(),
    MaybeContDisp = case proplists:get_value("mode", Params) of
                        "view" -> [];
                        _ -> [{"Content-Disposition", "attachment; filename=" ++ generate_diag_filename()}]
                    end,
    case proplists:get_value("noLogs", Params, "0") of
        "0" ->
            do_handle_diag(Req, MaybeContDisp);
        _ ->
            Resp = handle_just_diag(Req, MaybeContDisp),
            Resp:write_chunk(<<>>)
    end.

grab_per_node_diag(Nodes) ->
    {Results0, BadNodes} = rpc:multicall(Nodes,
                                         ?MODULE, do_diag_per_node_binary, [], 45000),
    Results1 = lists:zip(lists:subtract(Nodes, BadNodes), Results0),
    [{N, diag_failed} || N <- BadNodes] ++ Results1.

handle_just_diag(Req, Extra) ->
    Resp = menelaus_util:reply_ok(Req, "text/plain; charset=utf-8", chunked, Extra),

    Resp:write_chunk(<<"logs:\n-------------------------------\n">>),
    lists:foreach(fun (#log_entry{node = Node,
                                  module = Module,
                                  code = Code,
                                  msg = Msg,
                                  args = Args,
                                  cat = Cat,
                                  tstamp = TStamp}) ->
                          try io_lib:format(Msg, Args) of
                              S ->
                                  CodeString = ns_log:code_string(Module, Code),
                                  Type = menelaus_alert:category_bin(Cat),
                                  TStampEpoch = misc:time_to_epoch_ms_int(TStamp),
                                  Resp:write_chunk(iolist_to_binary(diag_format_log_entry(Type, Code, Module, Node, TStampEpoch, CodeString, S)))
                          catch _:_ -> ok
                          end
                  end, lists:keysort(#log_entry.tstamp, ns_log:recent())),
    Resp:write_chunk(<<"-------------------------------\n\n\n">>),

    Nodes = case proplists:get_value("oneNode", Req:parse_qs(), "0") of
                "0" -> ns_node_disco:nodes_actual();
                _ -> [node()]
            end,
    Results = grab_per_node_diag(Nodes),
    handle_per_node_just_diag(Resp, Results),

    Buckets = lists:sort(fun (A,B) -> element(1, A) =< element(1, B) end,
                         ns_bucket:get_buckets()),

    Infos = [["nodes_info = ~p", menelaus_web:build_nodes_info()],
             ["buckets = ~p", ns_config_log:sanitize(Buckets)]],

    [begin
         Text = io_lib:format(Fmt ++ "~n~n", Args),
         Resp:write_chunk(list_to_binary(Text))
     end || [Fmt | Args] <- Infos],

    Resp.

write_chunk_format(Resp, Fmt, Args) ->
    Text = io_lib:format(Fmt, Args),
    Resp:write_chunk(list_to_binary(Text)).

handle_per_node_just_diag(_Resp, []) ->
    erlang:garbage_collect();
handle_per_node_just_diag(Resp, [{Node, DiagBinary} | Results]) ->
    erlang:garbage_collect(),

    Diag = case is_binary(DiagBinary) of
               true ->
                   try
                       binary_to_term(DiagBinary)
                   catch
                       error:badarg ->
                           ?log_error("Could not convert "
                                      "binary diag to term (node ~p)", [Node]),
                           {diag_failed, binary_to_term_failed}
                   end;
               false ->
                   DiagBinary
           end,
    do_handle_per_node_just_diag(Resp, Node, Diag),
    handle_per_node_just_diag(Resp, Results).

do_handle_per_node_just_diag(Resp, Node, Failed) when not is_list(Failed) ->
    write_chunk_format(Resp, "per_node_diag(~p) = ~p~n~n~n", [Node, Failed]);
do_handle_per_node_just_diag(Resp, Node, PerNodeDiag) ->
    %% NOTE: as of 4.0 we're not collecting master events here; but I'm
    %% leaving this for mixed clusters
    DiagNoMasterEvents = lists:keydelete(master_events, 1, PerNodeDiag),
    do_handle_per_node_processes(Resp, Node, DiagNoMasterEvents).

get_other_node_processes(Key, PerNodeDiag) ->
    Processes = proplists:get_value(Key, PerNodeDiag, []),
    %% it may be rpc or any other error; just pretend it's the process so that
    %% the error is visible
    case is_list(Processes) of
        true ->
            Processes;
        false ->
            [Processes]
    end.

write_processes(Resp, Node, Key, Processes) ->
    misc:executing_on_new_process(
      fun () ->
              write_chunk_format(Resp, "per_node_~p(~p) =~n", [Key, Node]),
              lists:foreach(
                fun (Process) ->
                        write_chunk_format(Resp, "     ~p~n", [Process])
                end, Processes),
              Resp:write_chunk(<<"\n\n">>)
      end).

do_handle_per_node_processes(Resp, Node, PerNodeDiag) ->
    erlang:garbage_collect(),

    Processes = proplists:get_value(processes, PerNodeDiag),

    BabysitterProcesses = get_other_node_processes(babysitter_processes, PerNodeDiag),
    CouchdbProcesses = get_other_node_processes(couchdb_processes, PerNodeDiag),

    DiagNoProcesses = lists:keydelete(
                        processes, 1,
                        lists:keydelete(babysitter_processes, 1,
                                        lists:keydelete(couchdb_processes, 1, PerNodeDiag))),

    write_processes(Resp, Node, processes, Processes),
    write_processes(Resp, Node, babysitter_processes, BabysitterProcesses),
    write_processes(Resp, Node, couchdb_processes, CouchdbProcesses),

    do_handle_per_node_stats(Resp, Node, DiagNoProcesses).

do_handle_per_node_stats(Resp, Node, PerNodeDiag)->
    %% pre 3.0 versions return stats as part of per node diagnostics; since
    %% number of samples may be quite substantial to cause memory issues while
    %% pretty-printing all of them in a bulk, to play safe we have this code
    %% to print samples one by one
    Stats0 = proplists:get_value(stats, PerNodeDiag, []),
    Stats = case is_list(Stats0) of
                true ->
                    Stats0;
                false ->
                    [{"_", [{'_', [Stats0]}]}]
            end,

    misc:executing_on_new_process(
      fun () ->
              lists:foreach(
                fun ({Bucket, BucketStats}) ->
                        lists:foreach(
                          fun ({Period, Samples}) ->
                                  write_chunk_format(Resp, "per_node_stats(~p, ~p, ~p) =~n",
                                                     [Node, Bucket, Period]),

                                  lists:foreach(
                                    fun (Sample) ->
                                            write_chunk_format(Resp, "     ~p~n", [Sample])
                                    end, Samples)
                          end, BucketStats)
                end, Stats),
              Resp:write_chunk(<<"\n\n">>)
      end),

    DiagNoStats = lists:keydelete(stats, 1, PerNodeDiag),
    do_handle_per_node_ets_tables(Resp, Node, DiagNoStats).

print_ets_table(Resp, Node, Key, Table, Info, Values) ->
    write_chunk_format(Resp, "per_node_~p(~p, ~p) =~n",
                       [Key, Node, Table]),
    case Info of
        [] ->
            ok;
        _ ->
            write_chunk_format(Resp, "  Info: ~p~n", [Info])
    end,
    case Values of
        [] ->
            ok;
        _ ->
            Resp:write_chunk(<<"  Values: \n">>),
            lists:foreach(
              fun (Value) ->
                      write_chunk_format(Resp, "    ~p~n", [Value])
              end, Values)
    end,
    Resp:write_chunk(<<"\n">>).

write_ets_tables(Resp, Node, Key, PerNodeDiag) ->
    EtsTables0 = proplists:get_value(Key, PerNodeDiag, []),

    EtsTables = case is_list(EtsTables0) of
                    true ->
                        EtsTables0;
                    false ->
                        [{'_', [EtsTables0]}]
                end,

    misc:executing_on_new_process(
      fun () ->
              lists:foreach(
                fun ({{Table, Info}, Values}) ->
                        print_ets_table(Resp, Node, Key, Table, Info, Values);
                    ({Table, Values}) ->
                        print_ets_table(Resp, Node, Key, Table, [], Values)
                end, EtsTables)
      end),

    lists:keydelete(Key, 1, PerNodeDiag).

do_handle_per_node_ets_tables(Resp, Node, PerNodeDiag) ->
    PerNodeDiag1 = write_ets_tables(Resp, Node, ets_tables, PerNodeDiag),
    PerNodeDiag2 = write_ets_tables(Resp, Node, couchdb_ets_tables, PerNodeDiag1),
    do_continue_handling_per_node_just_diag(Resp, Node, PerNodeDiag2).

do_continue_handling_per_node_just_diag(Resp, Node, Diag) ->
    erlang:garbage_collect(),

    misc:executing_on_new_process(
      fun () ->
              write_chunk_format(Resp, "per_node_diag(~p) =~n", [Node]),
              write_chunk_format(Resp, "     ~p~n", [Diag])
      end),

    Resp:write_chunk(<<"\n\n">>).

do_handle_diag(Req, Extra) ->
    Resp = handle_just_diag(Req, Extra),

    Logs = [?DEBUG_LOG_FILENAME, ?STATS_LOG_FILENAME,
            ?DEFAULT_LOG_FILENAME, ?ERRORS_LOG_FILENAME,
            ?XDCR_LOG_FILENAME, ?XDCR_ERRORS_LOG_FILENAME,
            ?COUCHDB_LOG_FILENAME,
            ?VIEWS_LOG_FILENAME, ?MAPREDUCE_ERRORS_LOG_FILENAME,
            ?BABYSITTER_LOG_FILENAME,
            ?SSL_PROXY_LOG_FILENAME, ?REPORTS_LOG_FILENAME,
            ?XDCR_TRACE_LOG_FILENAME,
            ?METAKV_LOG_FILENAME,
            ?ACCESS_LOG_FILENAME, ?INT_ACCESS_LOG_FILENAME,
            ?QUERY_LOG_FILENAME, ?PROJECTOR_LOG_FILENAME,
            ?GOXDCR_LOG_FILENAME, ?INDEXER_LOG_FILENAME,
            ?FTS_LOG_FILENAME],

    lists:foreach(fun (Log) ->
                          handle_log(Resp, Log)
                  end, Logs),
    handle_memcached_logs(Resp),
    Resp:write_chunk(<<"">>).

handle_log(Resp, LogName) ->
    LogsHeader = io_lib:format("logs_node (~s):~n"
                               "-------------------------------~n", [LogName]),
    Resp:write_chunk(list_to_binary(LogsHeader)),
    ns_log_browser:stream_logs(LogName,
                               fun (Data) -> Resp:write_chunk(Data) end),
    Resp:write_chunk(<<"-------------------------------\n">>).


handle_memcached_logs(Resp) ->
    Config = ns_config:get(),
    Path = ns_config:search_node_prop(Config, memcached, log_path),
    Prefix = ns_config:search_node_prop(Config, memcached, log_prefix),

    {ok, AllFiles} = file:list_dir(Path),
    Logs = [filename:join(Path, F) || F <- AllFiles,
                                      string:str(F, Prefix) == 1],
    TsLogs = lists:map(fun(F) ->
                               case file:read_file_info(F) of
                                   {ok, Info} ->
                                       {F, Info#file_info.mtime};
                                   {error, enoent} ->
                                       deleted
                               end
                       end, Logs),

    Sorted = lists:keysort(2, lists:filter(fun (X) -> X /= deleted end, TsLogs)),

    Resp:write_chunk(<<"memcached logs:\n-------------------------------\n">>),

    lists:foreach(fun ({Log, _}) ->
                          handle_memcached_log(Resp, Log)
                  end, Sorted).

handle_memcached_log(Resp, Log) ->
    case file:open(Log, [raw, binary]) of
        {ok, File} ->
            try
                do_handle_memcached_log(Resp, File)
            after
                file:close(File)
            end;
        Error ->
            Resp:write_chunk(
              list_to_binary(io_lib:format("Could not open file ~s: ~p~n", [Log, Error])))
    end.

-define(CHUNK_SIZE, 65536).

do_handle_memcached_log(Resp, File) ->
    case file:read(File, ?CHUNK_SIZE) of
        eof ->
            ok;
        {ok, Data} ->
            Resp:write_chunk(Data)
    end.

handle_sasl_logs(LogName, Req) ->
    FullLogName = LogName ++ ".log",
    case ns_log_browser:log_exists(FullLogName) of
        true ->
            Resp = menelaus_util:reply_ok(Req, "text/plain; charset=utf-8", chunked),
            handle_log(Resp, FullLogName),
            Resp:write_chunk(<<"">>);
        false ->
            menelaus_util:reply_text(Req, "Requested log file not found.\r\n", 404)
    end.

handle_sasl_logs(Req) ->
    handle_sasl_logs("debug", Req).

plist_to_ejson_rewriter([Tuple|_] = ListOfTuples) when is_tuple(Tuple) ->
    Objects = [misc:rewrite(fun plist_to_ejson_rewriter/1, PL)
               || PL <- ListOfTuples],
    {stop, {Objects}};
plist_to_ejson_rewriter(_Other) ->
    continue.

handle_diag_ale(Req) ->
    PList = ale:capture_logging_diagnostics(),
    Objects = misc:rewrite(fun plist_to_ejson_rewriter/1, PList),
    menelaus_util:reply_json(Req, Objects).

arm_timeout(Millis) ->
    arm_timeout(Millis,
                fun (Pid) ->
                        Info = (catch grab_process_info(Pid)),
                        ?log_error("slow process info:~n~p~n", [Info])
                end).

arm_timeout(Millis, Callback) ->
    Pid = self(),
    spawn_link(fun () ->
                       receive
                           done -> ok
                       after Millis ->
                               erlang:unlink(Pid),
                               Callback(Pid)
                       end,
                       erlang:unlink(Pid)
               end).

disarm_timeout(Pid) ->
    Pid ! done.

diagnosing_timeouts(Body) ->
    OnTimeout = fun (Error) ->
                        timeout_diag_logger:log_diagnostics(Error),
                        exit(Error)
                end,

    try Body()
    catch
        exit:{timeout, _} = Error ->
            OnTimeout(Error);
        exit:timeout = Error ->
            OnTimeout(Error)
    end.

grab_system_info() ->
    Allocators = [temp_alloc,
                  eheap_alloc,
                  binary_alloc,
                  ets_alloc,
                  driver_alloc,
                  sl_alloc,
                  ll_alloc,
                  fix_alloc,
                  std_alloc,
                  sys_alloc,
                  mseg_alloc],

    Kinds = lists:flatten(
              [allocated_areas,
               allocator,
               alloc_util_allocators,

               [[{allocator, A},
                 {allocator_sizes, A}] || A <- Allocators],

               [{cpu_topology, T} || T <- [defined, detected, used]],

               build_type,
               c_compiler_used,
               check_io,
               compat_rel,
               creation,
               debug_compiled,
               dist,
               dist_buf_busy_limit,
               dist_ctrl,
               driver_version,
               dynamic_trace,
               dynamic_trace_probes,
               elib_malloc,
               ets_limit,
               fullsweep_after,
               garbage_collection,
               heap_sizes,
               heap_type,
               kernel_poll,
               logical_processors,
               logical_processors_available,
               logical_processors_online,
               machine,
               min_heap_size,
               min_bin_vheap_size,
               modified_timing_level,
               multi_scheduling,
               multi_scheduling_blockers,
               otp_release,
               port_count,
               port_limit,
               process_count,
               process_limit,
               scheduler_bind_type,
               scheduler_bindings,
               scheduler_id,
               schedulers,
               schedulers_online,
               smp_support,
               system_version,
               system_architecture,
               threads,
               thread_pool_size,
               trace_control_word,
               update_cpu_info,
               version,
               wordsize]),

    [{K, (catch erlang:system_info(K))} || K <- Kinds].

handle_diag_eval(Req) ->
    Snippet = binary_to_list(Req:recv_body()),

    ?log_debug("WARNING: /diag/eval:~n~n~s", [Snippet]),

    try misc:eval(Snippet, erl_eval:add_binding('Req', Req, erl_eval:new_bindings())) of
        {value, Value, _} ->
            case Value of
                done ->
                    ok;
                {json, V} ->
                    menelaus_util:reply_json(Req, V, 200);
                _ ->
                    menelaus_util:reply_text(Req, io_lib:format("~p", [Value]), 200)
            end
    catch
        T:E ->
            Msg = io_lib:format("/diag/eval failed.~nError: ~p~nBacktrace:~n~p",
                                [{T, E}, erlang:get_stacktrace()]),
            ?log_error("Server error during processing: ~s", [Msg]),

            menelaus_util:reply_text(Req, io_lib:format("/diag/eval failed.~nError: ~p~nBacktrace:~n~p",
                                                        [{T, E}, erlang:get_stacktrace()]),
                                     500)
    end.

handle_diag_master_events(Req) ->
    Params = Req:parse_qs(),
    case proplists:get_value("o", Params) of
        undefined ->
            do_handle_diag_master_events(Req);
        _ ->
            Body = master_activity_events:format_some_history(master_activity_events_keeper:get_history()),
            menelaus_util:reply_ok(Req, "text/kind-of-json; charset=utf-8", Body)
    end.

do_handle_diag_master_events(Req) ->
    Rep = menelaus_util:reply_ok(Req, "text/kind-of-json; charset=utf-8", chunked),
    Parent = self(),
    Sock = Req:get(socket),
    inet:setopts(Sock, [{active, true}]),
    spawn_link(
      fun () ->
              master_activity_events:stream_events(
                fun (Event, _Ignored, _Eof) ->
                        IOList = master_activity_events:event_to_formatted_iolist(Event),
                        case IOList of
                            [] ->
                                ok;
                            _ ->
                                Parent ! {write_chunk, IOList}
                        end,
                        ok
                end, [])
      end),
    Loop = fun (Loop) ->
                   receive
                       {tcp_closed, _} ->
                           exit(self(), shutdown);
                       {tcp, _, _} ->
                           %% eat & ignore
                           Loop(Loop);
                       {write_chunk, Chunk} ->
                           Rep:write_chunk(Chunk),
                           Loop(Loop)
                   end
           end,
    Loop(Loop).


diag_vbucket_accumulate_vbucket_stats(K, V, Dict) ->
    case misc:split_binary_at_char(K, $:) of
        {<<"vb_",VB/binary>>, AttrName} ->
            SubDict = case dict:find(VB, Dict) of
                          error ->
                              dict:new();
                          {ok, X} -> X
                      end,
            dict:store(VB, dict:store(AttrName, V, SubDict), Dict);
        _ ->
            Dict
    end.

diag_vbucket_per_node(BucketName, Node) ->
    {ok, RV1} = ns_memcached:raw_stats(Node, BucketName, <<"vbucket-details">>, fun diag_vbucket_accumulate_vbucket_stats/3, dict:new()),
    {ok, RV2} = ns_memcached:raw_stats(Node, BucketName, <<"checkpoint">>, fun diag_vbucket_accumulate_vbucket_stats/3, RV1),
    RV2.

handle_diag_vbuckets(Req) ->
    Params = Req:parse_qs(),
    BucketName = proplists:get_value("bucket", Params),
    {ok, BucketConfig} = ns_bucket:get_bucket(BucketName),
    Nodes = ns_node_disco:nodes_actual_proper(),
    RawPerNode = misc:parallel_map(fun (Node) ->
                                           diag_vbucket_per_node(BucketName, Node)
                                   end, Nodes, 30000),
    PerNodeStates = lists:zip(Nodes,
                              [{struct, [{K, {struct, dict:to_list(V)}} || {K, V} <- dict:to_list(Dict)]}
                               || Dict <- RawPerNode]),
    JSON = {struct, [{name, list_to_binary(BucketName)},
                     {bucketMap, proplists:get_value(map, BucketConfig, [])},
                     %% {ffMap, proplists:get_value(fastForwardMap, BucketConfig, [])},
                     {perNodeStates, {struct, PerNodeStates}}]},
    Hash = integer_to_list(erlang:phash2(JSON)),
    ExtraHeaders = [{"Cache-Control", "must-revalidate"},
                    {"ETag", Hash}],
    case Req:get_header_value("if-none-match") of
        Hash ->
            menelaus_util:reply(Req, 304, ExtraHeaders);
        _ ->
            menelaus_util:reply_json(Req, JSON, 200, ExtraHeaders)
    end.

handle_diag_get_password(Req) ->
    menelaus_util:ensure_local(Req),
    menelaus_util:reply_text(Req, ns_config_auth:get_password(special), 200).

-ifdef(EUNIT).

split_incremental(Binary, Separator) ->
    R = split_fold_incremental(Binary, Separator,
                               fun (Part, Acc) ->
                                       [Part | Acc]
                               end, []),
    lists:reverse(R).

split_incremental_test() ->
    String1 = <<"abc\n\ntext">>,
    String2 = <<"abc\n\ntext\n">>,
    String3 = <<"\nabc\n\ntext\n">>,
    Split1a = binary:split(String1, <<"\n">>, [global]),
    Split2a = binary:split(String2, <<"\n">>, [global]),
    Split3a = binary:split(String3, <<"\n">>, [global]),
    ?assertEqual(Split1a, split_incremental(String1, <<"\n">>)),
    ?assertEqual(Split2a, split_incremental(String2, <<"\n">>)),
    ?assertEqual(Split3a, split_incremental(String3, <<"\n">>)).

-endif.
