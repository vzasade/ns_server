%% @author Couchbase, Inc <info@couchbase.com>
%% @copyright 2015 Couchbase, Inc.
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
-module(index_stats_collector).

-include_lib("eunit/include/eunit.hrl").

-include("ns_common.hrl").

-include("ns_stats.hrl").

%% API
-export([start_link/0]).
-export([per_index_stat/2, global_index_stat/1]).

%% callbacks
-export([init/1, handle_info/2, grab_stats/1, process_stats/5]).

-record(state, {default_stats,
                buckets}).

-define(I_GAUGES, [disk_size, data_size, num_docs_pending, num_docs_queued,
                   items_count, frag_percent]).
-define(I_COUNTERS, [num_requests, num_rows_returned, num_docs_indexed,
                     scan_bytes_read, total_scan_duration]).
-define(I_COMPUTED, [disk_overhead_estimate]).

start_link() ->
    base_stats_collector:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    ets:new(index_stats_collector_names, [protected, named_table]),

    Self = self(),
    ns_pubsub:subscribe_link(
      ns_config_events,
      fun ({buckets, Buckets}) ->
              BucketConfigs = proplists:get_value(configs, Buckets, []),
              Self ! {buckets, ns_bucket:get_bucket_names(membase, BucketConfigs)};
          (_) ->
              ok
      end),

    Buckets = lists:map(fun list_to_binary/1, ns_bucket:get_bucket_names(membase)),
    Defaults = [{global_index_stat(atom_to_binary(Stat, latin1)), 0}
                || Stat <- ?I_GAUGES ++ ?I_COUNTERS ++ ?I_COMPUTED],


    {ok, #state{buckets = Buckets,
                default_stats = Defaults}}.

find_type(_, []) ->
    not_found;
find_type(Name, [{Type, Metrics} | Rest]) ->
    MaybeMetric = [Name || M <- Metrics,
                           atom_to_binary(M, latin1) =:= Name],

    case MaybeMetric of
        [_] ->
            Type;
        _ ->
            find_type(Name, Rest)
    end.

do_recognize_name(<<"needs_restart">>) ->
    {status, index_needs_restart};
do_recognize_name(<<"num_connections">>) ->
    {status, index_num_connections};
do_recognize_name(K) ->
    case binary:split(K, <<":">>, [global]) of
        [Bucket, Index, Metric] ->
            Type = find_type(Metric, [{gauge, ?I_GAUGES},
                                      {counter, ?I_COUNTERS}]),

            case Type of
                not_found ->
                    undefined;
                _ ->
                    {Type, {Bucket, Index, Metric}}
            end;
        _ ->
            undefined
    end.

recognize_name(K) ->
    case ets:lookup(index_stats_collector_names, K) of
        [{K, Type, NewK}] ->
            {Type, NewK};
        [{K, undefined}] ->
            undefined;
        [] ->
            case do_recognize_name(K) of
                undefined ->
                    ets:insert(index_stats_collector_names, {K, undefined}),
                    undefined;
                {Type, NewK} ->
                    ets:insert(index_stats_collector_names, {K, Type, NewK}),
                    {Type, NewK}
            end
    end.

massage_stats([], AccGauges, AccCounters, AccStatus) ->
    {AccGauges, AccCounters, AccStatus};
massage_stats([{K, V} | Rest], AccGauges, AccCounters, AccStatus) ->
    case recognize_name(K) of
        undefined ->
            massage_stats(Rest, AccGauges, AccCounters, AccStatus);
        {counter, NewK} ->
            massage_stats(Rest, AccGauges, [{NewK, V} | AccCounters], AccStatus);
        {gauge, NewK} ->
            massage_stats(Rest, [{NewK, V} | AccGauges], AccCounters, AccStatus);
        {status, NewK} ->
            massage_stats(Rest, AccGauges, AccCounters, [{NewK, V} | AccStatus])
    end.

grab_stats(_State) ->
    case ns_cluster_membership:should_run_service(ns_config:latest(), index, node()) of
        true ->
            get_stats();
        false ->
            []
    end.

get_stats() ->
    case index_rest:get_json(index, "stats?async=true") of
        {ok, {[_|_] = Stats}} ->
            Stats;
        {ok, Other} ->
            ?log_error("Got invalid stats response:~n~p", [Other]),
            [];
        {error, _} ->
            []
    end.

process_stats(TS, GrabbedStats, PrevCounters, PrevTS, #state{buckets = KnownBuckets,
                                                             default_stats = Defaults} = State) ->
    {Gauges0, Counters, Status} = massage_stats(GrabbedStats, [], [], []),
    Gauges = compute_disk_overhead_estimates(Gauges0) ++ Gauges0,

    {Stats, SortedCounters} =
        base_stats_collector:calculate_counters(TS, Gauges, Counters, PrevCounters, PrevTS),

    index_status_keeper:update(index, Status),

    AggregatedStats =
        [{"@index-"++binary_to_list(Bucket), Values} ||
            {Bucket, Values} <- aggregate_index_stats(Stats, KnownBuckets, Defaults)],
    {AggregatedStats, SortedCounters, State}.

aggregate_index_stats(Stats, Buckets, Defaults) ->
    do_aggregate_index_stats(Stats, Buckets, Defaults, []).

do_aggregate_index_stats([], Buckets, Defaults, Acc) ->
    [{B, Defaults} || B <- Buckets] ++ Acc;
do_aggregate_index_stats([{{Bucket, _, _}, _} | _] = Stats,
                         Buckets, Defaults, Acc) ->
    {BucketStats, RestStats} = aggregate_index_bucket_stats(Bucket, Stats, Defaults),

    OtherBuckets = lists:delete(Bucket, Buckets),
    do_aggregate_index_stats(RestStats, OtherBuckets, Defaults,
                             [{Bucket, BucketStats} | Acc]).

aggregate_index_bucket_stats(Bucket, Stats, Defaults) ->
    do_aggregate_index_bucket_stats(Defaults, Bucket, Stats).

do_aggregate_index_bucket_stats(Acc, _, []) ->
    {finalize_index_bucket_stats(Acc), []};
do_aggregate_index_bucket_stats(Acc, Bucket, [{{Bucket, Index, Name}, V} | Rest]) ->
    Global = global_index_stat(Name),
    PerIndex = per_index_stat(Index, Name),

    Acc1 =
        case lists:keyfind(Global, 1, Acc) of
            false ->
                [{Global, V} | Acc];
            {_, OldV} ->
                lists:keyreplace(Global, 1, Acc, {Global, OldV + V})
        end,

    Acc2 = [{PerIndex, V} | Acc1],

    do_aggregate_index_bucket_stats(Acc2, Bucket, Rest);
do_aggregate_index_bucket_stats(Acc, _, Stats) ->
    {finalize_index_bucket_stats(Acc), Stats}.

finalize_index_bucket_stats(Acc) ->
    lists:keysort(1, Acc).

aggregate_index_stats_test() ->
    In = [{{<<"a">>, <<"idx1">>, <<"m1">>}, 1},
          {{<<"a">>, <<"idx1">>, <<"m2">>}, 2},
          {{<<"b">>, <<"idx2">>, <<"m1">>}, 3},
          {{<<"b">>, <<"idx2">>, <<"m2">>}, 4},
          {{<<"b">>, <<"idx3">>, <<"m1">>}, 5},
          {{<<"b">>, <<"idx3">>, <<"m2">>}, 6}],
    Out = aggregate_index_stats(In, [], []),

    AStats0 = [{<<"index/idx1/m1">>, 1},
               {<<"index/idx1/m2">>, 2},
               {<<"index/m1">>, 1},
               {<<"index/m2">>, 2}],
    BStats0 = [{<<"index/idx2/m1">>, 3},
               {<<"index/idx2/m2">>, 4},
               {<<"index/idx3/m1">>, 5},
               {<<"index/idx3/m2">>, 6},
               {<<"index/m1">>, 3+5},
               {<<"index/m2">>, 4+6}],

    AStats = lists:keysort(1, AStats0),
    BStats = lists:keysort(1, BStats0),

    ?assertEqual(Out,
                 [{<<"b">>, BStats},
                  {<<"a">>, AStats}]).

handle_info({buckets, NewBuckets}, State) ->
    NewBuckets1 = lists:map(fun list_to_binary/1, NewBuckets),
    {noreply, State#state{buckets = NewBuckets1}};
handle_info(_Info, State) ->
    {noreply, State}.

per_index_stat(Index, Metric) ->
    iolist_to_binary([<<"index/">>, Index, $/, Metric]).

global_index_stat(StatName) ->
    iolist_to_binary([<<"index/">>, StatName]).

compute_disk_overhead_estimates(Stats) ->
    Dict = lists:foldl(
             fun ({StatKey, Value}, D) ->
                     {Bucket, Index, Metric} = StatKey,
                     Key = {Bucket, Index},

                     case Metric of
                         <<"frag_percent">> ->
                             misc:dict_update(
                               Key,
                               fun ({_, DiskSize}) ->
                                       {Value, DiskSize}
                               end, {undefined, undefined}, D);
                         <<"disk_size">> ->
                             misc:dict_update(
                               Key,
                               fun ({Frag, _}) ->
                                       {Frag, Value}
                               end, {undefined, undefined}, D);
                         _ ->
                             D
                     end
             end, dict:new(), Stats),

    dict:fold(
      fun ({Bucket, Index}, {Frag, DiskSize}, Acc) ->
              if
                  Frag =/= undefined andalso DiskSize =/= undefined ->
                      Est = (DiskSize * Frag) div 100,
                      [{{Bucket, Index, <<"disk_overhead_estimate">>}, Est} | Acc];
                  true ->
                      Acc
              end
      end, [], Dict).

compute_disk_overhead_estimates_test() ->
    In = [{{<<"a">>, <<"idx1">>, <<"disk_size">>}, 100},
          {{<<"a">>, <<"idx1">>, <<"frag_percent">>}, 0},
          {{<<"b">>, <<"idx2">>, <<"frag_percent">>}, 100},
          {{<<"b">>, <<"idx2">>, <<"disk_size">>}, 100},
          {{<<"b">>, <<"idx3">>, <<"disk_size">>}, 100},
          {{<<"b">>, <<"idx3">>, <<"frag_percent">>}, 50},
          {{<<"b">>, <<"idx3">>, <<"m">>}, 42}],
    Out = lists:keysort(1, compute_disk_overhead_estimates(In)),

    Expected0 = [{{<<"a">>, <<"idx1">>, <<"disk_overhead_estimate">>}, 0},
                 {{<<"b">>, <<"idx2">>, <<"disk_overhead_estimate">>}, 100},
                 {{<<"b">>, <<"idx3">>, <<"disk_overhead_estimate">>}, 50}],
    Expected = lists:keysort(1, Expected0),

    ?assertEqual(Expected, Out).
