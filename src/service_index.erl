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
-module(service_index).

-include("ns_common.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([start_keeper/0, get_status/1, get_indexes/0, get_indexes_version/0]).

-export([get_type/0, get_remote_items/1, get_local_status/0, restart/0,
         get_gauges/0, get_counters/0, get_computed/0, grab_stats/0,
         compute_gauges/1, get_service_gauges/0, compute_service_gauges/1, get_service_counters/0,
         process_status/1, process_status/2, split_stat_name/1]).

get_status(Timeout) ->
    service_status_keeper:get_status(?MODULE, Timeout).

get_indexes() ->
    service_status_keeper:get_items(?MODULE).

get_indexes_version() ->
    service_status_keeper:get_version(?MODULE).

get_type() ->
    index.

get_port() ->
    ns_config:read_key_fast({node, node(), indexer_http_port}, 9102).

get_timeout() ->
    ns_config:get_timeout(index_rest_request, 10000).

get_remote_items(Node) ->
    remote_api:get_indexes(Node).

get_local_status() ->
    rest_utils:get_json_local(indexer, "getIndexStatus", get_port(), get_timeout()).

restart() ->
    ns_ports_setup:restart_port_by_name(indexer).

status_mapping() ->
    AddType = case cluster_compat_mode:is_cluster_45() of
                  true ->
                      [{storageMode, <<"indexType">>}];
                  false ->
                      []
              end,
    [{id, <<"defnId">>},
     {index, <<"name">>},
     {bucket, <<"bucket">>},
     {status, <<"status">>},
     {definition, <<"definition">>},
     {progress, <<"completion">>},
     {hosts, <<"hosts">>} | AddType].

process_status(Status) ->
    process_status(Status, status_mapping()).

process_status({[_|_] = Status}, Mapping) ->
    case lists:keyfind(<<"code">>, 1, Status) of
        {_, <<"success">>} ->
            RawIndexes =
                case lists:keyfind(<<"status">>, 1, Status) of
                    false ->
                        [];
                    {_, V} ->
                        V
                end,

            {ok, process_indexes(RawIndexes, Mapping)};
        _ ->
            ?log_error("Indexer returned unsuccessful status:~n~p", [Status]),
            {error, bad_status}
    end;
process_status(Other, _Mapping) ->
    ?log_error("Got invalid status: ~p", [Other]),
    {error, bad_status}.

process_indexes(Indexes, Mapping) ->
    lists:map(
      fun ({Index}) ->
              lists:foldl(fun ({Key, BinKey}, Acc) when is_atom(Key) ->
                                  {_, Val} = lists:keyfind(BinKey, 1, Index),
                                  [{Key, Val} | Acc];
                              ({ListOfKeys, BinKey}, Acc) when is_list(ListOfKeys) ->
                                  {_, Val} = lists:keyfind(BinKey, 1, Index),
                                  lists:foldl(fun (Key, Acc1) ->
                                                      [{Key, Val} | Acc1]
                                              end, Acc, ListOfKeys)
                          end, [], Mapping)
      end, Indexes).

start_keeper() ->
    service_status_keeper:start_link(?MODULE).

get_gauges() ->
    [disk_size, data_size, num_docs_pending, num_docs_queued,
     items_count, frag_percent].

get_counters() ->
    [num_requests, num_rows_returned, num_docs_indexed,
     scan_bytes_read, total_scan_duration].

get_service_gauges() ->
    [memory_quota, memory_used].

get_service_counters() ->
    [].

get_computed() ->
    [disk_overhead_estimate].

grab_stats() ->
    rest_utils:get_json_local(indexer, "stats?async=true", get_port(), get_timeout()).

compute_service_gauges(Gauges) ->
    compute_index_ram_usage_stats(Gauges).

compute_index_ram_usage_stats(Stats) ->
    Quota = proplists:get_value(<<"index_memory_quota">>, Stats, undefined),
    Used = proplists:get_value(<<"index_memory_used">>, Stats, undefined),
    case Quota =/= undefined andalso Used =/= undefined of
        true ->
            [{<<"index_ram_percent">>, min(((Used / Quota) * 100), 100)},
             {<<"index_remaining_ram">>, max(Quota - Used, 0)}];
        false ->
            []
    end.

compute_gauges(Gauges) ->
    compute_disk_overhead_estimates(Gauges).

split_stat_name(Name) ->
    binary:split(Name, <<":">>, [global]).

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
