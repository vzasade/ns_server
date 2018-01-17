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
-module(service_fts).

-export([start_keeper/0, get_indexes/0]).

-export([get_type/0, get_remote_items/1, get_local_status/0, restart/0, process_status/1,
         get_gauges/0, get_counters/0, get_computed/0, grab_stats/0,
         per_index_stat/2, global_index_stat/1, compute_gauges/1,
         get_service_gauges/0,
         compute_service_gauges/1, get_service_counters/0]).

get_indexes() ->
    service_status_keeper:get_items(?MODULE).

get_type() ->
    fts.

get_port() ->
    ns_config:read_key_fast({node, node(), fts_http_port}, 8094).

get_timeout() ->
    ns_config:get_timeout(fts_rest_request, 10000).

get_remote_items(Node) ->
    remote_api:get_fts_indexes(Node).

get_local_status() ->
    rest_utils:get_json_local(fts, "api/nsstatus", get_port(), get_timeout()).

restart() ->
    ns_ports_setup:restart_port_by_name(fts).

status_mapping() ->
    [{[index, id], <<"name">>},
     {bucket, <<"bucket">>},
     {hosts, <<"hosts">>}].

process_status(Status) ->
    service_index:process_status(Status, status_mapping()).

start_keeper() ->
    service_status_keeper:start_link(?MODULE).

get_gauges() ->
    [num_mutations_to_index, doc_count, num_recs_to_persist, num_bytes_used_disk,
    num_pindexes_actual, num_pindexes_target].

get_counters() ->
    [total_bytes_indexed, total_compaction_written_bytes, total_queries,
    total_queries_slow, total_queries_timeout, total_queries_error,
    total_bytes_query_results, total_term_searchers, total_request_time].

get_computed() ->
    [].

get_service_gauges() ->
    [num_bytes_used_ram].

get_service_counters() ->
    [].

grab_stats() ->
    rest_utils:get_json_local(fts, "api/nsstats", get_port(), get_timeout()).

per_index_stat(Index, Metric) ->
    iolist_to_binary([atom_to_list(get_type()), $/, Index, $/, Metric]).

global_index_stat(StatName) ->
    iolist_to_binary([atom_to_list(get_type()), $/, StatName]).

compute_service_gauges(_Gauges) ->
    [].

compute_gauges(_Gauges) ->
    [].
