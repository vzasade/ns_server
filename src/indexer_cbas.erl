%% @author Couchbase, Inc <info@couchbase.com>
%% @copyright 2018 Couchbase, Inc.
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
-module(indexer_cbas).

-include("ns_common.hrl").

-export([get_type/0, restart/0,
         get_gauges/0, get_counters/0, get_computed/0, grab_stats/0, prefix/0,
         per_index_stat/2, global_index_stat/1, compute_gauges/1, get_service_gauges/0,
         get_service_counters/0, service_stat_prefix/0, service_event_name/0, compute_service_gauges/1]).

get_type() ->
    cbas.

restart() ->
    [].

get_gauges() ->
    ['incoming-records-count-total', 'failed-at-parser-records-count-total'].

get_counters() ->
    ['incoming-records-count', 'failed-at-parser-records-count'].

get_computed() ->
    [].

get_service_gauges() ->
    ['heap-used', 'system-load-average', 'thread-count'].

get_service_counters() ->
    ['gc-count', 'gc-time', 'io-reads', 'io-writes'].

grab_stats() ->
    cbas_rest:get_stats().

prefix() ->
    "@" ++ atom_to_list(get_type()) ++ "-".

service_stat_prefix() ->
    atom_to_list(get_type()) ++ "_".

service_event_name() ->
    "@" ++ atom_to_list(get_type()).

per_index_stat(Index, Metric) ->
    iolist_to_binary([atom_to_list(get_type()), $/, Index, $/, Metric]).

global_index_stat(StatName) ->
    iolist_to_binary([atom_to_list(get_type()), $/, StatName]).

compute_service_gauges(_Gauges) ->
    [].

compute_gauges(_Gauges) ->
    [].
