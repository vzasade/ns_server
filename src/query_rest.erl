%% @author Couchbase <info@couchbase.com>
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
%% @doc this module implements access to cbq-engine via REST API
%%

-module(query_rest).

-include("ns_common.hrl").

-export([get_query_port/2,
         get_ssl_query_port/2,
         get_stats/0,
         maybe_refresh_cert/0]).

get_query_port(Config, Node) ->
    ns_config:search(Config, {node, Node, query_port}, undefined).

get_ssl_query_port(Config, Node) ->
    ns_config:search(Config, {node, Node, ssl_query_port}, undefined).

get_stats() ->
    case ns_cluster_membership:should_run_service(ns_config:latest(), n1ql, node()) of
        true ->
            do_get_stats();
        false ->
            []
    end.

do_get_stats() ->
    Port = get_query_port(ns_config:latest(), node()),
    Timeout = ns_config:get_timeout({n1ql, stats}, 30000),
    case rest_utils:get_json_local(n1ql, "/admin/stats", Port, Timeout) of
        {ok, {Stats}} ->
            Stats;
        _Error ->
            []
    end.

refresh_cert() ->
    Port = get_query_port(ns_config:latest(), node()),
    Timeout = ns_config:get_timeout({n1ql, refresh_cert}, 30000),

    ?log_debug("Tell cbq-engine to refresh ssl certificate"),
    try rest_utils:request_local(n1ql, "/admin/ssl_cert", Port, "POST", [], [], Timeout) of
        {ok, {{200, _}, _Headers, _Body}} ->
            ok
    catch
        error:{badmatch, {error, {econnrefused, _}}} ->
            ?log_debug("Failed to notify cbq-engine because it is not started yet. This is usually normal")
    end.

maybe_refresh_cert() ->
    case ns_cluster_membership:should_run_service(ns_config:latest(), n1ql, node()) of
        true ->
            case get_ssl_query_port(ns_config:latest(), node()) of
                undefined ->
                    ok;
                _ ->
                    refresh_cert()
            end;
        false->
            ok
    end.
