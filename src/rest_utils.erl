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
-module(rest_utils).

-include("ns_common.hrl").

-export([request/6, get_json_local/4]).

request(Type, URL, Method, Headers, Body, Timeout) ->
    system_stats_collector:increment_counter({Type, requests}, 1),

    Start = os:timestamp(),
    RV = lhttpc:request(URL, Method, Headers, Body, Timeout,
                        [{pool, rest_lhttpc_pool}]),
    case RV of
        {ok, {{Code, _}, _, _}} ->
            Diff = timer:now_diff(os:timestamp(), Start),
            system_stats_collector:add_histo({Type, latency}, Diff),

            Class = (Code div 100) * 100,
            system_stats_collector:increment_counter({Type, status, Class}, 1);
        _ ->
            system_stats_collector:increment_counter({Type, failed_requests}, 1)
    end,

    RV.

request_local(Type, Path, Port, Method, Headers, Body, Timeout) ->
    URL = misc:local_url(Port, Path, []),
    User = ns_config_auth:get_user(special),
    Pwd = ns_config_auth:get_password(special),

    HeadersWithAuth = menelaus_rest:add_basic_auth(Headers, User, Pwd),

    request(Type, URL, Method, HeadersWithAuth, Body, Timeout).

get_json_local(Type, Path, Port, Timeout) ->
    RV = request_local(Type, Path, Port, "GET", [], [], Timeout),
    case RV of
        {ok, {{200, _}, _Headers, BodyRaw}} ->
            try
                {ok, ejson:decode(BodyRaw)}
            catch
                T:E ->
                    ?log_error("Received bad json in response from (~p) ~s: ~p",
                               [Type, Path, {T, E}]),
                    {error, bad_json}
            end;
        _ ->
            ?log_error("Request to (~p) ~s failed: ~p", [Type, Path, RV]),
            {error, RV}
    end.
