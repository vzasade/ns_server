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
-module(index_rest).

-include("ns_common.hrl").

-export([get_json/2, get_port_name/1]).

get_timeout(index) ->
    ns_config:get_timeout(index_rest_request, 10000);
get_timeout(fts) ->
    ns_config:get_timeout(fts_rest_request, 10000).

get_port(index) ->
    ns_config:read_key_fast({node, node(), indexer_http_port}, 9102);
get_port(fts) ->
    ns_config:read_key_fast({node, node(), fts_http_port}, 9110).

get_port_name(index) ->
    indexer;
get_port_name(fts) ->
    fts.

get_json(Type, Path) ->
    URL = lists:flatten(io_lib:format("http://127.0.0.1:~B/~s", [get_port(Type), Path])),

    User = ns_config_auth:get_user(special),
    Pwd = ns_config_auth:get_password(special),

    Headers = menelaus_rest:add_basic_auth([], User, Pwd),

    RV = rest_utils:request(get_port_name(Type), URL, "GET", Headers, [], get_timeout(Type)),
    case RV of
        {ok, {{200, _}, _Headers, BodyRaw}} ->
            try
                {ok, ejson:decode(BodyRaw)}
            catch
                T:E ->
                    ?log_error("Received bad json in response from (~p) ~s: ~p",
                               [Type, URL, {T, E}]),
                    {error, bad_json}
            end;
        _ ->
            ?log_error("Request to (~p) ~s failed: ~p", [Type, URL, RV]),
            {error, RV}
    end.
