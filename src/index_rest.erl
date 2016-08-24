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

-export([get_json/4]).

get_json(Type, Path, Port, Timeout) ->
    URL = lists:flatten(io_lib:format("http://127.0.0.1:~B/~s", [Port, Path])),

    Headers = menelaus_cbauth:add_local_auth_headers([]),

    RV = rest_utils:request(Type, URL, "GET", Headers, [], Timeout),
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
