%% @author Couchbase <info@couchbase.com>
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
-module(chronicle_manager).

-include("ns_common.hrl").
-include("cut.hrl").

-export([bootstrap/0, rename/0, leave/0, do_join_node/1, join_node/1,
         remove_node/1]).

bootstrap() ->
    application:set_env(chronicle, data_dir,
                        path_config:component_path(data, "config")),
    ok = application:ensure_started(chronicle, permanent),
    ok = maybe_provision(),
    ignore.

leave() ->
    ok = wipe(),
    maybe_provision().

rename() ->
    leave().

maybe_provision() ->
    case chronicle_agent:get_metadata() of
        {error, not_provisioned} ->
            ?log_debug("Chronicle is not provisioned."),
            chronicle:provision([{kv, chronicle_kv, []}]);
        {ok, M} ->
            ?log_debug("Chronicle is already provisioned ~p", [M]),
            ok
    end.

wipe() ->
    ?log_debug("Wiping the config"),
    chronicle_agent:wipe().

do_join_node(Node) ->
    ?log_debug("Adding node ~p", [Node]),
    chronicle:add_voters([Node]).

join_node(Node) ->
    ok = wipe(),
    ?log_debug("Joining to ~p", [Node]),
    rpc:call(Node, ?MODULE, do_join_node, [node()], 20000).

remove_node(Node) ->
    ?log_debug("Removing node ~p", [Node]),
    chronicle:remove_voters([Node]).
