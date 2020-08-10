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

-behaviour(gen_server2).

-include("ns_common.hrl").
-include("ns_config.hrl").
-include("cut.hrl").

-export([start_link/0,
         init/1,
         handle_call/3,
         rename/0,
         leave/0,
         join_node/1,
         remove_node/1]).

start_link() ->
    gen_server2:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    application:set_env(chronicle, data_dir,
                        path_config:component_path(data, "config")),
    ok = application:ensure_started(chronicle, permanent),

    ok = maybe_provision(),
    {ok, subscribe_to_events()}.

handle_call(leave, _From, Pid) ->
    ?log_debug("Handle leaving cluster"),
    {reply, ok, reprovision(Pid)};
handle_call(rename, _From, Pid) ->
    ?log_debug("Handle renaming"),
    List = chronicle_kv:submit_query(kv, get_snapshot, 10000, #{}),
    NewPid = reprovision(Pid),
    lists:foreach(
      fun ({kv, Key, Value, _Revision}) ->
              chronicle_kv:add(kv, Key, Value)
      end, List),
    {reply, ok, NewPid};
handle_call({add_node, Node}, _From, Pid) ->
    ?log_debug("Adding node ~p", [Node]),
    chronicle:add_voters([Node]),
    {reply, ok, Pid};
handle_call({join_node, Node}, _From, Pid) ->
    ok = wipe(Pid),
    ?log_debug("Joining to ~p", [Node]),
    gen_server2:call({?MODULE, Node}, {add_node, node()}),
    {reply, ok, subscribe_to_events()}.

leave() ->
    gen_server2:call(?MODULE, leave).

rename() ->
    gen_server2:call(?MODULE, rename).

join_node(Node) ->
    gen_server2:call(?MODULE, {join_node, Node}).

reprovision(Pid) ->
    ok = wipe(Pid),
    ok = maybe_provision(),
    subscribe_to_events().

subscribe_to_events() ->
    ns_pubsub:subscribe_link(
      chronicle_kv:event_manager(kv),
      fun ({{key, Key}, _Rev, {updated, Value}}) ->
              gen_event:notify(ns_config_events, {Key, Value});
          ({{key, Key}, _Rev, deleted}) ->
              gen_event:notify(ns_config_events, {Key, ?DELETED_MARKER})
      end).

maybe_provision() ->
    case chronicle_agent:get_metadata() of
        {error, not_provisioned} ->
            ?log_debug("Chronicle is not provisioned."),
            chronicle:provision([{kv, chronicle_kv, []}]);
        {ok, M} ->
            ?log_debug("Chronicle is already provisioned ~p", [M]),
            ok
    end.

wipe(Pid) ->
    ?log_debug("Wiping the config"),
    ns_pubsub:unsubscribe(Pid),
    chronicle_agent:wipe().

remove_node(Node) ->
    ?log_debug("Removing node ~p", [Node]),
    chronicle:remove_voters([Node]).
