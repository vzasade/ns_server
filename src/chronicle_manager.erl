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
         rename/1,
         leave/0,
         join_node/2,
         remove_node/1,
         upgrade/1,
         get/4,
         get/3,
         transaction/3,
         set_multiple/2]).

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
handle_call({rename, OldNode}, _From, Pid) ->
    ?log_debug("Handle renaming"),
    List = chronicle_kv:submit_query(kv, get_snapshot, 10000, #{}),
    NewPid = reprovision(Pid),
    NewNode = node(),
    lists:foreach(
      fun ({kv, Key, Value, _Revision}) ->
              chronicle_kv:add(
                kv,
                misc:rewrite_value(OldNode, NewNode, Key),
                misc:rewrite_value(OldNode, NewNode, Value))
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

rename(OldNode) ->
    gen_server2:call(?MODULE, {rename, OldNode}).

join_node(Node, CompatVer) ->
    case enabled(CompatVer) of
        true ->
            gen_server2:call(?MODULE, {join_node, Node});
        false ->
            ok
    end.

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

enabled() ->
    enabled(cluster_compat_mode:effective_cluster_compat_version_for(
              cluster_compat_mode:get_compat_version())).

enabled(CompatVersion) ->
    ns_node_disco:couchdb_node() =/= node() andalso
        CompatVersion >=
        cluster_compat_mode:effective_cluster_compat_version_for(
          ?VERSION_CHESHIRECAT).

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


get(Config, Key, Default, Opts) ->
    case get(Config, Key, Opts) of
        {error, not_found} ->
            Default;
        {ok, Value} ->
            Value
    end.

get(Config, Key, Opts) ->
    case get_with_revision(Config, Key, Opts) of
        {error, not_found} ->
            {error, not_found};
        {ok, {Value, _}} ->
            {ok, Value}
    end.

get_with_revision(Snapshot, Key, _Opts) when is_list(Snapshot) ->
    case lists:keysearch(Key, 2, Snapshot) of
        {value, {kv, Key, V, Revision}} ->
            {ok, {V, Revision}};
        false ->
            {error, not_found}
    end;
get_with_revision(Snapshot, Key, _Opts) when is_map(Snapshot) ->
    case maps:find(Key, Snapshot) of
        {ok, VR} ->
            {ok, VR};
        error ->
            {error, not_found}
    end;
get_with_revision(Config, Key, Opts) ->
    case enabled() of
        true ->
            chronicle_kv:get(kv, Key, Opts);
        false ->
            case ns_config:search(Config, Key) of
                {value, Value} ->
                    {ok, {Value, no_revision}};
                false ->
                    {error, not_found}
            end
    end.

set_multiple(List, Opts) ->
    case enabled() of
        true ->
            chronicle_kv:transaction(
              kv, [],
              fun (_) ->
                      {commit, [{set, K, V} || {K, V} <- List]}
              end, Opts);
        false ->
            ns_config:set(List)
    end.

transaction(Keys, Fun, Opts) ->
    case enabled() of
        true ->
            chronicle_kv:transaction(kv, Keys, Fun, Opts);
        false ->
            RV =
                ns_config:run_txn(
                  fun (Config, Set) ->
                          Snapshot =
                              maps:from_list(
                                lists:filtermap(
                                  fun (Key) ->
                                          case ns_config:search(Config, Key) of
                                              {value, V} ->
                                                  {true,
                                                   {Key, {V, no_revision}}};
                                              false ->
                                                  false
                                          end
                                  end, Keys)),
                          case Fun(Snapshot) of
                              {commit, List} ->
                                  {commit,
                                   lists:foldl(
                                     fun ({set, Key, Value}, Acc) ->
                                             Set(Key, Value, Acc);
                                         ({delete, Key}, Acc) ->
                                             Set(Key, ?DELETED_MARKER, Acc)
                                     end, Config, List)};
                              {abort, Error} ->
                                  {abort, Error}
                          end
                  end),
            case RV of
                {commit, _} ->
                    {ok, no_revision};
                {abort, Error} ->
                    Error;
                retry_needed ->
                    erlang:error(exceeded_retries)
            end
    end.

should_move(nodes_wanted) ->
    true;
should_move(server_groups) ->
    true;
should_move({node, _, membership}) ->
    true;
should_move({node, _, services}) ->
    true;
should_move(_) ->
    false.

upgrade(Config) ->
    ?log_debug("Chronicle content before the upgrade ~p",
               [chronicle_kv:submit_query(kv, get_snapshot, 10000, #{})]),
    ns_config:fold(
      fun (Key, Value, Acc) ->
              case should_move(Key) of
                  true ->
                      {ok, Rev} = chronicle_kv:set(kv, Key, Value),
                      ?log_debug("Key ~p is migrated to chronicle. Rev = ~p."
                                 "Value = ~p",
                                 [Key, Rev, Value]),
                      [Key | Acc];
                  false ->
                      Acc
              end
      end, [], Config).
