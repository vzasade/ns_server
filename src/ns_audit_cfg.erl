%% @author Couchbase <info@couchbase.com>
%% @copyright 2014-2017 Couchbase, Inc.
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
%% @doc server for maintaining audit configuration file
%%
-module(ns_audit_cfg).

-behaviour(gen_server).

-include("cut.hrl").
-include("ns_common.hrl").

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-export([start_link/0, get_global/0, set_global/1, default_audit_json_path/0,
         get_log_path/0]).

-export([upgrade_descriptors/0, upgrade_to_vulcan/1, get_descriptors/1,
         jsonifier/1]).

-record(state, {global,
                merged}).

jsonifier(log_path) ->
    fun list_to_binary/1;
jsonifier(descriptors_path) ->
    fun list_to_binary/1;
jsonifier(uuid) ->
    fun list_to_binary/1;
jsonifier(_) ->
    fun functools:id/1.

version(CompatMode) ->
    case cluster_compat_mode:is_version_vulcan(CompatMode) of
        true ->
            2;
        false ->
            1
    end.

fields(1) ->
    [version,
     auditd_enabled,
     log_path,
     rotate_interval,
     rotate_size,
     descriptors_path,
     disabled,
     sync];
fields(2) ->
    fields(1) ++
        [disabled_users,
         uuid,
         filtering_enabled].

is_notable_config_key(audit) ->
    true;
is_notable_config_key({node, N, audit}) ->
    N =:= node();
is_notable_config_key(cluster_compat_version) ->
    true;
is_notable_config_key(_) ->
    false.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

get_global() ->
    gen_server:call(?MODULE, get_global).

set_global(KVList) ->
    ns_config:set_sub(audit, KVList).

init([]) ->
    {Global, Local} = read_config(),
    CompatMode = cluster_compat_mode:get_compat_version(),
    Merged = prepare_params(Global, Local, CompatMode),

    Self = self(),
    ns_pubsub:subscribe_link(ns_config_events,
                             fun ({Key, _}) ->
                                     case is_notable_config_key(Key) of
                                         true ->
                                             Self ! update_audit_json;
                                         _ ->
                                             []
                                     end;
                                 (_Other) ->
                                     []
                             end),

    write_audit_json(CompatMode, Merged),
    {ok, #state{global = Global, merged = Merged}}.

handle_call(get_global, _From, #state{global = Global} = State) ->
    {reply, Global, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(notify_memcached, State) ->
    misc:flush(notify_memcached),
    ?log_debug("Instruct memcached to reload audit config"),
    ok = ns_memcached_sockets_pool:executing_on_socket(
           fun (Sock) ->
                   mc_client_binary:audit_config_reload(Sock)
           end),
    {noreply, State};

handle_info(update_audit_json, #state{merged = OldMerged}) ->
    misc:flush(update_audit_json),
    {Global, Local} = read_config(),
    CompatMode = cluster_compat_mode:get_compat_version(),
    Merged = prepare_params(Global, Local, CompatMode),

    case Merged of
        OldMerged ->
            ok;
        _ ->
            write_audit_json(CompatMode, Merged)
    end,
    {noreply, #state{global = Global, merged = Merged}}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

default_audit_json_path() ->
    filename:join(path_config:component_path(data, "config"), "audit.json").

audit_json_path() ->
    ns_config:search_node_prop(ns_config:latest(), memcached, audit_file).

is_enabled() ->
    ns_config:search_node_prop(
      ns_config:latest(), audit, auditd_enabled, false).

get_log_path() ->
    case is_enabled() of
        false ->
            undefined;
        true ->
            case ns_config:search_node_prop(
                   ns_config:latest(), audit, log_path) of
                undefined ->
                    undefined;
                Path ->
                    {ok, misc:absname(Path)}
            end
    end.

prepare_params(Global, Local, CompatMode) ->
    Merged = lists:ukeymerge(1, Local, Global),
    massage_params(version(CompatMode), CompatMode, Merged).

calculate_events(Params) ->
    %% leave only those events that change the default
    Descriptors = orddict:from_list((get_descriptors(ns_config:latest()))),
    Enabled = proplists:get_value(enabled, Params, []),
    Disabled = proplists:get_value(disabled, Params, []),

    Filter =
        fun (List, IsEnabled) ->
                lists:filter(
                  fun (Id) ->
                          case orddict:find(Id, Descriptors) of
                              {ok, Props} ->
                                  proplists:get_value(enabled, Props)
                                      =/= IsEnabled;
                              error ->
                                  false
                          end
                  end, List)
        end,
    {Filter(Enabled, true), Filter(Disabled, false)}.

massage_params(1, _CompatMode, Params) ->
    Params;
massage_params(2, CompatMode, Params) ->
    {Enabled, Disabled} = calculate_events(Params),

    FilteringEnabled = Enabled =/= [] orelse
        Disabled =/= [] orelse
        proplists:get_value(disabled_users, Params, []) =/= [],

    NewParams =
        misc:update_proplist(Params, [{enabled, Enabled},
                                      {disabled, Disabled},
                                      {filtering_enabled, FilteringEnabled}]),

    UUID = integer_to_list(erlang:phash2({NewParams, CompatMode})),

    [{uuid, UUID} | NewParams].

write_audit_json(CompatMode, Params) ->
    Version = version(CompatMode),
    CompleteParams = [{descriptors_path, path_config:component_path(sec)},
                      {version, Version}] ++ Params,

    Path = audit_json_path(),

    Fields = fields(Version),
    Json = lists:filtermap(
             fun({K, V}) ->
                     case lists:member(K, Fields) of
                         false ->
                             false;
                         true ->
                             {true, {K, (jsonifier(K))(V)}}
                     end
             end, CompleteParams),
    ?log_debug("Writing new content to ~p : ~p", [Path, Json]),

    Bytes = misc:ejson_encode_pretty({Json}),
    ok = misc:atomic_write_file(Path, Bytes),
    self() ! notify_memcached.

read_config() ->
    {case ns_config:search(audit) of
         {value, V} ->
             lists:keysort(1, V);
         false ->
             []
     end,
     case ns_config:search({node, node(), audit}) of
         {value, V} ->
             lists:keysort(1, V);
         false ->
             []
     end}.

get_descriptors(Config) ->
    ns_config:search(Config, audit_decriptors, []).

editable(n1ql) ->
    true;
editable(_) ->
    false.

read_descriptors() ->
    Path = filename:join(path_config:component_path(sec), "audit_events.json"),
    {ok, Bin} = file:read_file(Path),
    {Json} = ejson:decode(Bin),
    1 = proplists:get_value(<<"version">>, Json),
    Modules = proplists:get_value(<<"modules">>, Json),
    lists:flatmap(
      fun ({Module}) ->
              ModuleIdBin = proplists:get_value(<<"module">>, Module),
              ModuleId = list_to_atom(binary_to_list(ModuleIdBin)),
              case editable(ModuleId) of
                  true ->
                      Events = proplists:get_value(<<"events">>, Module),
                      lists:map(
                        fun ({Event}) ->
                                {proplists:get_value(<<"id">>, Event),
                                 [{name,
                                   proplists:get_value(<<"name">>, Event)},
                                  {description,
                                   proplists:get_value(<<"description">>,
                                                       Event)},
                                  {enabled,
                                   proplists:get_value(<<"enabled">>, Event)},
                                  {module, ModuleId}]}
                        end, Events);
                  false ->
                      []
              end
      end, Modules).

upgrade_descriptors() ->
    [{set, audit_decriptors, lists:ukeysort(1, read_descriptors())}].

upgrade_to_vulcan(Config) ->
    {value, Current} = ns_config:search(Config, audit),
    New =
        functools:chain(
          Current,
          [lists:keystore(enabled, 1, _, {enabled, []}),
           lists:keystore(disabled_users, 1, _, {disabled_users, []})]),
    [{set, audit, New}].
