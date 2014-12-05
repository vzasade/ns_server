%% @author Northscale <info@northscale.com>
%% @copyright 2009 NorthScale, Inc.
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
-module(ns_config_log).

-behaviour(gen_server).

-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3, sanitize/1]).

-export([doc/0]).

-include("ns_common.hrl").

-record(state, {buckets=[]}).

%% state sanitization
-export([format_status/2]).

doc() ->
    {gen_server, ?MODULE, "logs config changes"}.

format_status(_Opt, [_PDict, State]) ->
    sanitize(State).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    Self = self(),
    ns_pubsub:subscribe_link(ns_config_events,
                             fun (KVList) when is_list(KVList) ->
                                     Self ! {config_change, KVList};
                                 (_) ->
                                     ok
                             end),
    {ok, #state{}}.

terminate(_Reason, _State)     -> ok.
code_change(_OldVsn, State, _) -> {ok, State}.

% Don't log values for some password/auth-related config values.

handle_call(Request, From, State) ->
    ?log_warning("Unexpected handle_call(~p, ~p, ~p)", [Request, From, State]),
    {reply, ok, State, hibernate}.

handle_cast(Request, State) ->
    ?log_warning("Unexpected handle_cast(~p, ~p)", [Request, State]),
    {noreply, State, hibernate}.

handle_info({config_change, KVList}, State) ->
    NewState =
        lists:foldl(
          fun (KV, Acc) ->
                  log_kv(KV, Acc)
          end, State, KVList),
    {noreply, NewState, hibernate};
handle_info(Info, State) ->
    ?log_warning("Unexpected handle_info(~p, ~p)", [Info, State]),
    {noreply, State, hibernate}.

%% Internal functions
compute_buckets_diff(NewBuckets, OldBuckets) ->
    OldConfigs = proplists:get_value(configs, OldBuckets, []),
    NewConfigs = proplists:get_value(configs, NewBuckets, []),

    Diffed =
        merge_bucket_configs(
          fun (NewValue, OldValue) ->
                  OldMap = proplists:get_value(map, OldValue, []),
                  NewMap = proplists:get_value(map, NewValue, []),
                  MapDiff = misc:compute_map_diff(NewMap, OldMap),

                  OldFFMap = proplists:get_value(fastForwardMap, OldValue, []),
                  NewFFMap = proplists:get_value(fastForwardMap, NewValue, []),
                  FFMapDiff = misc:compute_map_diff(NewFFMap, OldFFMap),

                  misc:update_proplist(
                    NewValue,
                    [{map, MapDiff},
                     {fastForwardMap, FFMapDiff}])
          end, NewConfigs, OldConfigs),

    misc:update_proplist(NewBuckets, [{configs, Diffed}]).

sanitize(Config) ->
    misc:rewrite_tuples(fun (T) ->
                                case T of
                                    {password, _} ->
                                        {stop, {password, "*****"}};
                                    {sasl_password, _} ->
                                        {stop, {sasl_password, "*****"}};
                                    {admin_pass, _} ->
                                        {stop, {admin_pass, "*****"}};
                                    {pass, _} ->
                                        {stop, {pass, "*****"}};
                                    {cert_and_pkey, [VClock|{Cert, _PKey}]} ->
                                        {stop, {cert_and_pkey, [VClock|{Cert, <<"*****">>}]}};
                                    {cert_and_pkey, {Cert, _PKey}} ->
                                        {stop, {cert_and_pkey, {Cert, <<"*****">>}}};
                                    _ ->
                                        {continue, T}
                                end
                        end, Config).

log_kv({buckets, RawBuckets0}, #state{buckets=OldBuckets} = State) ->
    VClock = ns_config:extract_vclock(RawBuckets0),
    RawBuckets = ns_config:strip_metadata(RawBuckets0),

    NewBuckets = sort_buckets(RawBuckets),
    BucketsDiff = compute_buckets_diff(NewBuckets, OldBuckets),
    NewState = State#state{buckets=NewBuckets},
    log_common(buckets, [VClock | BucketsDiff]),
    NewState;
log_kv({K, V}, State) ->
    log_common(K, V),
    State.

log_common(K, V) ->
    %% These can get pretty big, so pre-format them for the logger.
    {_, VS} = sanitize({K, V}),
    VB = list_to_binary(io_lib:print(VS, 0, 80, 100)),
    ?log_debug("config change:~n~p ->~n~s", [K, VB]).

sort_buckets(Buckets) ->
    Configs = proplists:get_value(configs, Buckets, []),
    SortedConfigs = lists:keysort(1, Configs),
    misc:update_proplist(Buckets, [{configs, SortedConfigs}]).

%% Merge bucket configs using a function. Note that only those buckets that
%% are present in the first list will be present in the resulting list.
merge_bucket_configs(_Fun, [], _) ->
    [];
merge_bucket_configs(Fun, [X | Xs], []) ->
    {_, XValue} = X,
    [Fun(XValue, []) | merge_bucket_configs(Fun, Xs, [])];
merge_bucket_configs(Fun, [X | XRest] = Xs, [Y | YRest] = Ys) ->
    {XName, XValue} = X,
    {YName, YValue} = Y,

    if
        XName < YName ->
            [{XName, Fun(XValue, [])} | merge_bucket_configs(Fun, XRest, Ys)];
        XName > YName ->
            merge_bucket_configs(Fun, Xs, YRest);
        true ->
            [{XName, Fun(XValue, YValue)} | merge_bucket_configs(Fun, XRest, YRest)]
    end.
