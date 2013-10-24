%% @author Couchbase <info@couchbase.com>
%% @copyright 2012 Couchbase, Inc.
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

-module(tap_replication_manager).

-behavior(gen_server).

-export([start_link/1]).

-export([init/1, handle_call/3, handle_info/2, terminate/2, code_change/3]).
-export([handle_cast/2]).


-include("ns_common.hrl").
-include_lib("eunit/include/eunit.hrl").

-record(state, {bucket_name :: bucket_name(),
                not_readys_per_node_ets :: ets:tid(),
                desired_replications :: [{node(), [vbucket_id()]}]}).


start_link(Bucket) ->
    proc_lib:start_link(?MODULE, init, [Bucket]).

init(Bucket) ->
    T = ets:new(a, [set, private]),

    proc_lib:init_ack({ok, self()}),

    gen_server:enter_loop(?MODULE, [],
                          #state{bucket_name = Bucket,
                                 not_readys_per_node_ets = T,
                                 desired_replications = []}).

handle_cast(Msg, State) ->
    {stop, {unexpected_cast, Msg}, State}.

handle_call(get_desired_replications, _From, #state{desired_replications = CurrentReps} = State) ->
    {reply, CurrentReps, State};
handle_call(get_actual_replications, _From, #state{bucket_name = Bucket} = State) ->
    {reply, get_incoming_replication_map_as_list(Bucket), State};
handle_call({set_desired_replications, DesiredReps}, _From, #state{} = State) ->
    ok = do_set_incoming_replication_map(State, DesiredReps),
    {reply, ok, State#state{desired_replications = DesiredReps}}.

handle_info({have_not_ready_vbuckets, Node}, #state{not_readys_per_node_ets = T} = State) ->
    {ok, TRef} = timer2:send_after(30000, {restart_replicator, Node}),
    ets:insert(T, {Node, TRef}),
    {noreply, State};
handle_info({restart_replicator, Node}, State) ->
    {Node, VBuckets} = lists:keyfind(Node, 1, State#state.desired_replications),
    ?log_info("Restarting replicator that had not_ready_vbuckets: ~p", [{Node, VBuckets}]),
    ets:delete(State#state.not_readys_per_node_ets, Node),
    kill_child(State, Node, VBuckets),
    start_child(State, Node, VBuckets),
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

-spec get_incoming_replication_map(Bucket::bucket_name()) ->
                                          not_running |
                                          [{Node::node(), [non_neg_integer()]}].
get_incoming_replication_map(Bucket) ->
    case get_children(Bucket) of
        not_running -> not_running;
        Kids ->
            lists:sort([{_Node, _VBuckets} = childs_node_and_vbuckets(Child) || Child <- Kids])
    end.

get_incoming_replication_map_as_list(Bucket) ->
    case get_incoming_replication_map(Bucket) of
        not_running -> [];
        RV -> RV
    end.

do_set_incoming_replication_map(#state{bucket_name = Bucket} = State, DesiredReps) ->
    CurrentReps = get_incoming_replication_map_as_list(Bucket),
    do_set_incoming_replication_map(State, DesiredReps, CurrentReps).

categorize_replications([] = _Diff, AccToKill, AccToStart, AccToChange) ->
    {AccToKill, AccToStart, AccToChange};
categorize_replications([{N, NewVBs, OldVBs} = T | Rest], AccToKill, AccToStart, AccToChange) ->
    if
        NewVBs =:= [] -> categorize_replications(Rest, [{N, OldVBs} | AccToKill], AccToStart, AccToChange);
        OldVBs =:= [] -> categorize_replications(Rest, AccToKill, [{N, NewVBs} | AccToStart], AccToChange);
        NewVBs =:= OldVBs -> categorize_replications(Rest, AccToKill, AccToStart, AccToChange);
        true -> categorize_replications(Rest, AccToKill, AccToStart, [T | AccToChange])
    end.

do_set_incoming_replication_map(State, DesiredReps, CurrentReps) ->
    Diff = replication_manager:replications_difference(DesiredReps, CurrentReps),
    {NodesToKill, NodesToStart, NodesToChange} = categorize_replications(Diff, [], [], []),
    [kill_child(State, SrcNode, VBuckets)
     || {SrcNode, VBuckets} <- NodesToKill],
    [start_child(State, SrcNode, VBuckets)
     || {SrcNode, VBuckets} <- NodesToStart],
    [change_vbucket_filter(State, SrcNode, OldVBs, NewVBs)
     || {SrcNode, NewVBs, OldVBs} <- NodesToChange],
    ok.

cancel_replicator_reset(T, SrcNode) ->
    case ets:lookup(T, SrcNode) of
        [] ->
            ok;
        [{SrcNode, TRef}] ->
            timer2:cancel(TRef),
            ets:delete(T, SrcNode)
    end.


start_child(#state{bucket_name = Bucket,
                   not_readys_per_node_ets = T},
            SrcNode, VBuckets) ->
    ?log_info("Starting replication from ~p for~n~p", [SrcNode, VBuckets]),
    [] = _MaybeSameSrcNode = [Child || Child <- get_children(Bucket),
                                       {SrcNodeC, _} <- [childs_node_and_vbuckets(Child)],
                                       SrcNodeC =:= SrcNode],
    Sup = ns_vbm_sup:server_name(Bucket),
    Child = ns_vbm_sup:make_replicator(SrcNode, VBuckets),
    ChildSpec = child_to_supervisor_spec(Bucket, Child),
    cancel_replicator_reset(T, SrcNode),
    case supervisor:start_child(Sup, ChildSpec) of
        {ok, _} = R -> R;
        {ok, _, _} = R -> R
    end.

kill_child(#state{bucket_name = Bucket,
                  not_readys_per_node_ets = T},
           SrcNode, VBuckets) ->
    ?log_info("Going to stop replication from ~p", [SrcNode]),
    Sup = ns_vbm_sup:server_name(Bucket),
    Child = ns_vbm_sup:make_replicator(SrcNode, VBuckets),
    cancel_replicator_reset(T, SrcNode),
    %% we're ok if child is already dead. There's not much we can or
    %% should do about that
    _ = supervisor:terminate_child(Sup, Child).

change_vbucket_filter(#state{bucket_name = Bucket,
                             not_readys_per_node_ets = T} = State,
                      SrcNode, OldVBuckets, NewVBuckets) ->
    %% TODO: potential slowness here. Consider ordsets
    ?log_info("Going to change replication from ~p to have~n~p (~p, ~p)",
              [SrcNode, NewVBuckets, NewVBuckets--OldVBuckets, OldVBuckets--NewVBuckets]),
    OldChildId = ns_vbm_sup:make_replicator(SrcNode, OldVBuckets),
    NewChildId = ns_vbm_sup:make_replicator(SrcNode, NewVBuckets),
    Args = build_replicator_args(Bucket, SrcNode, NewVBuckets),

    MFA = {ebucketmigrator_srv, start_vbucket_filter_change, [NewVBuckets]},

    cancel_replicator_reset(T, SrcNode),
    try ns_vbm_sup:perform_vbucket_filter_change(Bucket,
                                                     OldChildId,
                                                     NewChildId,
                                                     Args,
                                                     MFA,
                                                     ns_vbm_sup:server_name(Bucket)) of
        RV -> {ok, RV}
    catch error:upstream_conn_is_down ->
            ?log_debug("Detected upstream_conn_is_down and going to simply start fresh ebucketmigrator"),
            start_child(State, SrcNode, NewVBuckets),
            {ok, ok}
    end.

childs_node_and_vbuckets(Child) ->
    {Node, _} = ns_vbm_sup:replicator_nodes(node(), Child),
    VBs = ns_vbm_sup:replicator_vbuckets(Child),
    {Node, VBs}.

child_to_supervisor_spec(Bucket, Child) ->
    {SrcNode, VBuckets} = childs_node_and_vbuckets(Child),
    Args = build_replicator_args(Bucket, SrcNode, VBuckets),
    ns_vbm_sup:build_child_spec(Child, Args).

build_replicator_args(Bucket, SrcNode, VBuckets) ->
    Args = ebucketmigrator_srv:build_args(node(), Bucket,
                                          SrcNode, node(),
                                          VBuckets, false),
    Self = self(),
    ebucketmigrator_srv:add_args_option(Args, on_not_ready_vbuckets,
                                        fun () -> handle_not_ready_vbuckets_from(Self, SrcNode) end).

handle_not_ready_vbuckets_from(RepManagerPid, SrcNode) ->
    RepManagerPid ! {have_not_ready_vbuckets, SrcNode}.


-spec get_children(bucket_name()) -> list() | not_running.
get_children(Bucket) ->
    try supervisor:which_children(ns_vbm_sup:server_name(Bucket)) of
        RawKids ->
            [Id || {Id, _Child, _Type, _Mods} <- RawKids]
    catch exit:{noproc, _} ->
            not_running
    end.
