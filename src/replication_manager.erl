%% @author Couchbase <info@couchbase.com>
%% @copyright 2013 Couchbase, Inc.
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
%% @doc common stab for tap and upr replication managers
%%
-module(replication_manager).

-behavior(gen_server).

-include("ns_common.hrl").

-record(state, {bucket_name :: bucket_name(),
                repl_type :: tap | upr,
                remaining_tap_partitions = undefined :: [vbucket_id()],
                tap_replicator = undefined,
                upr_replicator = undefined
               }).

-export([start_link/1,
         get_incoming_replication_map/1,
         set_incoming_replication_map/2,
         change_vbucket_replication/3,
         remove_undesired_replications/2]).

-export([replications_difference/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

server_name(Bucket) ->
    list_to_atom("replication_manager-" ++ Bucket).

start_link(Bucket) ->
    gen_server:start_link({local, server_name(Bucket)}, ?MODULE, Bucket, []).

init(Bucket) ->
    {ReplType, TapPartitions} = get_replication_type(Bucket),

    {ok, #state{
            bucket_name = Bucket,
            repl_type = ReplType,
            remaining_tap_partitions = TapPartitions
           }}.

get_incoming_replication_map(Bucket) ->
    gen_server:call(server_name(Bucket), get_incoming_replication_map, infinity).

-spec set_incoming_replication_map(bucket_name(),
                                   [{node(), [vbucket_id(),...]}]) -> ok.
set_incoming_replication_map(Bucket, DesiredReps) ->
    gen_server:call(server_name(Bucket), {set_desired_replications, DesiredReps}, infinity).

-spec remove_undesired_replications(bucket_name(), [{node(), [vbucket_id(),...]}]) -> ok.
remove_undesired_replications(Bucket, DesiredReps) ->
    gen_server:call(server_name(Bucket), {remove_undesired_replications, DesiredReps}, infinity).

-spec change_vbucket_replication(bucket_name(), vbucket_id(), node() | undefined) -> ok.
change_vbucket_replication(Bucket, VBucket, ReplicateFrom) ->
    gen_server:call(server_name(Bucket), {change_vbucket_replication, VBucket, ReplicateFrom}, infinity).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_cast(Msg, State) ->
    {stop, {unexpected_cast, Msg}, State}.

terminate(_Reason, _State) ->
    ok.

handle_info(_Msg, State) ->
    {noreply, State}.

handle_call(get_incoming_replication_map, _From, State) ->
    {reply, get_incoming_replications(State), State};
handle_call({remove_undesired_replications, FutureReps}, From, State) ->
    CurrentReps = get_current_replications(State),
    Diff = replications_difference(FutureReps, CurrentReps),
    CleanedReps0 = [{N, ordsets:intersection(FutureVBs, CurrentVBs)} || {N, FutureVBs, CurrentVBs} <- Diff],
    CleanedReps = [{N, VBs} || {N, [_|_] = VBs} <- CleanedReps0],
    handle_call({set_desired_replications, CleanedReps}, From, State);
handle_call({set_desired_replications, DesiredReps}, _From, #state{} = State) ->
    {TReps, UReps} = split_replications(DesiredReps, State),
    State1 = maybe_start_rep_managers(TReps, UReps, State),

    call_replicators({set_desired_replications, TReps},
                     {set_desired_replications, UReps},
                     fun(A, B) -> ok = A = B end, ok, State),
    {reply, ok, State1};
handle_call({change_vbucket_replication, VBucket, NewSrc}, _From, #state{bucket_name = Bucket} = State) ->
    CurrentReps = get_incoming_replications(Bucket),
    CurrentReps0 = [{Node, ordsets:del_element(VBucket, VBuckets)}
                    || {Node, VBuckets} <- CurrentReps],
    %% TODO: consider making it faster
    DesiredReps = case NewSrc of
                      undefined ->
                          CurrentReps0;
                      _ ->
                          misc:ukeymergewith(fun ({Node, VBucketsA}, {_, VBucketsB}) ->
                                                     {Node, ordsets:union(VBucketsA, VBucketsB)}
                                             end, 1,
                                             CurrentReps0, [{NewSrc, [VBucket]}])
                  end,
    handle_call({set_desired_replications, DesiredReps}, [], State).

split_replications(Reps, #state{repl_type = ReplType, remaining_tap_partitions = TapPartitions}) ->
    case {ReplType, TapPartitions} of
        {tap, _} ->
            {Reps, undefined};
        {upr, undefined} ->
            {undefined, Reps};
        {_, _} ->
            do_split_replications(Reps, TapPartitions)
    end.

do_split_replications(Reps, TapPartitions) ->
    {RT, RU} = lists:foldl(fun ({Node, Partitions}, {Taps, Uprs}) ->
                                   case split_partitions(Partitions, TapPartitions) of
                                       {[], U} ->
                                           {Taps, [{Node, U} | Uprs]};
                                       {T, []} ->
                                           {[{Node, T} | Taps], Uprs};
                                       {T, U} ->
                                           {[{Node, T} | Taps], [{Node, U} | Uprs]}
                                   end
                           end, {[], []}, Reps),
    {lists:reverse(RT), lists:reverse(RU)}.

split_partitions(Partitions, TapPartitions) ->
    {T, U} = lists:foldl(fun (Partition, {Tap, Upr}) ->
                                 case ordsets:is_element(Partition, TapPartitions) of
                                     true ->
                                         {[Partitions | Tap], Upr};
                                     false ->
                                         {Tap, [Partitions | Upr]}
                                 end
                         end, {[], []}, Partitions),
    {lists:reverse(T), lists:reverse(U)}.

maybe_start_rep_managers(TReps, UReps, #state{bucket_name = Bucket,
                                              tap_replicator = Tap,
                                              upr_replicator = Upr} = State) ->
    {ok, NewTap} = case {Tap, TReps} of
                       {undefined, [_|_]} ->
                           tap_replication_manager:start_link(Bucket);
                       _ ->
                           {ok, Tap}
                   end,
    {ok, NewUpr} = case {Upr, UReps} of
                       {undefined, [_|_]} ->
                           upr_replication_manager:start_link(Bucket);
                       _ ->
                           {ok, Upr}
                   end,
    State#state{tap_replicator = NewTap, upr_replicator = NewUpr}.

replications_difference(RepsA, RepsB) ->
    L = [{N, VBs, []} || {N, VBs} <- RepsA],
    R = [{N, [], VBs} || {N, VBs} <- RepsB],
    MergeFn = fun ({N, VBsL, []}, {N, [], VBsR}) ->
                      {N, VBsL, VBsR}
              end,
    misc:ukeymergewith(MergeFn, 1, L, R).

merge_replications(RepsA, RepsB) ->
    MergeFn = fun ({N, PartitionsA}, {N, PartitionsB}) ->
                      {N, PartitionsA ++ PartitionsB}
              end,
    misc:ukeymergewith(MergeFn, 1, RepsA, RepsB).

call_replicators(TapReq, UprReq, MergeCB, Default,
                 #state{tap_replicator = Tap, upr_replicator = Upr} = State) ->
    case {Tap, Upr} of
        {undefined, undefined} ->
            Default;
        {undefined, _} ->
            gen_server:call(Upr, UprReq, infinity);
        {_, undefined} ->
            gen_server:call(Tap, TapReq, infinity);
        {_, _} ->
            MergeCB(
              gen_server:call(Tap, TapReq, infinity),
              gen_server:call(Upr, UprReq, infinity))
    end.

get_current_replications(State) ->
    call_replicators(get_current_replications, get_current_replications,
                     fun merge_replications/2, [], State).

%% what's the difference between get_incoming_replications & get_current_replications?
%% do they ever return different result?
get_incoming_replications(State) ->
    call_replicators(get_incoming_replications, get_current_replications,
                     fun merge_replications/2, [], State).

get_replication_type(Bucket) ->
    case ns_bucket:replication_type(ns_bucket:get_bucket(Bucket)) of
        tap ->
            {tap, ordsets:new()};
        upr ->
            {upr, ordsets:new()};
        {upr, Partitions} ->
            {upr, ordsets:from_list(Partitions)}
    end.
