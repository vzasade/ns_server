%% @author Couchbase <info@couchbase.com>
%% @copyright 2014 Couchbase, Inc.
%%
%% Licensed under the Apache License, Version 2.0 (the "License"); you may not
%% use this file except in compliance with the License. You may obtain a copy of
%% the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
%% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
%% License for the specific language governing permissions and limitations under
%% the License.
%%
-module(xdcr_opaque_checker).

-behaviour(gen_server).

%% includes ns_common.hrl already
-include("xdc_replicator.hrl").
-include("remote_clusters_info.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

-export([start_link/1, init/1, handle_call/3, handle_info/2, handle_cast/2]).
-export([code_change/3, terminate/2]).

-export([doc/0]).

doc() ->
    {gen_server, ?MODULE,
     "mass checks remote nodes for failover also mass checks local vbuckets highseqno and updates" ++
         " parent replicator seqnos remaining to replicate stats"}.

start_link(Rep) ->
    gen_server:start_link(?MODULE, Rep, []).

init(Rep) ->
    arm_check_opaques_timer(),
    arm_stats_check_timer(),
    {ok, Rep}.

arm_check_opaques_timer() ->
    Time = ns_config:read_key_fast(xdcr_opaque_checker_period, 60000),
    erlang:send_after(Time, self(), check_opaques).

arm_stats_check_timer() ->
    Time = ns_config:read_key_fast(xdcr_stats_checker_period, 60000),
    erlang:send_after(Time, self(), check_stats).

handle_call(_Request, _From, _State) ->
    erlang:error(unexpected_call).

handle_cast(_Msg, _State) ->
    erlang:error(unexpected_cast).

handle_info(check_opaques, Rep) ->
    maybe_check_opaques(Rep),
    {noreply, Rep};
handle_info(check_stats, Rep) ->
    check_stats(Rep),
    {noreply, Rep};
handle_info(_Msg, State) ->
    {noreply, State}.


terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

maybe_check_opaques(#rep{target = TargetRef} = Rep) ->
    case remote_clusters_info:get_remote_bucket_by_ref(TargetRef, false) of
        {ok, RBucket} ->
            case lists:member(<<"xdcrCheckpointing">>, RBucket#remote_bucket.bucket_caps) of
                true ->
                    do_check_opaques(Rep);
                _ ->
                    ?xdcr_debug("Remote bucket is not mass vbopaque check capable")
            end;
        Err ->
            ?xdcr_debug("Got error asking remote bucket info: ~p", [Err])
    end.

url_of_sample(#xdcr_vb_stats_sample{httpdb = H}) ->
    xdc_vbucket_rep_ckpt:httpdb_to_base_url(H).

do_check_opaques(#rep{id = Id,
                      target = TargetRef}) ->
    StatsMS = ets:fun2ms(
                fun (#xdcr_vb_stats_sample{id_and_vb = {I, _}} = S)
                    when I =:= Id ->
                        S
                end),
    Stats0 = ets:select(xdcr_stats, StatsMS),
    Stats1 = [{url_of_sample(Sample), Sample} || Sample <- Stats0],
    Groups0 = misc:sort_and_keygroup(1, Stats1),
    Groups = [{URL, [S || {_URL, S} <- G]} || {URL, G} <- Groups0],
    [do_check_group(TargetRef, List) || {_URL, List} <- Groups],
    arm_check_opaques_timer().

do_check_group(TargetRef, StatsSamples) ->
    [#xdcr_vb_stats_sample{httpdb = H,
                           bucket_uuid = UUID} | _] = StatsSamples,
    ?xdcr_debug("Going to do check for (siblings of) ~s", [xdc_rep_utils:sanitize_url(H#httpdb.url)]),
    ToSignal =
        case xdc_vbucket_rep_ckpt:mass_validate_vbopaque(H, TargetRef, UUID, StatsSamples) of
            BadSamples when is_list(BadSamples) ->
                BadSamples;
            Err ->
                ?xdcr_error("Got error trying to validate against bucket ~s. "
                            "Will signal mismatch to all vbuckets. Vbuckets: ~w. Error:~n~p",
                        [TargetRef,
                         [Vb || #xdcr_vb_stats_sample{id_and_vb = {_, Vb}} <- StatsSamples],
                         Err]),
                StatsSamples
        end,
    case ToSignal of
        [] ->
            ok;
        _ ->
            case ale:is_loglevel_enabled(ns_server, debug) of
                true ->
                    Pairs = [{Vb, Pid} || #xdcr_vb_stats_sample{id_and_vb = {_, Vb}, pid = Pid} <- ToSignal],
                    ?xdcr_debug("Check failed for some vbuckets. Will signal: ~w", [Pairs]);
                _ ->
                    ok
            end
    end,
    [Pid ! opaque_mismatch || #xdcr_vb_stats_sample{pid = Pid} <- ToSignal].

check_stats(#rep{id = Id,
                 source = SourceBucket}) ->
    StatsMS = ets:fun2ms(
                fun (#xdcr_vb_stats_sample{id_and_vb = {I, _}} = S)
                      when I =:= Id ->
                        S
                end),
    Stats = ets:select(xdcr_stats, StatsMS),
    Keys0 = [{iolist_to_binary(io_lib:format("vb_~B:high_seqno", [Vb])),
              Sample} || #xdcr_vb_stats_sample{id_and_vb = {_, Vb}} = Sample <- Stats],
    Keys = lists:sort(Keys0),
    {ok, VBStats0} = ns_memcached:stats(binary_to_list(SourceBucket), <<"vbucket-seqno">>),
    VBStats = lists:sort(VBStats0),
    StatUpdates = find_matching_stats_loop(VBStats, Keys, []),
    [Pid ! {updated_highseqno, Seqno}
     || {#xdcr_vb_stats_sample{pid = Pid}, Seqno} <- StatUpdates],
    arm_stats_check_timer().

find_matching_stats_loop([{VBK, VBV} | RestVBStats] = AllVBStats,
                         [{SK, FirstSample} | RestSamples] = AllSamples,
                         Acc) ->
    if
        VBK =:= SK ->
            Seqno = list_to_integer(binary_to_list(VBV)),
            NewAcc = case FirstSample#xdcr_vb_stats_sample.vbucket_seqno < Seqno of
                         true ->
                             [{FirstSample, Seqno} | Acc];
                         false ->
                             Acc
                     end,
            find_matching_stats_loop(RestVBStats, RestSamples, NewAcc);
        VBK < SK ->
            find_matching_stats_loop(RestVBStats, AllSamples, Acc);
        VBK > SK ->
            find_matching_stats_loop(AllVBStats, RestSamples, Acc)
    end;
find_matching_stats_loop(_, _, Acc) ->
    Acc.
