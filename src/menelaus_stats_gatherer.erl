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
%% @doc this service is used to wait for sample_archived event on the
%% particular node and then gather stats on this node and maybe on other nodes
%%
-module(menelaus_stats_gatherer).

-behaviour(gen_server).

-export([start_link/0, gather_stats/4, invoke_archiver/3]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("ns_stats.hrl").

-export([doc/0]).

doc() ->
    {gen_server, ?MODULE,
     "this service is used to wait for sample_archived event on the " ++
         "particular node and then gather stats on this node and maybe on other nodes",
     [{pubsub_link, nil, {to, ns_stats_event}}]}.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

gather_stats(_Bucket, [], _ClientTStamp, _Window) ->
    {none, [], []};
gather_stats(Bucket, Nodes, ClientTStamp, Window) ->
    FirstNode = case Nodes of
                    [X] ->
                        X;
                    [FN | _] ->
                        case lists:member(node(), Nodes) of
                            true ->
                                node();
                            _ ->
                                FN
                        end
                end,
    case cluster_compat_mode:is_node_compatible(FirstNode, [2, 3, 0]) of
        true ->
            gen_server:call({?MODULE, FirstNode},
                            {gather_stats, Bucket, Nodes, ClientTStamp, Window}, infinity);
        false ->
            gather_op_stats(FirstNode, Bucket, Nodes, ClientTStamp, Window)
    end.

gather_op_stats(FirstNode, Bucket, Nodes, ClientTStamp, {_, Period, _} = Window) ->
    Self = self(),
    Ref = make_ref(),
    Subscription = ns_pubsub:subscribe_link(
                     ns_stats_event,
                     fun (_, done) -> done;
                         ({sample_archived, Name, _}, _)
                           when Name =:= Bucket ->
                             Self ! Ref,
                             done;
                         (_, X) -> X
                     end, []),
    %% don't wait next sample for anything other than real-time stats
    RefToPass = case Period of
                    minute -> Ref;
                    _ -> []
                end,
    try gather_op_stats_body(FirstNode, Bucket, Nodes, ClientTStamp, RefToPass, Window) of
        Something -> Something
    after
        ns_pubsub:unsubscribe(Subscription),

        misc:flush(Ref)
    end.

gather_op_stats_body(FirstNode, Bucket, Nodes, ClientTStamp,
                     Ref, Window) ->
    case invoke_archiver(Bucket, FirstNode, Window) of
        [] -> {FirstNode, [], []};
        [_] -> {FirstNode, [], []};
        RV ->
            OtherNodes = lists:delete(FirstNode, Nodes),

            %% only if we aggregate more than one node
            %% we throw out last sample 'cause it might be missing on other nodes yet
            %% previous samples should be ok on all live nodes
            Samples = case OtherNodes of
                          [] ->
                              lists:reverse(RV);
                          _ ->
                              tl(lists:reverse(RV))
                      end,
            LastTStamp = (hd(Samples))#stat_entry.timestamp,
            case LastTStamp of
                %% wait if we don't yet have fresh sample
                ClientTStamp when Ref =/= [] ->
                    receive
                        Ref ->
                            gather_op_stats_body(FirstNode, Bucket, Nodes, ClientTStamp, [], Window)
                    after 2000 ->
                            {FirstNode, [], []}
                    end;
                _ ->
                    %% cut samples up-to and including ClientTStamp
                    CutSamples = lists:dropwhile(fun (Sample) ->
                                                         Sample#stat_entry.timestamp =/= ClientTStamp
                                                 end, lists:reverse(Samples)),
                    MainSamples = case CutSamples of
                                      [] -> Samples;
                                      _ -> lists:reverse(CutSamples)
                                  end,

                    Replies = case OtherNodes of
                                  [] ->
                                      [];
                                  _ ->
                                      invoke_archiver(Bucket, OtherNodes, Window)
                              end,

                    {FirstNode, MainSamples, Replies}
            end
    end.

invoke_archiver(Bucket, NodeS, {Step, Period, Count}) ->
    RV = case Step of
             1 ->
                 catch stats_reader:latest(Period, NodeS, Bucket, Count);
             _ ->
                 catch stats_reader:latest(Period, NodeS, Bucket, Step, Count)
         end,
    case is_list(NodeS) of
        true -> [{K, V} || {K, {ok, V}} <- RV];
        _ ->
            case RV of
                {ok, List} -> List;
                _ -> []
            end
    end.


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    {ok, {}}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_call({gather_stats, Bucket, Nodes, ClientTStamp, Window}, _From, State) ->
    {reply, gather_op_stats(node(), Bucket, Nodes, ClientTStamp, Window), State};
handle_call(_, _From, State) ->
    {reply, not_supported, State}.

handle_info(_, State) ->
    {noreply, State}.

handle_cast(_, State) ->
    {noreply, State}.
