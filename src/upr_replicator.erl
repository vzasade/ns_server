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
%% @doc partitions replicator that uses UPR protocol
%%
-module(upr_replicator).

-behaviour(gen_server).

-include("ns_common.hrl").

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-export([start_link/2, setup_replication/4]).

-record(state, {partitions,
                producer,
                bucket}).

init({ProducerNode, Bucket}) ->
    #state{
       partitions = sets:new(),
       producer = ProducerNode,
       bucket = Bucket
      }.

start_link(ProducerNode, Bucket) ->
    gen_server:start_link({local, server_name(ProducerNode, Bucket)}, ?MODULE,
                          {ProducerNode, Bucket}, []).

server_name(ProducerNode, Bucket) ->
    list_to_atom(?MODULE_STRING "-" ++ Bucket ++ "-" ++ atom_to_list(ProducerNode)).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_cast(Msg, State) ->
    ?rebalance_warning("Unhandled cast: ~p" , [Msg]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

handle_info(Msg, State) ->
    ?rebalance_warning("Unexpected handle_info(~p, ~p)", [Msg, State]),
    {noreply, State}.

handle_call({setup_replication, Partitions}, _From,
            #state{partitions = CurrentPartitions,
                   producer = Producer,
                   bucket = Bucket} = State) ->
    PartitionsSet = sets:from_list(Partitions),
    StreamsToStart = sets:subtract(PartitionsSet, CurrentPartitions),
    StreamsToStop = sets:subtract(CurrentPartitions, StreamsToStart),

    upr_proxy:modify_streams(Producer, Bucket,
                             sets:to_list(StreamsToStart), sets:to_list(StreamsToStop)),
    State#state{partitions = PartitionsSet};

handle_call(Command, _From, State) ->
    ?rebalance_warning("Unexpected handle_call(~p, ~p)", [Command, State]),
    {reply, refused, State}.

setup_replication(ConsumerNode, ProducerNode, Bucket, Partitions) ->
    gen_server:call({server_name(ProducerNode, Bucket), ConsumerNode},
                    {setup_replication, Partitions}).
