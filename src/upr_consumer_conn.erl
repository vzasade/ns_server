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
-module(upr_consumer_conn).

-include("ns_common.hrl").
-include("mc_constants.hrl").
-include("mc_entry.hrl").

-export([start_link/2,
         setup_streams/2, takeover/2]).

-export([init/1, handle_packet/4, handle_call/4]).

-record(stream_state, {owner :: {pid(), any()},
                       to_add,
                       to_close,
                       errors
                      }).

-record(state, {state = idle,
                partitions
               }).

start_link(ConnName, Bucket) ->
    upr_proxy:start_link(consumer, ConnName, node(), Bucket, ?MODULE, []).

init([]) ->
    #state{
       partitions = sets:new()
      }.


handle_packet(response, ?UPR_ADD_STREAM, Packet,
              #state{state = #stream_state{to_add = ToAdd, errors = Errors} = StreamState} = State) ->
    {NewToAdd, NewErrors} = process_add_close_stream_response(Packet, ToAdd, Errors),
    NewStreamState = StreamState#stream_state{to_add = NewToAdd, errors = NewErrors},
    State#state{state = maybe_reply_setup_streams(NewStreamState)};

handle_packet(response, ?UPR_CLOSE_STREAM, Packet,
              #state{state = #stream_state{to_close = ToClose, errors = Errors} = StreamState} = State) ->
    {NewToClose, NewErrors} = process_add_close_stream_response(Packet, ToClose, Errors),
    NewStreamState = StreamState#stream_state{to_close = NewToClose, errors = NewErrors},
    State#state{state = maybe_reply_setup_streams(NewStreamState)};

handle_packet(_, _, _, State) ->
    State.

handle_call(get_partitions, _From, _Sock, #state{partitions=CurrentPartitions} = State) ->
    {reply, CurrentPartitions, State};
handle_call({setup_streams, Partitions}, From, Sock,
            #state{state=idle, partitions=CurrentPartitions} = State) ->
    PartitionsSet = sets:from_list(Partitions),
    StreamsToStart = sets:subtract(PartitionsSet, CurrentPartitions),
    StreamsToStop = sets:subtract(CurrentPartitions, StreamsToStart),

    StartStreamRequests = lists:map(fun (Partition) ->
                                            upr_add_stream(Sock, Partition),
                                            {Partition}
                                    end, sets:to_list(StreamsToStart)),

    StopStreamRequests = lists:map(fun (Partition) ->
                                           upr_close_stream(Sock, Partition),
                                           {Partition}
                                   end, sets:to_list(StreamsToStop)),

    {noreply, State#state{state = #stream_state{
                                     owner = From,
                                     to_add = StartStreamRequests,
                                     to_close = StopStreamRequests,
                                     errors = []
                                    },
                          partitions = PartitionsSet
                         }
    };

handle_call({takeover, Partition}, From, Sock, #state{state=idle} = State) ->
    upr_takeover(Sock, Partition),
    {noreply, State#state{state = #stream_state{
                                     owner = From,
                                     to_add = [{}],
                                     to_close = [],
                                     errors = []
                                    }
                         }};

handle_call(Command, _From, _Sock, State) ->
    ?rebalance_warning("Unexpected handle_call(~p, ~p)", [Command, State]),
    {reply, refused, State}.

process_add_close_stream_response(Packet, PendingPartitions, Errors) ->
    {Header, Entry} = mc_binary:decode_packet(Packet),
    case lists:keytake(Header#mc_header.opaque, 1, PendingPartitions) of
        {value, {Partition} , N} ->
            case Header#mc_header.status of
                ?SUCCESS ->
                    {N, Errors};
                Status ->
                    {N, [{Status, Partition} | Errors]}
            end;
        false ->
            ?rebalance_warning("Unexpected response (~p, ~p)", [Header, Entry]),
            {PendingPartitions, Errors}
    end.

maybe_reply_setup_streams(StreamState) ->
    case {StreamState#stream_state.to_add, StreamState#stream_state.to_close} of
        {[], []} ->
            Reply = case StreamState#stream_state.errors of
                        [] ->
                            ok;
                        Errors ->
                            {errors, Errors}
                    end,
            gen_server:reply(StreamState#stream_state.owner, Reply),
            idle;
        _ ->
            StreamState
    end.

setup_streams(Pid, Partitions) ->
    gen_server:call(Pid, {setup_streams, Partitions}, infinity).

takeover(Pid, Partition) ->
    gen_server:call(Pid, {takeover, Partition}, infinity).

%% UPR commands

upr_add_stream(Sock, Partition) ->
    {ok, quiet} = mc_client_binary:cmd_quiet(?UPR_ADD_STREAM, Sock,
                                             {#mc_header{opaque = Partition,
                                                         vbucket = Partition},
                                              #mc_entry{ext = <<0:32>>}}).

upr_takeover(Sock, Partition) ->
    upr_proxy:process_upr_response(
      mc_client_binary:cmd_vocal(?UPR_ADD_STREAM, Sock,
                                 {#mc_header{opaque = Partition,
                                             vbucket = Partition},
                                  #mc_entry{ext = <<1:32>>}})).

upr_close_stream(Sock, Partition) ->
    {ok, quiet} = mc_client_binary:cmd_quiet(?UPR_CLOSE_STREAM, Sock,
                                             {#mc_header{opaque = Partition,
                                                         vbucket = Partition},
                                              #mc_entry{}}).
