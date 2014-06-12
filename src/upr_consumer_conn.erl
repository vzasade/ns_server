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
%% @doc consumer side of the UPR proxy
%%
-module(upr_consumer_conn).

-include("ns_common.hrl").
-include("mc_constants.hrl").
-include("mc_entry.hrl").

-export([start_link/2,
         setup_streams/2, takeover/2, maybe_close_stream/2]).

-export([init/2, handle_packet/5, handle_call/4, handle_cast/3]).

-record(stream_state, {owner :: {pid(), reference()},
                       to_add :: [vbucket_id()],
                       to_close :: [vbucket_id()],
                       to_close_on_producer :: [vbucket_id()],
                       errors :: [{non_neg_integer(), vbucket_id()}]
                      }).

-record(takeover_state, {owner :: {pid(), reference()},
                         state :: requested | active,
                         opaque :: integer() | undefined,
                         partition :: vbucket_id(),
                         requested_partition_state = none :: none | int_vb_state(),
                         open_ack = false :: boolean()
                        }).

-record(state, {state :: idle | #stream_state{},
                partitions :: [vbucket_id()],
                takeovers :: [{vbucket_id(), #takeover_state{}}],
                opaques :: [{integer(), vbucket_id()}]
               }).

start_link(ConnName, Bucket) ->
    upr_proxy:start_link(consumer, ConnName, node(), Bucket, ?MODULE, []).

init([], ParentState) ->
    {#state{
        partitions = [],
        takeovers = [],
        opaques = [],
        state = idle
       }, upr_proxy:maybe_connect(ParentState)}.

handle_packet(response, ?UPR_ADD_STREAM, Packet, State, ParentState) ->
    DecodedPacket = {Header, _Body} = mc_binary:decode_packet(Packet),
    case get_takeover(Header#mc_header.opaque, State) of
        undefined ->
            handle_add_stream_resp(DecodedPacket, State#state.state, State, ParentState);
        TakeoverState ->
            handle_add_stream_resp(DecodedPacket, TakeoverState, State, ParentState)
    end;

handle_packet(request, ?UPR_STREAM_REQ, Packet, State, ParentState) ->
    {Header, _Body} = mc_binary:decode_packet(Packet),
    case get_takeover(Header#mc_header.vbucket, State) of
        #takeover_state{state = requested} = TakeoverState ->
            NewState = store_opaque(Header#mc_header.opaque, Header#mc_header.vbucket, State),
            NewTakeoverState = TakeoverState#takeover_state{opaque = Header#mc_header.opaque},
            {proxy, store_takeover(NewTakeoverState, NewState), ParentState};
        undefined ->
            {proxy, State, ParentState}
    end;

handle_packet(response, ?UPR_CLOSE_STREAM, Packet,
              #state{state = #stream_state{to_close = ToClose, errors = Errors} = StreamState}
              = State, ParentState) ->
    {Header, _Body} = mc_binary:decode_packet(Packet),

    {Partition, NewToClose, NewErrors} = process_add_close_stream_response(Header, ToClose, Errors),
    NewStreamState = StreamState#stream_state{to_close = NewToClose, errors = NewErrors},

    NewState =
        case Partition of
            error ->
                State;
            _ ->
                del_partition(Partition, State)
        end,
    {block, maybe_reply_setup_streams(NewState#state{state = NewStreamState}), ParentState};

handle_packet(response, ?UPR_SET_VBUCKET_STATE, Packet, State, ParentState) ->
    {Header, _Body} = mc_binary:decode_packet(Packet),

    case {get_takeover_by_opaque(Header#mc_header.opaque, State),
          Header#mc_header.status} of
        {undefined, _} ->
            {proxy, State, ParentState};
        {#takeover_state{state = requested,
                         partition = Partition,
                         owner = From,
                         requested_partition_state = VbState} = TakeoverState,
         ?SUCCESS} ->
            ?rebalance_debug("Partition ~p changed status to ~p",
                             [Partition, mc_client_binary:vbucket_state_to_atom(VbState)]),
            case VbState of
                ?VB_STATE_ACTIVE ->
                    NewTakeoverState = TakeoverState#takeover_state{state = active},
                    {proxy, maybe_reply_takeover(From, NewTakeoverState, State), ParentState};
                _ ->
                    {proxy, store_takeover(TakeoverState#takeover_state{
                                             requested_partition_state = none
                                            }, State), ParentState}
            end;
        {TakeoverState, ResponseStatus} ->
            ?rebalance_error("Unexpected response during takeover.~nTakeoverState: ~p~nResponse status: ~p~n",
                             [TakeoverState, mc_client_binary:map_status(ResponseStatus)]),
            erlang:error({unexpected_set_vbucket_state_response,
                          Header#mc_header.opaque, ResponseStatus})
    end;

handle_packet(_, _, _, State, ParentState) ->
    {proxy, State, ParentState}.

handle_call(get_partitions, _From, State, ParentState) ->
    {reply, get_partitions(State), State, ParentState};

handle_call({maybe_close_stream, Partition}, From,
            #state{state=idle} = State, ParentState) ->
    CurrentPartitions = get_partitions(State),

    StreamsToSet = lists:delete(Partition, CurrentPartitions),
    handle_call({setup_streams, StreamsToSet}, From, State, ParentState);

handle_call({setup_streams, Partitions}, From,
            #state{state=idle} = State, ParentState) ->
    Sock = upr_proxy:get_socket(ParentState),
    CurrentPartitions = get_partitions(State),

    StreamsToStart = Partitions -- CurrentPartitions,
    StreamsToStop = CurrentPartitions -- Partitions,

    case {StreamsToStart, StreamsToStop} of
        {[], []} ->
            {reply, ok, State, ParentState};
        _ ->
            StartStreamRequests = lists:map(fun (Partition) ->
                                                    upr_commands:add_stream(Sock, Partition,
                                                                            Partition, add),
                                                    {Partition}
                                            end, StreamsToStart),

            StopStreamRequests = lists:map(fun (Partition) ->
                                                   Producer = upr_proxy:get_partner(ParentState),
                                                   upr_commands:close_stream(Sock, Partition, Partition),
                                                   gen_server:cast(Producer, {close_stream, Partition}),
                                                   {Partition}
                                           end, StreamsToStop),

            ?log_debug("Setup UPR streams:~nCurrent ~w~nStreams to open ~w~nStreams to close ~w~n",
                       [CurrentPartitions, StreamsToStart, StreamsToStop]),

            {noreply, State#state{state = #stream_state{
                                             owner = From,
                                             to_add = StartStreamRequests,
                                             to_close = StopStreamRequests,
                                             to_close_on_producer = StopStreamRequests,
                                             errors = []
                                            }
                                 }, ParentState}
    end;

handle_call({takeover, Partition}, From, #state{state=idle} = State, ParentState) ->
    Sock = upr_proxy:get_socket(ParentState),
    case has_partition(Partition, State) of
        true ->
            {reply, {error, takeover_on_open_stream_is_not_allowed}, State, ParentState};
        false ->
            case has_takeover(Partition, State) of
                true ->
                    {reply, {error, takeover_is_in_process}, State, ParentState};
                false ->
                    upr_commands:add_stream(Sock, Partition, Partition, takeover),
                    NewState = store_takeover(#takeover_state{
                                                 owner = From,
                                                 state = requested,
                                                 opaque = Partition,
                                                 partition = Partition
                                                }, State),
                    {noreply, NewState, ParentState}
            end
    end;

handle_call(Msg, _From, State, ParentState) ->
    ?rebalance_warning("Unhandled call: Msg = ~p, State = ~p", [Msg, State]),
    {reply, refused, State, ParentState}.


handle_cast({producer_stream_closed, Packet},
            #state{state = #stream_state{to_close_on_producer = ToClose,
                                         errors = Errors} = StreamState} = State,
            ParentState) ->
    {Header, _Body} = mc_binary:decode_packet(Packet),

    {Partition, NewToClose, NewErrors} = process_add_close_stream_response(Header, ToClose, Errors),
    NewStreamState = StreamState#stream_state{to_close_on_producer = NewToClose,
                                              errors = NewErrors},

    NewState =
        case Partition of
            error ->
                State;
            _ ->
                del_partition(Partition, State)
        end,
    {noreply, maybe_reply_setup_streams(NewState#state{state = NewStreamState}), ParentState};

handle_cast({set_vbucket_state, Packet}, State, ParentState) ->
    {Header, Body} = mc_binary:decode_packet(Packet),

    case get_takeover_by_opaque(Header#mc_header.opaque, State) of
        #takeover_state{state = requested,
                        partition = Partition,
                        requested_partition_state = none} = TakeoverState ->
            <<VbState:8>> = Body#mc_entry.ext,
            ?rebalance_debug("Partition ~p is about to change status to ~p",
                             [Partition, mc_client_binary:vbucket_state_to_atom(VbState)]),
            {noreply, store_takeover(TakeoverState#takeover_state{requested_partition_state = VbState},
                                     State), ParentState};
        undefined ->
            {noreply, State, ParentState}
    end;

handle_cast(Msg, State, ParentState) ->
    ?rebalance_warning("Unhandled cast: Msg = ~p, State = ~p", [Msg, State]),
    {noreply, State, ParentState}.

handle_add_stream_resp({Header, Body},
                       #stream_state{to_add = ToAdd, errors = Errors} = StreamState,
                       State, ParentState) ->
    {Partition, NewToAdd, NewErrors} = process_add_close_stream_response(Header, ToAdd, Errors),
    NewStreamState = StreamState#stream_state{to_add = NewToAdd, errors = NewErrors},

    NewState =
        case Partition of
            error ->
                State;
            _ ->
                {ok, Opaque} = upr_commands:process_response(Header, Body),
                ?rebalance_debug("Stream has been added for partition ~p, stream opaque = ~.16X",
                                 [Partition, Opaque, "0x"]),
                add_partition(Partition, State)
        end,
    {block, maybe_reply_setup_streams(NewState#state{state = NewStreamState}), ParentState};
handle_add_stream_resp({Header, Body},
                       #takeover_state{owner = From, partition = Partition, open_ack = false} =
                           TakeoverState,
                       State, ParentState) ->
    case upr_commands:process_response(Header, Body) of
        {ok, _} ->
            NewTakeoverState = TakeoverState#takeover_state{open_ack = true},
            {block, maybe_reply_takeover(From, NewTakeoverState, State), ParentState};
        Error ->
            gen_server:reply(From, Error),
            {block, del_takeover(Partition, State), ParentState}
    end.

process_add_close_stream_response(Header, PendingPartitions, Errors) ->
    case lists:keytake(Header#mc_header.opaque, 1, PendingPartitions) of
        {value, {Partition} , N} ->
            case Header#mc_header.status of
                ?SUCCESS ->
                    {Partition, N, Errors};
                Status ->
                    {error, N, [{Status, Partition} | Errors]}
            end;
        false ->
            ?rebalance_error("Unexpected response. Unrecognized opaque ~p~nHeader: ~p~nPartitions: ~p~nErrors: ~p",
                             [Header#mc_header.opaque, Header, PendingPartitions, Errors]),
            erlang:error({unrecognized_opaque, Header#mc_header.opaque, PendingPartitions})
    end.

maybe_reply_setup_streams(#state{state = StreamState} = State) ->
    case {StreamState#stream_state.to_add, StreamState#stream_state.to_close,
          StreamState#stream_state.to_close_on_producer} of
        {[], [], []} ->
            Reply =
                case StreamState#stream_state.errors of
                    [] ->
                        ok;
                    Errors ->
                        {errors, Errors}
                end,
            gen_server:reply(StreamState#stream_state.owner, Reply),

            ?log_debug("Setup stream request completed with ~p.", [Reply]),
            State#state{state = idle};
        _ ->
            State
    end.

maybe_reply_takeover(From,
                     #takeover_state{open_ack = true, state = active, partition = Partition},
                     State) ->
    gen_server:reply(From, ok),
    del_takeover(Partition, State);
maybe_reply_takeover(_From, TakeoverState, State) ->
    store_takeover(TakeoverState, State).

setup_streams(Pid, Partitions) ->
    gen_server:call(Pid, {setup_streams, Partitions}, infinity).

maybe_close_stream(Pid, Partition) ->
    gen_server:call(Pid, {maybe_close_stream, Partition}, infinity).

takeover(Pid, Partition) ->
    gen_server:call(Pid, {takeover, Partition}, infinity).

add_partition(Partition, #state{partitions = CurrentPartitions} = State) ->
    State#state{partitions = ordsets:add_element(Partition, CurrentPartitions)}.

del_partition(Partition, #state{partitions = CurrentPartitions} = State) ->
    State#state{partitions = ordsets:del_element(Partition, CurrentPartitions)}.

get_partitions(#state{partitions = CurrentPartitions}) ->
    CurrentPartitions.

has_partition(Partition, #state{partitions = CurrentPartitions}) ->
    ordsets:is_element(Partition, CurrentPartitions).

store_takeover(#takeover_state{partition = Partition} = TakeoverState,
               #state{takeovers = Takeovers} = State) ->
    State#state{takeovers = orddict:store(Partition, TakeoverState, Takeovers)}.

has_takeover(Partition, #state{takeovers = Takeovers}) ->
    orddict:is_key(Partition, Takeovers).

get_takeover(Partition, #state{takeovers = Takeovers}) ->
    case orddict:find(Partition, Takeovers) of
        error ->
            undefined;
        {ok, Takeover} ->
            Takeover
    end.

store_opaque(Opaque, Partition, #state{opaques = Opaques} = State) ->
    State#state{opaques = orddict:store(Opaque, Partition, Opaques)}.

get_takeover_by_opaque(Opaque, #state{opaques = Opaques} = State) ->
    case orddict:find(Opaque, Opaques) of
        error ->
            undefined;
        {ok, Partition} ->
            get_takeover(Partition, State)
    end.

del_takeover(Partition, #state{takeovers = Takeovers, opaques = Opaques} = State) ->
    case get_takeover(Partition, State) of
        undefined ->
            State;
        #takeover_state{opaque = Opaque} ->
            State#state{opaques = orddict:erase(Opaque, Opaques),
                        takeovers = orddict:erase(Partition, Takeovers)}
    end.
