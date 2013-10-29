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
-module(upr_proxy).

-behaviour(gen_server).

-include("ns_common.hrl").
-include("mc_constants.hrl").
-include("mc_entry.hrl").

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-export([start_link/2, server_name/2,
         setup_streams/2, takeover/2, nuke_connections/3, gen_connection_name/3]).

-record(stream_state, {owner :: {pid(), any()},
                       to_add,
                       to_close,
                       errors
                      }).

-record(state, {producer :: port(),
                consumer :: port(),
                producer_buf = <<>> :: binary(),
                consumer_buf = <<>> :: binary(),
                state = idle,
                partitions
               }).

init({ProducerNode, Bucket}) ->
    {ProducerSock, ConsumerSock} = connect_both(ProducerNode, node(), Bucket),

    erlang:register(server_name(ProducerNode, Bucket), self()),
    proc_lib:init_ack({ok, self()}),

    gen_server:enter_loop(?MODULE, [],
                          #state{
                             producer = ProducerSock,
                             consumer = ConsumerSock,
                             partitions = sets:new()
                            }).

start_link(ProducerNode, Bucket) ->
    proc_lib:start_link(?MODULE, init, [{ProducerNode, Bucket}]).

server_name(ProducerNode, Bucket) ->
    list_to_atom(?MODULE_STRING "-" ++ Bucket ++ "-" ++ atom_to_list(ProducerNode)).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_cast(Msg, State) ->
    ?rebalance_warning("Unhandled cast: ~p", [Msg]),
    {noreply, State}.

terminate(_Reason, State) ->
    disconnect(State#state.consumer),
    disconnect(State#state.producer).

handle_info({tcp, Socket, Data}, #state{producer = Producer,
                                        consumer = Consumer} = State) ->
    %% Set up the socket to receive another message
    ok = inet:setopts(Socket, [{active, once}]),
    State1 = case Socket of
                 Producer ->
                     mc_socket:process_data(Data, #state.producer_buf,
                                            fun process_producer_packet/2, State);
                 Consumer ->
                     mc_socket:process_data(Data, #state.consumer_buf,
                                            fun process_consumer_packet/2, State)
             end,
    {noreply, State1};

handle_info({tcp_closed, _Socket}, State) ->
    {stop, normal, State};

handle_info({'EXIT', _Pid, _Reason} = ExitSignal, State) ->
    ?rebalance_error("killing myself due to exit signal: ~p", [ExitSignal]),
    {stop, {got_exit, ExitSignal}, State};

handle_info(Msg, State) ->
    ?rebalance_warning("Unexpected handle_info(~p, ~p)", [Msg, State]),
    {noreply, State}.

handle_call(get_partitions, _From, #state{partitions=CurrentPartitions} = State) ->
    {reply, CurrentPartitions, State};
handle_call({setup_streams, Partitions}, From,
            #state{state=idle, partitions=CurrentPartitions} = State) ->
    PartitionsSet = sets:from_list(Partitions),
    StreamsToStart = sets:subtract(PartitionsSet, CurrentPartitions),
    StreamsToStop = sets:subtract(CurrentPartitions, StreamsToStart),

    StartStreamRequests = lists:map(fun (Partition) ->
                                            {request_add_stream(Partition, State, regular)}
                                    end, sets:to_list(StreamsToStart)),

    StopStreamRequests = lists:map(fun (Partition) ->
                                           {request_close_stream(Partition, State)}
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

handle_call({takeover, Partition}, From, #state{state=idle} = State) ->
    {noreply, State#state{state = #stream_state{
                                     owner = From,
                                     to_add = [{request_add_stream(Partition, State, takeover)}],
                                     to_close = [],
                                     errors = []
                                    }
                         }};

handle_call(Command, _From, State) ->
    ?rebalance_warning("Unexpected handle_call(~p, ~p)", [Command, State]),
    {reply, refused, State}.

handle_packet(response, consumer, ?UPR_ADD_STREAM, Packet,
              #state{state = #stream_state{to_add = ToAdd, errors = Errors} = StreamState} = State) ->
    {NewToAdd, NewErrors} = process_add_close_stream_response(Packet, ToAdd, Errors),
    NewStreamState = StreamState#stream_state{to_add = NewToAdd, errors = NewErrors},
    State#state{state = maybe_reply_setup_streams(NewStreamState)};

handle_packet(response, consumer, ?UPR_CLOSE_STREAM, Packet,
              #state{state = #stream_state{to_close = ToClose, errors = Errors} = StreamState} = State) ->
    {NewToClose, NewErrors} = process_add_close_stream_response(Packet, ToClose, Errors),
    NewStreamState = StreamState#stream_state{to_close = NewToClose, errors = NewErrors},
    State#state{state = maybe_reply_setup_streams(NewStreamState)};

handle_packet(_, _, _, _, State) ->
    State.

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

process_consumer_packet(<<?REQ_MAGIC:8, Opcode:8, _Rest/binary>> = Packet, State) ->
    ok = gen_tcp:send(State#state.producer, Packet),
    handle_packet(request, consumer, Opcode, Packet, State);
process_consumer_packet(<<?RES_MAGIC:8, Opcode:8, _Rest/binary>> = Packet, State) ->
    ok = gen_tcp:send(State#state.producer, Packet),
    handle_packet(response, consumer, Opcode, Packet, State).

process_producer_packet(<<?REQ_MAGIC:8, Opcode:8, _Rest/binary>> = Packet, State) ->
    ok = gen_tcp:send(State#state.consumer, Packet),
    handle_packet(request, producer, Opcode, Packet, State);
process_producer_packet(<<?RES_MAGIC:8, Opcode:8, _Rest/binary>> = Packet, State) ->
    ok = gen_tcp:send(State#state.consumer, Packet),
    handle_packet(response, producer, Opcode, Packet, State).

gen_connection_name(Type, Node, Bucket) ->
    Type ++ ":" ++ atom_to_list(Node) ++ ":" ++ Bucket.

connect(Type, ConnName, Address, Username, Password, Bucket) ->
    Sock = mc_socket:connect(Address, Username, Password, Bucket),
    ok = mc_client_binary:upr_open(Sock, ConnName, Type),
    Sock.

connect_both(Producer, Consumer, Bucket) ->
    {Username, Password} = ns_bucket:credentials(Bucket),

    ProducerConnName = gen_connection_name("producer", Consumer, Bucket),
    ConsumerConnName = gen_connection_name("consumer", Producer, Bucket),
    {connect(producer, ProducerConnName, ns_memcached:host_port(Producer),
             Username, Password, Bucket),
     connect(consumer, ConsumerConnName, ns_memcached:host_port(Consumer),
             Username, Password, Bucket)}.

disconnect(Sock) ->
    gen_tcp:close(Sock).

nuke_connections(Producer, Consumer, Bucket) ->
    {Sock1, Sock2} = connect_both(Producer, Consumer, Bucket),
    disconnect(Sock1),
    disconnect(Sock2).

setup_streams(Pid, Partitions) ->
    gen_server:call(Pid, {setup_streams, Partitions}, infinity).

takeover(Pid, Partition) ->
    gen_server:call(Pid, {takeover, Partition}, infinity).

request_add_stream(Partition, State, Type) ->
    mc_client_binary:upr_add_stream(State#state.consumer, Partition, Type),
    Partition.

request_close_stream(Partition, State) ->
    mc_client_binary:upr_close_stream(State#state.consumer, Partition),
    Partition.
