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

-export([start_link/2, modify_streams/3, nuke_connections/3, gen_connection_name/3]).

-record(stream_state, {owner :: {pid(), any()},
                       to_add,
                       to_close,
                       errors
                      }).

-record(state, {producer :: port(),
                consumer :: port(),
                producer_uuid :: term(),
                consumer_uuid :: term(),
                producer_buf = <<>> :: binary(),
                consumer_buf = <<>> :: binary(),
                state = idle
               }).

init({ProducerNode, Bucket}) ->
    {{ProducerSock, ProducerUUID},
     {ConsumerSock, ConsumerUUID}} = connect_both(ProducerNode, node(), Bucket),

    proc_lib:init_ack({ok, self()}),

    gen_server:enter_loop(?MODULE, [],
                          #state{
                             producer = ProducerSock,
                             consumer = ConsumerSock,
                             producer_uuid = ProducerUUID,
                             consumer_uuid = ConsumerUUID
                            }).

start_link(ProducerNode, Bucket) ->
    proc_lib:start_link(?MODULE, init, [{ProducerNode, Bucket}]).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_cast(Msg, State) ->
    ?rebalance_warning("Unhandled cast: ~p", [Msg]),
    {noreply, State}.

terminate(_Reason, State) ->
    disconnect(State#state.consumer, State#state.consumer_uuid),
    disconnect(State#state.producer, State#state.producer_uuid).

handle_info({tcp, Socket, Data}, #state{producer = Producer,
                                        consumer = Consumer,
                                        producer_buf = PBuf,
                                        consumer_buf = CBuf} = State) ->
    %% Set up the socket to receive another message
    ok = inet:setopts(Socket, [{active, once}]),
    State1 = case Socket of
                 Producer ->
                     mc_socket:process_data(Data, PBuf,
                                            fun process_producer_packet/2, State);
                 Consumer ->
                     mc_socket:process_data(Data, CBuf,
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

handle_call({modify_streams, StartStreams, StopStreams}, From, #state{state=idle} = State) ->
    StartStreamRequests = lists:map(fun (Partition) ->
                                            {request_add_stream(Partition, State),
                                             Partition}
                                    end, StartStreams),

    StopStreamRequests = lists:map(fun (Partition) ->
                                           {request_close_stream(Partition, State),
                                            Partition}
                                   end, StopStreams),

    {noreply, State#state{state = #stream_state{
                                     owner = From,
                                     to_add = StartStreamRequests,
                                     to_close = StopStreamRequests,
                                     errors = []
                                    }
                         }};
handle_call(Command, _From, State) ->
    ?rebalance_warning("Unexpected handle_call(~p, ~p)", [Command, State]),
    {reply, refused, State}.

handle_packet(response, consumer, ?UPR_CTRL_ADD_STREAM, Packet,
              #state{state = #stream_state{to_add = ToAdd, errors = Errors} = StreamState} = State) ->
    {NewToAdd, NewErrors} = process_add_close_stream_response(Packet, ToAdd, Errors),
    NewStreamState = StreamState#stream_state{to_add = NewToAdd, errors = NewErrors},
    State#state{state = maybe_reply_modify_streams(NewStreamState)};

handle_packet(response, consumer, ?UPR_CTRL_CLOSE_STREAM, Packet,
              #state{state = #stream_state{to_close = ToClose, errors = Errors} = StreamState} = State) ->
    {NewToClose, NewErrors} = process_add_close_stream_response(Packet, ToClose, Errors),
    NewStreamState = StreamState#stream_state{to_close = NewToClose, errors = NewErrors},
    State#state{state = maybe_reply_modify_streams(NewStreamState)};

handle_packet(_, _, _, _, State) ->
    State.

process_add_close_stream_response(Packet, PendingPartitions, Errors) ->
    {Header, Entry} = mc_binary:decode_packet(Packet),
    case lists:keytake(Header#mc_header.opaque, 1, PendingPartitions) of
        {value, {_, Partition} , N} ->
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

maybe_reply_modify_streams(StreamState) ->
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

connect(ConnName, Address, Username, Password, Bucket) ->
    Sock = mc_socket:connect(Address, Username, Password, Bucket),
    {ok, ConnUUID} = mc_client_binary:upr_open(Sock, ConnName),
    {Sock, ConnUUID}.

connect_both(Producer, Consumer, Bucket) ->
    {Username, Password} = ns_bucket:credentials(Bucket),

    ProducerConnName = gen_connection_name("producer", Consumer, Bucket),
    ConsumerConnName = gen_connection_name("consumer", Producer, Bucket),
    {connect(ProducerConnName, ns_memcached:host_port(Producer),
             Username, Password, Bucket),
     connect(ConsumerConnName, ns_memcached:host_port(Consumer),
             Username, Password, Bucket)}.

disconnect(Sock, UUID) ->
    mc_client_binary:upr_close(Sock, UUID),
    gen_tcp:close(Sock).

nuke_connections(Producer, Consumer, Bucket) ->
    {{Sock1, UUID1}, {Sock2, UUID2}} = connect_both(Producer, Consumer, Bucket),
    disconnect(Sock1, UUID1),
    disconnect(Sock2, UUID2).

modify_streams(Pid, StartStreams, StopStreams) ->
    gen_server:call(Pid, {modify_streams, StartStreams, StopStreams}).

request_add_stream(Partition, State) ->
    Opaque = Partition,
    mc_client_binary:upr_add_stream(State#state.consumer, State#state.consumer_uuid,
                                    Partition, Opaque),
    Opaque.

request_close_stream(Partition, State) ->
    Opaque = Partition,
    mc_client_binary:upr_close_stream(State#state.consumer, State#state.consumer_uuid,
                                    Partition, Opaque),
    Opaque.
