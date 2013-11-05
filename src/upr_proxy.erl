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

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-export([start_link/5, connect_proxies/2, nuke_connection/4]).

-record(state, {sock :: port(),
                buf = <<>> :: binary(),
                ext_module,
                ext_state,
                proxy_to
               }).

init({Type, ConnName, Node, Bucket, ExtModule}) ->
    Sock = connect(Type, ConnName, Node, Bucket),

    ExtState = erlang:apply(ExtModule, init, []),

    {ok, #state{
            sock = Sock,
            ext_module = ExtModule,
            ext_state = ExtState
           }}.

start_link(Type, ConnName, Node, Bucket, ExtModule) ->
    gen_server:start_link(?MODULE, {Type, ConnName, Node, Bucket, ExtModule}, []).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_cast({setup_proxy, ProxyTo}, State) ->
    {noreply, State#state{proxy_to = ProxyTo}};
handle_cast(Msg, State) ->
    ?rebalance_warning("Unhandled cast: ~p", [Msg]),
    {noreply, State}.

terminate(_Reason, State) ->
    disconnect(State#state.sock).

handle_info({tcp, Socket, Data}, #state{sock = Socket} = State) ->
    %% Set up the socket to receive another message
    ok = inet:setopts(Socket, [{active, once}]),
    {noreply, mc_socket:process_data(Data, #state.buf,
                                     fun process_packet/2, State)};

handle_info({tcp_closed, _Socket}, State) ->
    {stop, normal, State};

handle_info({'EXIT', _Pid, _Reason} = ExitSignal, State) ->
    ?rebalance_error("killing myself due to exit signal: ~p", [ExitSignal]),
    {stop, {got_exit, ExitSignal}, State};

handle_info(Msg, State) ->
    ?rebalance_warning("Unexpected handle_info(~p, ~p)", [Msg, State]),
    {noreply, State}.

handle_call(get_socket, _From, State = #state{sock = Sock}) ->
    {reply, Sock, State};
handle_call(Command, From, State = #state{sock = Sock, ext_module = ExtModule, ext_state = ExtState}) ->
    {ReplyType, Reply, NewExtState} =
        erlang:apply(ExtModule, handle_call, [Command, From, Sock, ExtState]),
    {ReplyType, Reply, State#state{ext_state = NewExtState}}.

handle_packet(Type, Msg, Packet, State = #state{ext_module = ExtModule, ext_state = ExtState}) ->
    State#state{ext_state = erlang:apply(ExtModule, handle_packet, [Type, Msg, Packet, ExtState])}.

process_packet(<<?REQ_MAGIC:8, Opcode:8, _Rest/binary>> = Packet, State) ->
    ok = gen_tcp:send(State#state.proxy_to, Packet),
    handle_packet(request, Opcode, Packet, State);
process_packet(<<?RES_MAGIC:8, Opcode:8, _Rest/binary>> = Packet, State) ->
    ok = gen_tcp:send(State#state.proxy_to, Packet),
    handle_packet(response, Opcode, Packet, State).

connect(Type, ConnName, Node, Bucket) ->
    {Username, Password} = ns_bucket:credentials(Bucket),

    Sock = mc_socket:connect(ns_memcached:host_port(Node), Username, Password, Bucket),
    ok = mc_client_binary:upr_open(Sock, ConnName, Type),
    Sock.

disconnect(Sock) ->
    gen_tcp:close(Sock).

nuke_connection(Type, ConnName, Node, Bucket) ->
    disconnect(connect(Type, ConnName, Node, Bucket)).

connect_proxies(Pid1, Pid2) ->
    gen_server:cast(Pid1, {setup_proxy, gen_server:call(Pid2, get_socket, infinity)}),
    gen_server:cast(Pid2, {setup_proxy, gen_server:call(Pid1, get_socket, infinity)}).
