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

-export([init/1, handle_packet/4, handle_call/4, handle_cast/2]).

-record(stream_state, {owner :: {pid(), any()},
                       to_add,
                       to_close,
                       errors
                      }).

-record(takeover_state, {owner :: {pid(), any()},
                         opaque
                        }).

-record(state, {state = idle,
                partitions
               }).

start_link(ConnName, Bucket) ->
    upr_proxy:start_link(consumer, ConnName, node(), Bucket, ?MODULE, []).

init([]) ->
    #state{
       partitions = ordsets:new()
      }.


handle_packet(response, ?UPR_ADD_STREAM, Packet,
              #state{state = #stream_state{to_add = ToAdd, errors = Errors} = StreamState} = State) ->
    {NewToAdd, NewErrors} = process_add_close_stream_response(Packet, ToAdd, Errors),
    NewStreamState = StreamState#stream_state{to_add = NewToAdd, errors = NewErrors},
    {block, maybe_reply_setup_streams(State#state{state = NewStreamState})};

handle_packet(response, ?UPR_CLOSE_STREAM, Packet,
              #state{state = #stream_state{to_close = ToClose, errors = Errors} = StreamState} = State) ->
    {NewToClose, NewErrors} = process_add_close_stream_response(Packet, ToClose, Errors),
    NewStreamState = StreamState#stream_state{to_close = NewToClose, errors = NewErrors},
    {block, maybe_reply_setup_streams(State#state{state = NewStreamState})};

handle_packet(response, ?UPR_SET_VBUCKET_STATE, Packet,
              #state{state = #takeover_state{owner = From, opaque = Opaque}} = State) ->
    {Header, Entry} = mc_binary:decode_packet(Packet),
    case {Header#mc_header.opaque, Entry#mc_entry.ext} of
        {Opaque, ?VB_STATE_ACTIVE} ->
            gen_server:reply(From, ok),
            {proxy, State#state{state = idle}};
        _ ->
            {proxy, State}
    end;

handle_packet(_, _, _, State) ->
    {proxy, State}.

handle_call(get_partitions, _From, _Sock, #state{partitions=CurrentPartitions} = State) ->
    {reply, ordsets:to_list(CurrentPartitions), State};

handle_call({maybe_close_stream, Partition}, From, Sock,
            #state{state=idle, partitions=CurrentPartitions} = State) ->
    StreamsToSet = ordsets:subtract(CurrentPartitions, ordsets:from_list([Partition])),
    handle_call({setup_streams, StreamsToSet}, From, Sock, State);

handle_call({setup_streams, Partitions}, From, Sock,
            #state{state=idle, partitions=CurrentPartitions} = State) ->
    StreamsToStart = ordsets:to_list(ordsets:subtract(Partitions, CurrentPartitions)),
    StreamsToStop = ordsets:to_list(ordsets:subtract(CurrentPartitions, Partitions)),

    case {StreamsToStart, StreamsToStop} of
        {[], []} ->
            {reply, ok, State};
        _ ->
            StartStreamRequests = lists:map(fun (Partition) ->
                                                    upr_add_stream(Sock, Partition),
                                                    {Partition}
                                            end, StreamsToStart),

            StopStreamRequests = lists:map(fun (Partition) ->
                                                   upr_close_stream(Sock, Partition),
                                                   {Partition}
                                           end, StreamsToStop),

            ?log_info("Setup UPR streams:~nCurrent ~w~nStreams to open ~w~nStreams to close ~w~n",
                       [CurrentPartitions, StreamsToStart, StreamsToStop]),

            {noreply, State#state{state = #stream_state{
                                             owner = From,
                                             to_add = StartStreamRequests,
                                             to_close = StopStreamRequests,
                                             errors = []
                                            },
                                  partitions = Partitions
                                 }}
    end;

handle_call({takeover, Partition}, From, Sock, #state{state=idle, partitions=Partitions} = State) ->
    case ordsets:is_element(Partition, Partitions) of
        true ->
            {reply, {error, takeover_on_open_stream_is_not_allowed}, State};
        false ->
            case upr_takeover(Sock, Partition) of
                {ok, Opaque} ->
                    {noreply, State#state{state = #takeover_state{
                                                     owner = From,
                                                     opaque = Opaque
                                                    }
                                         }};
                Error ->
                    {reply, Error, State}
            end
    end;

handle_call(Command, _From, _Sock, State) ->
    ?rebalance_warning("Unexpected handle_call(~p, ~p)", [Command, State]),
    {reply, refused, State}.

handle_cast(Msg, State) ->
    ?rebalance_warning("Unhandled cast: ~p", [Msg]),
    {noreply, State}.

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
            ?rebalance_warning("Unexpected response. Unrecognised opaque ~p (~p, ~p)",
                               [Header#mc_header.opaque, Header, Entry]),
            {PendingPartitions, Errors}
    end.

maybe_reply_setup_streams(#state{state = StreamState, partitions = Partitions} = State) ->
    case {StreamState#stream_state.to_add, StreamState#stream_state.to_close} of
        {[], []} ->
            {Reply, NewPartitions} =
                case StreamState#stream_state.errors of
                    [] ->
                        {ok, Partitions};
                    Errors ->
                        ToRemove = [P || {_, P} <- Errors],
                        {{errors, Errors}, ordsets:subtract(Partitions, ordsets:from_list(ToRemove))}
                end,
            gen_server:reply(StreamState#stream_state.owner, Reply),

            ?log_info("Setup stream request completed with ~p.", [Reply]),
            State#state{state = idle, partitions = NewPartitions};
        _ ->
            State
    end.

setup_streams(Pid, Partitions) ->
    gen_server:call(Pid, {setup_streams, ordsets:from_list(Partitions)}, infinity).

maybe_close_stream(Pid, Partition) ->
    gen_server:call(Pid, {maybe_close_stream, Partition}, infinity).

takeover(Pid, Partition) ->
    gen_server:call(Pid, {takeover, Partition}, infinity).

%% UPR commands
upr_add_stream(Sock, Partition) ->
    {ok, quiet} = mc_client_binary:cmd_quiet(?UPR_ADD_STREAM, Sock,
                                             {#mc_header{opaque = Partition,
                                                         vbucket = Partition},
                                              #mc_entry{ext = <<0:32>>}}).

upr_takeover(Sock, Partition) ->
    Resp = mc_client_binary:cmd_vocal(?UPR_ADD_STREAM, Sock,
                                      {#mc_header{opaque = Partition,
                                                  vbucket = Partition},
                                       #mc_entry{ext = <<1:32>>}}),

    case upr_proxy:process_upr_response(Resp) of
        ok ->
            {ok, _Header, Entry} = Resp,
            {ok, Entry#mc_entry.ext};
        Error ->
            Error
    end.

upr_close_stream(Sock, Partition) ->
    {ok, quiet} = mc_client_binary:cmd_quiet(?UPR_CLOSE_STREAM, Sock,
                                             {#mc_header{opaque = Partition,
                                                         vbucket = Partition},
                                              #mc_entry{}}).
