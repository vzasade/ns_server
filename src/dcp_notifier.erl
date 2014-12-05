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
%% @doc the service that provides notification to subscribers every time
%%      when new mutations appear on certain partition after certain seqno
%%
-module(dcp_notifier).

-include("ns_common.hrl").
-include("mc_constants.hrl").
-include("mc_entry.hrl").

-record(partition, {partition :: vbucket_id(),
                    last_known_pos = undefined :: {seq_no(), integer()} | undefined,
                    stream_state = closed :: pending | open | closed,
                    subscribers = []}).

-export([start_link/1, subscribe/4]).

-export([init/2, handle_packet/5, handle_call/4, handle_cast/3]).

-export([doc/1]).

doc(Bucket) ->
    {gen_server, ?MODULE, {name, server_name(Bucket)}, {bucket, Bucket}, {since, "3.0"},
     "note: it's part of data path. Allows xdcr-over-xmem to check/wait until vbucket" ++ 
         " has greater seqno then xdcr consumed. Only applies to couchbase buckets"}.

start_link(Bucket) ->
    single_bucket_sup:ignore_if_not_couchbase_bucket(
      Bucket,
      fun (_) ->
              dcp_proxy:start_link(notifier,
                                   get_connection_name(Bucket, node()),
                                   node(), Bucket, ?MODULE, [Bucket])
      end).

subscribe(Bucket, Partition, StartSeqNo, UUID) ->
    gen_server:call(server_name(Bucket), {subscribe, Partition, StartSeqNo, UUID}, infinity).

init([Bucket], ParentState) ->
    erlang:register(server_name(Bucket), self()),
    erlang:put(suppress_logging_for_xdcr, true),
    {ets:new(ok, []), ParentState}.

server_name(Bucket) ->
    list_to_atom(?MODULE_STRING "-" ++ Bucket).

handle_call({subscribe, Partition, StartSeqNo, UUID}, From, State, ParentState) ->
    NewParentState = dcp_proxy:maybe_connect(ParentState),
    PartitionState = get_partition(Partition, State),
    do_subscribe(From, {StartSeqNo, UUID}, PartitionState, State, NewParentState).

handle_cast(Msg, State, ParentState) ->
    ?log_warning("Unhandled cast: ~p", [Msg]),
    {noreply, State, ParentState}.

handle_packet(request, ?NOOP, _Packet, State, ParentState) ->
    {ok, quiet} = mc_client_binary:respond(?NOOP, dcp_proxy:get_socket(ParentState),
                                           {#mc_header{status = ?SUCCESS},
                                            #mc_entry{}}),
    {block, State, ParentState};

handle_packet(request, Opcode, Packet, State, ParentState) ->
    {Header, Body} = mc_binary:decode_packet(Packet),
    PartitionState = get_partition(Header#mc_header.opaque, State),
    {block,
     set_partition(handle_request(Opcode, Header, Body, PartitionState, ParentState), State),
     ParentState};

handle_packet(response, Opcode, Packet, State, ParentState) ->
    {Header, Body} = mc_binary:decode_packet(Packet),
    PartitionState = get_partition(Header#mc_header.opaque, State),
    {block,
     set_partition(handle_response(Opcode, Header, Body, PartitionState, ParentState), State),
     ParentState}.

handle_response(?DCP_STREAM_REQ, Header, Body,
                #partition{stream_state = pending,
                           partition = Partition,
                           last_known_pos = {_, UUID}} = PartitionState, ParentState) ->
    case dcp_commands:process_response(Header, Body) of
        ok ->
            PartitionState#partition{stream_state = open};
        {rollback, RollbackSeqNo} ->
            ?log_debug("Rollback stream for partition ~p to seqno ~p", [Partition, RollbackSeqNo]),
            dcp_commands:stream_request(dcp_proxy:get_socket(ParentState),
                                        Partition, Partition,
                                        RollbackSeqNo, 0, UUID,
                                        RollbackSeqNo, RollbackSeqNo),
            PartitionState;
        Error ->
            close_stream(Error, PartitionState)
    end.

handle_request(?DCP_STREAM_END, _Header, _Body,
               #partition{stream_state = open} = PartitionState, _ParentState) ->
    close_stream(ok, PartitionState).

close_stream(Result, #partition{subscribers = Subscribers} = PartitionState) ->
    [gen_server:reply(From, Result) || From <- Subscribers],
    PartitionState#partition{stream_state = closed,
                             subscribers = []}.

get_connection_name(Bucket, Node) ->
    "xdcr:notifier:" ++ atom_to_list(Node) ++ ":" ++ Bucket.

get_partition(Partition, Ets) ->
    case ets:lookup(Ets, Partition) of
        [] ->
            #partition{partition = Partition};
        [{Partition, PartitionState}] ->
            PartitionState
    end.

set_partition(#partition{partition = Partition} = PartitionState,
              Ets) ->
    ets:insert(Ets, {Partition, PartitionState}),
    Ets.

add_subscriber(From, #partition{subscribers = Subscribers} = PartitionState) ->
    PartitionState#partition{subscribers = [From | Subscribers]}.

%% here we have the following possible situations:
%%
%% 1. stream is closed
%%   1.1. requested pos is equal to the last known pos
%%          return immediately because we know that there is data after this pos
%%   1.2  requested pos is behind the last known pos
%%          we should return immediately because we know that there is data
%%          after this pos but we can detect this situation for sure only if
%%          UUID for these two positions match. in a quite rare case if they
%%          don't match we still going to open stream
%%   1.3  we cannot say that the requested pos is behind the last known pos
%%          it can be ahead or the positions have different UUID in this case
%%          we open a stream and change last known pos to the requested pos we
%%          will notify the subscriber as soon as the stream closes
%%
%% 2. stream is open or pending (opening)
%%   2.1. requested pos is equal to the last known pos
%%          add the subscriber to the list of subscribers
%%          we will notify the subscriber as soon as the stream closes
%%   2.2. requested pos is not equal to the last known pos
%%          there's quite a slim chance that the requested pos is ahead of
%%          the pos the stream was opened on (only in case of race between the
%%          subscription request and close_stream message)
%%          and we are ok with some false positives
%%          so we can assume that the requested pos is behind the last known pos
%%          and return immediately since the data is already available
do_subscribe(_From, {StartSeqNo, UUID},
             #partition{last_known_pos = {LNStartSeqNo, UUID},
                        stream_state = closed} = PartitionState, State, ParentState)
  when StartSeqNo =< LNStartSeqNo ->
    {reply, ok, set_partition(PartitionState, State), ParentState};

do_subscribe(From, {StartSeqNo, UUID} = Pos,
             #partition{partition = Partition,
                        stream_state = closed} = PartitionState,
             State, ParentState) ->
    dcp_commands:stream_request(dcp_proxy:get_socket(ParentState),
                                Partition, Partition,
                                StartSeqNo, 0, UUID, StartSeqNo, StartSeqNo),

    PartitionState1 = PartitionState#partition{last_known_pos = Pos,
                                               stream_state = pending},
    {noreply, set_partition(add_subscriber(From, PartitionState1), State), ParentState};

do_subscribe(From, Pos,
             #partition{last_known_pos = Pos} = PartitionState,
             State, ParentState) ->
    {noreply, set_partition(add_subscriber(From, PartitionState), State), ParentState};

do_subscribe(_From, _Pos,
             PartitionState, State, ParentState) ->
    {reply, ok, set_partition(PartitionState, State), ParentState}.
