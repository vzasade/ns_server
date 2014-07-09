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
%% @doc frontend for compaction daemon
%%
-module(compaction_api).

-export([set_global_purge_interval/1,
         get_purge_interval/1,
         force_compact_bucket/1,
         force_compact_db_files/1,
         force_compact_view/2,
         force_purge_compact_bucket/1,
         cancel_forced_bucket_compaction/1,
         cancel_forced_db_compaction/1,
         cancel_forced_view_compaction/2]).

-export([handle_call/1]).

-include("ns_common.hrl").

-define(RPC_TIMEOUT, 10000).

-spec set_global_purge_interval(integer()) -> ok.
set_global_purge_interval(Value) ->
    ns_config:set(global_purge_interval, Value).

-define(DEFAULT_DELETIONS_PURGE_INTERVAL, 3).

-spec get_purge_interval(global | binary()) -> integer().
get_purge_interval(BucketName) ->
    BucketConfig =
        case BucketName of
            global ->
                [];
            _ ->
                case ns_bucket:get_bucket(binary_to_list(BucketName)) of
                    not_present ->
                        [];
                    {ok, X} -> X
                end
        end,
    RawPurgeInterval = proplists:get_value(purge_interval, BucketConfig),
    UseGlobal = RawPurgeInterval =:= undefined
        orelse case proplists:get_value(autocompaction, BucketConfig) of
                   false -> true;
                   undefined -> true;
                   _ -> false
               end,

    %% in case bucket does have purge_interval and does not have own
    %% autocompaction settings we should match UI and return global
    %% settings
    case UseGlobal of
        true ->
            ns_config:search('latest-config-marker', global_purge_interval,
                             ?DEFAULT_DELETIONS_PURGE_INTERVAL);
        false ->
            RawPurgeInterval
    end.

to_bin({Command, Arg}) ->
    {Command, list_to_binary(Arg)};
to_bin({Command, Arg1, Arg2}) ->
    {Command, list_to_binary(Arg1), list_to_binary(Arg2)}.

multi_call(Request) ->
    Nodes = ns_node_disco:nodes_actual_proper(),
    RequestBin = to_bin(Request),

    Results =
        misc:parallel_map(
          fun (Node) ->
                  case rpc:call(Node, ?MODULE, handle_call, [RequestBin], ?RPC_TIMEOUT) of
                      {badrpc, {'EXIT', Reason}} = RV ->
                          case misc:is_undef_exit(?MODULE, handle_call, [RequestBin], Reason) of
                              true ->
                                  %% backwards compat with pre 3.0 nodes
                                  gen_fsm:sync_send_all_state_event({compaction_daemon, Node},
                                                                    RequestBin,
                                                                    ?RPC_TIMEOUT);
                              false ->
                                  RV
                          end;
                      RV ->
                          RV
                  end
          end, Nodes, infinity),

    case lists:filter(
          fun ({_Node, Result}) ->
                  Result =/= ok
          end, lists:zip(Nodes, Results)) of
        [] ->
            ok;
        Failed ->
            log_failed(Request, Failed)
    end,
    ok.

handle_call(Request) ->
    gen_fsm:sync_send_all_state_event(compaction_daemon, Request, infinity).

log_failed({force_compact_bucket, Bucket}, Failed) ->
    ale:error(?USER_LOGGER,
              "Failed to start bucket compaction "
              "for `~s` on some nodes: ~n~p", [Bucket, Failed]);
log_failed({force_purge_compact_bucket, Bucket}, Failed) ->
    ale:error(?USER_LOGGER,
              "Failed to start deletion purge compaction "
              "for `~s` on some nodes: ~n~p", [Bucket, Failed]);
log_failed({force_compact_db_files, Bucket}, Failed) ->
    ale:error(?USER_LOGGER,
              "Failed to start bucket databases compaction "
              "for `~s` on some nodes: ~n~p", [Bucket, Failed]);
log_failed({force_compact_view, Bucket, DDocId}, Failed) ->
    ale:error(?USER_LOGGER,
              "Failed to start index compaction "
              "for `~s/~s` on some nodes: ~n~p",
              [Bucket, DDocId, Failed]);
log_failed({cancel_forced_bucket_compaction, Bucket}, Failed) ->
    ale:error(?USER_LOGGER,
              "Failed to cancel bucket compaction "
              "for `~s` on some nodes: ~n~p", [Bucket, Failed]);
log_failed({cancel_forced_db_compaction, Bucket}, Failed) ->
    ale:error(?USER_LOGGER,
              "Failed to cancel bucket databases compaction "
              "for `~s` on some nodes: ~n~p", [Bucket, Failed]);
log_failed({cancel_forced_view_compaction, Bucket, DDocId}, Failed) ->
    ale:error(?USER_LOGGER,
              "Failed to cancel index compaction "
              "for `~s/~s` on some nodes: ~n~p",
              [Bucket, DDocId, Failed]).

force_compact_bucket(Bucket) ->
    multi_call({force_compact_bucket, Bucket}).

force_purge_compact_bucket(Bucket) ->
    multi_call({force_purge_compact_bucket, Bucket}).

force_compact_db_files(Bucket) ->
    multi_call({force_compact_db_files, Bucket}).

force_compact_view(Bucket, DDocId) ->
    multi_call({force_compact_view, Bucket, DDocId}).

cancel_forced_bucket_compaction(Bucket) ->
    multi_call({cancel_forced_bucket_compaction, Bucket}).

cancel_forced_db_compaction(Bucket) ->
    multi_call({cancel_forced_db_compaction, Bucket}).

cancel_forced_view_compaction(Bucket, DDocId) ->
    multi_call({cancel_forced_view_compaction, Bucket, DDocId}).
