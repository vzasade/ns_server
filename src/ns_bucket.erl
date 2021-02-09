%% @author Couchbase <info@couchbase.com>
%% @copyright 2009-2021 Couchbase, Inc.
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
-module(ns_bucket).

-include("ns_common.hrl").
-include("ns_config.hrl").
-include("cut.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([auth_type/1,
         get_servers/1,
         bucket_type/1,
         kv_bucket_type/1,
         kv_backend_type/1,
         num_replicas_changed/1,
         create_bucket/3,
         credentials/1,
         delete_bucket/1,
         display_type/1,
         display_type/2,
         external_bucket_type/1,
         durability_min_level/1,
         failover_warnings/0,
         root/0,
         sub_key/2,
         get_snapshot/0,
         get_snapshot/1,
         sub_key_match/1,
         buckets_change/1,
         names_change/1,
         key_filter/0,
         key_filter/1,
         remove_from_snapshot/2,
         toy_buckets/1,
         bucket_exists/2,
         get_bucket/1,
         get_bucket/2,
         ensure_bucket/1,
         get_bucket_names/0,
         get_bucket_names/1,
         get_bucket_names_of_type/1,
         get_bucket_names_of_type/2,
         get_buckets/0,
         get_buckets/1,
         is_named_bucket_persistent/1,
         is_persistent/1,
         is_ephemeral_bucket/1,
         is_valid_bucket_name/1,
         live_bucket_nodes/1,
         live_bucket_nodes_from_config/1,
         map_to_replicas/1,
         replicated_vbuckets/3,
         moxi_port/1,
         name_conflict/1,
         name_conflict/2,
         names_conflict/2,
         node_locator/1,
         num_replicas/1,
         pitr_enabled/1,
         pitr_granularity/1,
         pitr_max_history_age/1,
         attribute_default/1,
         attribute_min/1,
         attribute_max/1,
         ram_quota/1,
         conflict_resolution_type/1,
         drift_thresholds/1,
         eviction_policy/1,
         storage_mode/1,
         storage_backend/1,
         raw_ram_quota/1,
         sasl_password/1,
         update_maps/3,
         set_bucket_config/2,
         set_fast_forward_map/2,
         set_map/2,
         set_map_opts/2,
         set_servers/2,
         filter_ready_buckets/1,
         update_bucket_props/2,
         update_bucket_props/4,
         node_bucket_names/1,
         node_bucket_names/2,
         node_bucket_names_of_type/2,
         node_bucket_names_of_type/3,
         all_node_vbuckets/1,
         update_vbucket_map_history/2,
         past_vbucket_maps/0,
         past_vbucket_maps/1,
         config_to_map_options/1,
         needs_rebalance/2,
         can_have_views/1,
         get_view_nodes/1,
         get_num_vbuckets/0,
         get_max_buckets/0,
         bucket_uuid/1,
         buckets_with_data_on_this_node/0,
         activate_bucket_data_on_this_node/1,
         deactivate_bucket_data_on_this_node/1,
         config_upgrade_to_65/1,
         config_upgrade_to_66/1,
         upgrade_to_chronicle/1]).


%%%===================================================================
%%% API
%%%===================================================================

%% @doc Return {Username, Password} for a bucket.
-spec credentials(nonempty_string()) ->
                         {nonempty_string(), string()}.
credentials(Bucket) ->
    {ok, BucketConfig} = get_bucket(Bucket),
    {Bucket, proplists:get_value(sasl_password, BucketConfig, "")}.

root() ->
    bucket_names.

sub_key(Bucket, SubKey) ->
    {bucket, Bucket, SubKey}.

sub_key_match({bucket, Bucket, SubKey}) ->
    {true, Bucket, SubKey};
sub_key_match(_) ->
    false.

%% do not detect changes bucket_names because it is always in the same
%% transaction with props key
buckets_change(buckets) ->
    true;
buckets_change(Key) ->
    case sub_key_match(Key) of
        {true, _, props} ->
            true;
        _ ->
            false
    end.

names_change(buckets) ->
    true;
names_change(bucket_names) ->
    true;
names_change(_) ->
    false.

get_snapshot() ->
    chronicle_compat:get_snapshot(key_filter()).

get_snapshot(Bucket) ->
    chronicle_compat:get_snapshot(key_filter(Bucket)).

key_filter() ->
    case chronicle_compat:backend() of
        ns_config ->
            [{ns_config, ns_config_key_filter()} | collections:key_filter()];
        chronicle ->
            [{chronicle, fun (bucket_names) ->
                                 true;
                             (Key) ->
                                 sub_key_match(Key) =/= false
                         end}]
    end.

key_filter(Bucket) ->
    case chronicle_compat:backend() of
        ns_config ->
            [{ns_config, ns_config_key_filter()} |
             collections:key_filter(Bucket)];
        chronicle ->
            [{chronicle, [root(), sub_key(Bucket, props),
                          collections:key(Bucket)]}]
    end.

ns_config_key_filter() ->
    fun (buckets) ->
            {true, fun buckets_to_chronicle/1};
        (_) ->
            false
    end.

upgrade_to_chronicle(Buckets) ->
    BucketConfigs = proplists:get_value(configs, Buckets, []),
    bucket_configs_to_chronicle(BucketConfigs) ++
        collections:default_kvs(BucketConfigs).

buckets_to_chronicle(Buckets) ->
    bucket_configs_to_chronicle(proplists:get_value(configs, Buckets, [])).

bucket_configs_to_chronicle(BucketConfigs) ->
    [{root(), [N || {N, _} <- BucketConfigs]} |
     [{sub_key(B, props), BC} || {B, BC} <- BucketConfigs]].

remove_from_snapshot(BucketName, Snapshot) ->
    functools:chain(
      Snapshot,
      [maps:remove(sub_key(BucketName, props), _),
       maps:remove(collections:key(BucketName), _),
       maps:update_with(root(), fun ({List, Rev}) ->
                                        {List -- [BucketName], Rev}
                                end, _)]).

toy_buckets(List) ->
    maps:from_list(
      [{root(), {[N || {N, _} <- List], no_rev}} |
       lists:flatmap(
         fun ({Bucket, Props}) ->
                 [{ns_bucket:sub_key(Bucket, K), {V, no_rev}} ||
                     {K, V} <- Props]
         end, List)]).

bucket_exists(Bucket, Snapshot) ->
    case get_bucket(Bucket, Snapshot) of
        {ok, _} ->
            true;
        not_present ->
            false
    end.

get_bucket(Bucket) ->
    get_bucket(Bucket, direct).

get_bucket(Bucket, direct) ->
    case chronicle_compat:backend() of
        chronicle ->
            case chronicle_compat:get(sub_key(Bucket, props), #{}) of
                {ok, Props} ->
                    {ok, Props};
                {error, not_found} ->
                    not_present
            end;
        ns_config ->
            get_bucket(Bucket, ns_config:latest())
    end;
get_bucket(Bucket, Snapshot) when is_map(Snapshot) ->
    case maps:find(sub_key(Bucket, props), Snapshot) of
        {ok, {Props, _}} ->
            {ok, Props};
        error ->
            not_present
    end;
get_bucket(Bucket, Config) ->
    BucketConfigs = get_buckets(Config),
    get_bucket_from_configs(Bucket, BucketConfigs).

ensure_bucket(Bucket) ->
    case ns_bucket:get_bucket(Bucket) of
        not_present ->
            exit({bucket_not_present, Bucket});
        {ok, BucketConfig} ->
            BucketConfig
    end.

get_bucket_from_configs(Bucket, Configs) ->
    case lists:keysearch(Bucket, 1, Configs) of
        {value, {_, BucketConfig}} ->
            {ok, BucketConfig};
        false -> not_present
    end.

get_bucket_names() ->
    get_bucket_names(direct).

get_bucket_names(direct) ->
    case chronicle_compat:backend() of
        chronicle ->
            chronicle_compat:get(root(), #{required => true});
        ns_config ->
            get_bucket_names(get_buckets())
    end;
get_bucket_names(Snapshot) when is_map(Snapshot) ->
    {ok, {Names, _}} = maps:find(root(), Snapshot),
    Names;
get_bucket_names(BucketConfigs) ->
    proplists:get_keys(BucketConfigs).

-type bucket_type_mode() :: memcached|membase|persistent|auto_compactable|
                            {membase, couchstore}|
                            {membase, magma}|
                            {membase, ephemeral}| {memcached, undefined}.

-spec get_bucket_names_of_type(bucket_type_mode()) -> list().
get_bucket_names_of_type(Type) ->
    get_bucket_names_of_type(Type, get_buckets()).

-spec get_bucket_names_of_type(bucket_type_mode(), list()) -> list().
get_bucket_names_of_type({Type, Mode}, BucketConfigs) ->
    [Name || {Name, Config} <- BucketConfigs, bucket_type(Config) == Type,
             storage_mode(Config) == Mode];
get_bucket_names_of_type(persistent, BucketConfigs) ->
    [Name || {Name, Config} <- BucketConfigs,
             is_persistent(Config)];
get_bucket_names_of_type(auto_compactable, BucketConfigs) ->
    [Name || {Name, Config} <- BucketConfigs,
             is_auto_compactable(Config)];
get_bucket_names_of_type(Type, BucketConfigs) ->
    [Name || {Name, Config} <- BucketConfigs, bucket_type(Config) == Type].

get_buckets() ->
    get_buckets(direct).

get_buckets(direct) ->
    case chronicle_compat:backend() of
        chronicle ->
            get_buckets(get_snapshot());
        ns_config ->
            get_buckets(ns_config:latest())
    end;
get_buckets(Snapshot) when is_map(Snapshot) ->
    lists:map(fun (N) ->
                      {ok, {Props, _}} = maps:find(sub_key(N, props), Snapshot),
                      {N, Props}
              end, get_bucket_names(Snapshot));
get_buckets(Config) ->
    ns_config:search_prop(Config, buckets, configs, []).

live_bucket_nodes(Bucket) ->
    {ok, BucketConfig} = get_bucket(Bucket),
    live_bucket_nodes_from_config(BucketConfig).

live_bucket_nodes_from_config(BucketConfig) ->
    Servers = get_servers(BucketConfig),
    LiveNodes = [node()|nodes()],
    [Node || Node <- Servers, lists:member(Node, LiveNodes) ].

-spec conflict_resolution_type([{_,_}]) -> atom().
conflict_resolution_type(BucketConfig) ->
    proplists:get_value(conflict_resolution_type, BucketConfig, seqno).

drift_thresholds(BucketConfig) ->
    case conflict_resolution_type(BucketConfig) of
        lww ->
            {proplists:get_value(drift_ahead_threshold_ms, BucketConfig),
             proplists:get_value(drift_behind_threshold_ms, BucketConfig)};
        seqno ->
            undefined;
        custom ->
            undefined
    end.

eviction_policy(BucketConfig) ->
    Default = case storage_mode(BucketConfig) of
                  undefined -> value_only;
                  couchstore -> value_only;
                  magma -> value_only;
                  ephemeral -> no_eviction
              end,
    proplists:get_value(eviction_policy, BucketConfig, Default).

-spec storage_mode([{_,_}]) -> atom().
storage_mode(BucketConfig) ->
    case bucket_type(BucketConfig) of
        memcached ->
            undefined;
        membase ->
            proplists:get_value(storage_mode, BucketConfig, couchstore)
    end.

-spec storage_backend([{_,_}]) -> atom().
storage_backend(BucketConfig) ->
    BucketType = bucket_type(BucketConfig),
    StorageMode = storage_mode(BucketConfig),
    case BucketType of
        membase ->
            case StorageMode of
                ephemeral ->
                    undefined;
                SM ->
                    SM
            end;
        memcached ->
            undefined
    end.

durability_min_level(BucketConfig) ->
    case bucket_type(BucketConfig) of
        memcached ->
            undefined;
        membase ->
            case proplists:get_value(durability_min_level, BucketConfig,
                                     none) of
                none ->
                    none;
                majority ->
                    majority;
                majorityAndPersistActive ->
                    majority_and_persist_on_master;
                persistToMajority ->
                    persist_to_majority
            end
    end.

%% Point-in-time Recovery numerical parameter ranges and default values.

%% The default value of the attribute. Currently PITR-only.
attribute_default(Name) ->
    case Name of
        pitr_granularity -> 600;        % 10 minutes
        pitr_max_history_age -> 86400   % 24 hours
    end.

%% The minimum value of the attribute. Currently PITR-only.
attribute_min(Name) ->
    case Name of
        pitr_granularity -> 1;          % 1 second
        pitr_max_history_age -> 1       % 1 second
    end.

%% The maximum value of the attribute. Currently PITR-only.
attribute_max(Name) ->
    case Name of
        pitr_granularity -> 18000;      % 5 hours
        pitr_max_history_age -> 172800  % 48 hours
    end.

%% Per-bucket-type point-in-time recovery attributes.  Point-in-time
%% recovery is not supported on memcached buckets.
pitr_enabled(BucketConfig) ->
    case bucket_type(BucketConfig) of
        memcached ->
            false;
        membase ->
            proplists:get_bool(pitr_enabled, BucketConfig)
    end.

pitr_granularity(BucketConfig) ->
    case bucket_type(BucketConfig) of
        memcached ->
            undefined;
        membase ->
            proplists:get_value(pitr_granularity, BucketConfig,
                                attribute_default(pitr_granularity))
    end.

pitr_max_history_age(BucketConfig) ->
    case bucket_type(BucketConfig) of
        memcached ->
            undefined;
        membase ->
            proplists:get_value(pitr_max_history_age, BucketConfig,
                                attribute_default(pitr_max_history_age))
    end.

%% returns bucket ram quota multiplied by number of nodes this bucket
%% resides on. I.e. gives amount of ram quota that will be used by
%% across the cluster for this bucket.
-spec ram_quota([{_,_}]) -> integer().
ram_quota(Bucket) ->
    case proplists:get_value(ram_quota, Bucket) of
        X when is_integer(X) ->
            X * length(get_servers(Bucket))
    end.

%% returns bucket ram quota for _single_ node. Each node will subtract
%% this much from it's node quota.
-spec raw_ram_quota([{_,_}]) -> integer().
raw_ram_quota(Bucket) ->
    case proplists:get_value(ram_quota, Bucket) of
        X when is_integer(X) ->
            X
    end.

-define(FS_HARD_NODES_NEEDED, 4).
-define(FS_FAILOVER_NEEDED, 3).
-define(FS_REBALANCE_NEEDED, 2).
-define(FS_SOFT_REBALANCE_NEEDED, 1).
-define(FS_OK, 0).

bucket_failover_safety(BucketConfig, ActiveNodes, LiveNodes) ->
    ReplicaNum = num_replicas(BucketConfig),
    case ReplicaNum of
        %% if replica count for bucket is 0 we cannot failover at all
        0 -> {?FS_OK, ok};
        _ ->
            MinLiveCopies = min_live_copies(LiveNodes, BucketConfig),
            BucketNodes = get_servers(BucketConfig),
            BaseSafety =
                if
                    MinLiveCopies =:= undefined -> % janitor run pending
                        case LiveNodes of
                            [_,_|_] -> ?FS_OK;
                            _ -> ?FS_HARD_NODES_NEEDED
                        end;
                    MinLiveCopies =< 1 ->
                        %% we cannot failover without losing data
                        %% is some of chain nodes are down ?
                        DownBucketNodes = lists:any(fun (N) -> not lists:member(N, LiveNodes) end,
                                                    BucketNodes),
                        if
                            DownBucketNodes ->
                                %% yes. User should bring them back or failover/replace them (and possibly add more)
                                ?FS_FAILOVER_NEEDED;
                            %% Can we replace missing chain nodes with other live nodes ?
                            LiveNodes =/= [] andalso tl(LiveNodes) =/= [] -> % length(LiveNodes) > 1, but more efficent
                                %% we're generally fault tolerant, just not balanced enough
                                ?FS_REBALANCE_NEEDED;
                            true ->
                                %% we have one (or 0) of live nodes, need at least one more to be fault tolerant
                                ?FS_HARD_NODES_NEEDED
                        end;
                    true ->
                        case needs_rebalance(BucketConfig, ActiveNodes) of
                            true ->
                                ?FS_SOFT_REBALANCE_NEEDED;
                            false ->
                                ?FS_OK
                        end
                end,
            ExtraSafety =
                if
                    length(LiveNodes) =< ReplicaNum andalso BaseSafety =/= ?FS_HARD_NODES_NEEDED ->
                        %% if we don't have enough nodes to put all replicas on
                        softNodesNeeded;
                    true ->
                        ok
                end,
            {BaseSafety, ExtraSafety}
    end.

failover_safety_rec(?FS_HARD_NODES_NEEDED, _ExtraSafety, _, _ActiveNodes, _LiveNodes) ->
    {?FS_HARD_NODES_NEEDED, ok};
failover_safety_rec(BaseSafety, ExtraSafety, [], _ActiveNodes, _LiveNodes) ->
    {BaseSafety, ExtraSafety};
failover_safety_rec(BaseSafety, ExtraSafety, [BucketConfig | RestConfigs], ActiveNodes, LiveNodes) ->
    {ThisBaseSafety, ThisExtraSafety} = bucket_failover_safety(BucketConfig, ActiveNodes, LiveNodes),
    NewBaseSafety = case BaseSafety < ThisBaseSafety of
                        true -> ThisBaseSafety;
                        _ -> BaseSafety
                    end,
    NewExtraSafety = if ThisExtraSafety =:= softNodesNeeded
                        orelse ExtraSafety =:= softNodesNeeded ->
                             softNodesNeeded;
                        true ->
                             ok
                     end,
    failover_safety_rec(NewBaseSafety, NewExtraSafety,
                        RestConfigs, ActiveNodes, LiveNodes).

-spec failover_warnings() -> [failoverNeeded | rebalanceNeeded | hardNodesNeeded | softNodesNeeded].
failover_warnings() ->
    Snapshot = chronicle_compat:get_snapshot(
                 [key_filter(), ns_cluster_membership:key_filter()]),

    ActiveNodes = ns_cluster_membership:service_active_nodes(Snapshot, kv),
    LiveNodes = ns_cluster_membership:service_actual_nodes(Snapshot, kv),
    {BaseSafety0, ExtraSafety}
        = failover_safety_rec(?FS_OK, ok,
                              [C || {_, C} <- get_buckets(Snapshot),
                                    membase =:= bucket_type(C)],
                              ActiveNodes,
                              LiveNodes),
    BaseSafety = case BaseSafety0 of
                     ?FS_HARD_NODES_NEEDED -> hardNodesNeeded;
                     ?FS_FAILOVER_NEEDED -> failoverNeeded;
                     ?FS_REBALANCE_NEEDED -> rebalanceNeeded;
                     ?FS_SOFT_REBALANCE_NEEDED -> softRebalanceNeeded;
                     ?FS_OK -> ok
                 end,
    [S || S <- [BaseSafety, ExtraSafety], S =/= ok].

map_to_replicas(Map) ->
    lists:foldr(
      fun ({VBucket, [Master | Replicas]}, Acc) ->
              case Master of
                  undefined ->
                      Acc;
                  _ ->
                      [{Master, R, VBucket} || R <- Replicas, R =/= undefined] ++
                          Acc
              end
      end, [], misc:enumerate(Map, 0)).

%% returns _sorted_ list of vbuckets that are replicated from SrcNode
%% to DstNode according to given Map.
replicated_vbuckets(Map, SrcNode, DstNode) ->
    VBuckets = [V || {S, D, V} <- map_to_replicas(Map),
                     S =:= SrcNode, DstNode =:= D],
    lists:sort(VBuckets).

%% @doc Return the minimum number of live copies for all vbuckets.
-spec min_live_copies([node()], list()) -> non_neg_integer() | undefined.
min_live_copies(LiveNodes, Config) ->
    case proplists:get_value(map, Config) of
        undefined -> undefined;
        Map ->
            lists:foldl(
              fun (Chain, Min) ->
                      NumLiveCopies =
                          lists:foldl(
                            fun (Node, Acc) ->
                                    case lists:member(Node, LiveNodes) of
                                        true -> Acc + 1;
                                        false -> Acc
                                    end
                            end, 0, Chain),
                      erlang:min(Min, NumLiveCopies)
              end, length(hd(Map)), Map)
    end.

node_locator(BucketConfig) ->
    case proplists:get_value(type, BucketConfig) of
        membase ->
            vbucket;
        memcached ->
            ketama
    end.

-spec num_replicas([{_,_}]) -> integer().
num_replicas(Bucket) ->
    case proplists:get_value(num_replicas, Bucket) of
        X when is_integer(X) ->
            X
    end.

%% ns_server type (membase vs memcached)
bucket_type(Bucket) ->
    proplists:get_value(type, Bucket).

%% KV type (persistent vs ephemeral)
kv_bucket_type(BucketConfig) ->
    case is_persistent(BucketConfig) of
        true -> persistent;
        false -> ephemeral
    end.

kv_backend_type(BucketConfig) ->
    StorageMode = storage_mode(BucketConfig),
    case StorageMode of
        couchstore -> couchdb;
        magma -> magma;
        %% KV requires a value but only accepts: couchdb, magma, rocksdb.
        %% So we've always passed couchdb for ephemeral buckets which KV
        %% will parse as an acceptable value but not use it.
        ephemeral -> couchdb
    end.

%% Used for REST API compatibility.  This transforms the internal
%% representation of bucket types to externally known bucket types.
%% Ideally the 'display_type' function should suffice here but there
%% is too much reliance on the atom membase by other modules (ex: xdcr).
external_bucket_type(BucketConfig) ->
    BucketType = bucket_type(BucketConfig),
    case BucketType of
        memcached -> memcached;
        membase ->
            case storage_mode(BucketConfig) of
                couchstore -> membase;
                magma -> membase;
                ephemeral -> ephemeral
            end
    end.

%% Default bucket type is now couchbase and not membase. Ideally, we should
%% change the default bucket type atom to couchbase but the bucket type membase
%% is used/checked at multiple locations. For similar reasons, the ephemeral
%% bucket type also gets stored as 'membase' and to differentiate between the
%% couchbase and ephemeral buckets we store an extra parameter called
%% 'storage_mode'. So to fix the log message to display the correct bucket type
%% we use both type and storage_mode parameters of the bucket config.
display_type(BucketConfig) ->
    BucketType = bucket_type(BucketConfig),
    StorageMode = storage_mode(BucketConfig),
    display_type(BucketType, StorageMode).

display_type(membase = _Type, couchstore = _StorageMode) ->
    couchbase;
display_type(membase = _Type, magma = _StorageMode) ->
    couchbase;
display_type(membase = _Type, ephemeral = _StorageMode) ->
    ephemeral;
display_type(Type, _) ->
    Type.

auth_type(Bucket) ->
    proplists:get_value(auth_type, Bucket).

sasl_password(Bucket) ->
    proplists:get_value(sasl_password, Bucket, "").

moxi_port(Bucket) ->
    proplists:get_value(moxi_port, Bucket).

get_servers(BucketConfig) ->
    proplists:get_value(servers, BucketConfig).

set_bucket_config(Bucket, NewConfig) ->
    update_bucket_config(Bucket, fun (_) -> NewConfig end).

%% Here's code snippet from bucket-engine.  We also disallow '.' &&
%% '..' which cause problems with browsers even when properly
%% escaped. See bug 953
%%
%% static bool has_valid_bucket_name(const char *n) {
%%     bool rv = strlen(n) > 0;
%%     for (; *n; n++) {
%%         rv &= isalpha(*n) || isdigit(*n) || *n == '.' || *n == '%' || *n == '_' || *n == '-';
%%     }
%%     return rv;
%% }
%%
%% Now we also disallow bucket names starting with '.'. It's because couchdb
%% creates (at least now) auxiliary directories which start with dot. We don't
%% want to conflict with them
is_valid_bucket_name([]) -> {error, empty};
is_valid_bucket_name([$. | _]) -> {error, starts_with_dot};
is_valid_bucket_name(BucketName) ->
    case is_valid_bucket_name_inner(BucketName) of
        {error, _} = X ->
            X;
        true ->
            Reserved =
                string:str(string:to_lower(BucketName), "_users.couch.") =:= 1 orelse
                string:str(string:to_lower(BucketName), "_replicator.couch.") =:= 1,
            case Reserved of
                true ->
                    {error, reserved};
                false ->
                    true
            end
    end.

is_valid_bucket_name_inner([Char | Rest]) ->
    case ($A =< Char andalso Char =< $Z)
        orelse ($a =< Char andalso Char =< $z)
        orelse ($0 =< Char andalso Char =< $9)
        orelse Char =:= $. orelse Char =:= $%
        orelse Char =:= $_ orelse Char =:= $- of
        true ->
            case Rest of
                [] -> true;
                _ -> is_valid_bucket_name_inner(Rest)
            end;
        _ -> {error, invalid}
    end.

get_max_buckets() ->
    ns_config:read_key_fast(max_bucket_count, ?MAX_BUCKETS_SUPPORTED).

get_num_vbuckets() ->
    case ns_config:search(couchbase_num_vbuckets_default) of
        false ->
            misc:getenv_int("COUCHBASE_NUM_VBUCKETS", 1024);
        {value, X} ->
            X
    end.

new_bucket_default_params(membase) ->
    [{type, membase},
     {num_vbuckets, get_num_vbuckets()},
     {num_replicas, 1},
     {ram_quota, 0},
     {replication_topology, star},
     {repl_type, dcp},
     {servers, []}];
new_bucket_default_params(memcached) ->
    Nodes = ns_cluster_membership:service_active_nodes(kv),
    [{type, memcached},
     {num_vbuckets, 0},
     {num_replicas, 0},
     {servers, Nodes},
     {map, []},
     {ram_quota, 0}].

cleanup_bucket_props(Props) ->
    case proplists:get_value(moxi_port, Props) of
        undefined ->
            lists:keydelete(moxi_port, 1, Props);
        _ ->
            Props
    end.

generate_sasl_password() ->
    binary_to_list(couch_uuids:random()).

generate_sasl_password(Props) ->
    [{auth_type, sasl} |
     lists:keystore(sasl_password, 1, Props,
                    {sasl_password, generate_sasl_password()})].

create_bucket(BucketType, BucketName, NewConfig) ->
    case is_valid_bucket_name(BucketName) of
        true ->
            MergedConfig0 =
                misc:update_proplist(new_bucket_default_params(BucketType),
                                     NewConfig),
            MergedConfig1 = generate_sasl_password(MergedConfig0),
            BucketUUID = couch_uuids:random(),
            MergedConfig = [{uuid, BucketUUID} | MergedConfig1],
            do_create_bucket(chronicle_compat:backend(), BucketName,
                             MergedConfig),
            %% The janitor will handle creating the map.
            ok;
        {error, _} ->
            {error, {invalid_bucket_name, BucketName}}
    end.

do_create_bucket(ns_config, BucketName, Config) ->
    ns_config:update_sub_key(
      buckets, configs,
      fun (List) ->
              case lists:keyfind(BucketName, 1, List) of
                  false -> ok;
                  Tuple ->
                      exit({already_exists, Tuple})
              end,
              [{BucketName, Config} | List]
      end),
    %% for FORCE_CHRONICLE support only
    case collections:enabled(Config) of
        true ->
            ns_config:set(collections:key(BucketName),
                          collections:default_manifest());
        false ->
            ok
    end;
do_create_bucket(chronicle, BucketName, Config) ->
    {ok, _} =
        chronicle_kv:transaction(
          kv, [root()],
          fun (Snapshot) ->
                  BucketNames = get_bucket_names(Snapshot),
                  case lists:member(BucketName, BucketNames) of
                      true ->
                          {abort, already_exists};
                      false ->
                          {commit, create_bucket_sets(
                                     BucketName, BucketNames, Config)}
                  end
          end).

create_bucket_sets(Name, Buckets, Config) ->
    [{set, root(), lists:usort([Name | Buckets])},
     {set, sub_key(Name, props), Config} |
     [{set, collections:key(Name), collections:default_manifest()} ||
         collections:enabled(Config)]].

-spec delete_bucket(bucket_name()) ->
                           {ok, BucketConfig :: list()} |
                           {exit, {not_found, bucket_name()}, any()}.
delete_bucket(BucketName) ->
    do_delete_bucket(chronicle_compat:backend(), BucketName).

do_delete_bucket(ns_config, BucketName) ->
    Ref = make_ref(),
    Process = self(),
    RV = ns_config:update_sub_key(
           buckets, configs,
           fun (List) ->
                   case lists:keyfind(BucketName, 1, List) of
                       false -> exit({not_found, BucketName});
                       {_, BucketConfig} = Tuple ->
                           Process ! {Ref, BucketConfig},
                           lists:delete(Tuple, List)
                   end
           end),
    case RV of
        ok ->
            receive
                {Ref, BucketConfig} ->
                    %% for FORCE_CHRONICLE support only
                    ns_config:delete(collections:key(BucketName)),

                    {ok, BucketConfig}
            after 0 ->
                    exit(this_cannot_happen)
            end;
        {exit, {not_found, _}, _} ->
            RV
    end;
do_delete_bucket(chronicle, BucketName) ->
    RootKey = root(),
    CollectionsKey = collections:key(BucketName),
    PropsKey = sub_key(BucketName, props),
    RV = chronicle_kv:transaction(
           kv, [RootKey, CollectionsKey, PropsKey],
           fun (Snapshot) ->
                   BucketNames = get_bucket_names(Snapshot),
                   case lists:member(BucketName, BucketNames) of
                       false ->
                           {abort, not_found};
                       true ->
                           {ok, BucketConfig} =
                               get_bucket(BucketName, Snapshot),
                           {commit,
                            [{set, RootKey, BucketNames -- [BucketName]} |
                             [{delete, K} || K <- [CollectionsKey, PropsKey],
                                             maps:is_key(K, Snapshot)]],
                            BucketConfig}
                   end
           end),
    case RV of
        {ok, _, BucketConfig} ->
            {ok, BucketConfig};
        not_found ->
            {exit, {not_found, BucketName}, nothing}
    end.

filter_ready_buckets(BucketInfos) ->
    lists:filter(fun ({_Name, PList}) ->
                         case get_servers(PList) of
                             [_|_] = List ->
                                 lists:member(node(), List);
                             _ -> false
                         end
                 end, BucketInfos).

%% Updates properties of bucket of given name and type.  Check of type
%% protects us from type change races in certain cases.
%%
%% If bucket with given name exists, but with different type, we
%% should return {exit, {not_found, _}, _}
update_bucket_props(Type, StorageMode, BucketName, Props) ->
    case lists:member(BucketName,
                      get_bucket_names_of_type({Type, StorageMode})) of
        true ->
            update_bucket_props(BucketName, Props);
        false ->
            {exit, {not_found, BucketName}, []}
    end.

update_bucket_props(BucketName, Props) ->
    update_bucket_config(
      BucketName,
      fun (OldProps) ->
              NewProps = lists:foldl(
                           fun ({K, _V} = Tuple, Acc) ->
                                   [Tuple | lists:keydelete(K, 1, Acc)]
                           end, OldProps, Props),
              cleanup_bucket_props(NewProps)
      end).

set_property(Bucket, Key, Value, Default, Fun) ->
    ok = update_bucket_config(
           Bucket,
           fun (OldConfig) ->
                   Fun(proplists:get_value(Key, OldConfig, Default)),
                   lists:keystore(Key, 1, OldConfig, {Key, Value})
           end).

set_property(Bucket, Key, Value) ->
    ok = update_bucket_config(Bucket, lists:keystore(Key, 1, _, {Key, Value})).

set_fast_forward_map(Bucket, Map) ->
    set_property(Bucket, fastForwardMap, Map, [],
                 master_activity_events:note_set_ff_map(Bucket, Map, _)).

set_map(Bucket, Map) ->
    case mb_map:is_valid(Map) of
        true ->
            ok;
        different_length_chains ->
            %% Never expect to set map with different_length_chains
            %% pre-6.5.
            true = cluster_compat_mode:is_cluster_65()
    end,
    set_property(Bucket, map, Map, [],
                 master_activity_events:note_set_map(Bucket, Map, _)).

set_map_opts(Bucket, Opts) ->
    set_property(Bucket, map_opts_hash, erlang:phash2(Opts)).

set_servers(Bucket, Servers) ->
    set_property(Bucket, servers, Servers).

% Update the bucket config atomically.
update_bucket_config(BucketName, Fun) ->
    update_bucket_config(chronicle_compat:backend(), BucketName, Fun).

update_bucket_config(ns_config, BucketName, Fun) ->
    ns_config:update_sub_key(
      buckets, configs,
      fun (Buckets) ->
              RV = misc:key_update(BucketName, Buckets, Fun),
              RV =/= false orelse exit({not_found, BucketName}),
              RV
      end);
update_bucket_config(chronicle, BucketName, Fun) ->
    PropsKey = sub_key(BucketName, props),
    RV =
        chronicle_kv:transaction(
          kv, [PropsKey],
          fun (Snapshot) ->
                  case get_bucket(BucketName, Snapshot) of
                      {ok, Config} ->
                          {commit, [{set, PropsKey, Fun(Config)}]};
                      not_present ->
                          {abort, not_found}
                  end
          end),
    case RV of
        {ok, _} ->
            ok;
        Other ->
            Other
    end.

update_maps(Buckets, OnMap, ExtraSets) ->
    update_maps(chronicle_compat:backend(), Buckets, OnMap, ExtraSets).

update_maps(ns_config, Buckets, OnMap, ExtraSets) ->
    update_many(
      fun (AllBuckets) ->
              {lists:filtermap(
                 fun ({Name, BC}) ->
                         case lists:member(Name, Buckets) of
                             true ->
                                 {true, {Name, misc:key_update(
                                                 map, BC, OnMap(Name, _))}};
                             false ->
                                 false
                         end
                 end, AllBuckets), ExtraSets}
      end);
update_maps(chronicle, Buckets, OnMap, ExtraSets) ->
    {ok, _} =
        chronicle_kv:transaction(
          kv, [sub_key(N, props) || N <- Buckets],
          fun (Snapshot) ->
                  Sets =
                      lists:filtermap(
                        fun (Name) ->
                                case get_bucket(Name, Snapshot) of
                                    {ok, BC} ->
                                        {true, {set, sub_key(Name, props),
                                                misc:key_update(
                                                  map, BC, OnMap(Name, _))}};
                                    not_present ->
                                        false
                                end
                        end, Buckets),
                  {commit, Sets ++ [{set, K, V} || {K, V} <- ExtraSets]}
          end),
    ok.

update_many(Fun) ->
    RV =
        ns_config:run_txn(
          fun(Config, SetFn) ->
                  Buckets = get_buckets(Config),
                  {ModifiedBuckets, ExtraSets} = Fun(Buckets),
                  NewBuckets = misc:update_proplist(Buckets, ModifiedBuckets),

                  BucketSet = {buckets, [{configs, NewBuckets}]},
                  {commit,
                   functools:chain(
                     Config,
                     [SetFn(K, V, _) || {K, V} <- [BucketSet | ExtraSets]])}
          end),
    case RV of
        {commit, _} ->
            ok;
        Error ->
            Error
    end.

is_named_bucket_persistent(BucketName) ->
    {ok, BucketConfig} = get_bucket(BucketName),
    is_persistent(BucketConfig).

is_persistent(BucketConfig) ->
    bucket_type(BucketConfig) =:= membase andalso
        (storage_mode(BucketConfig) =:= couchstore orelse
         storage_mode(BucketConfig) =:= magma).

is_auto_compactable(BucketConfig) ->
    is_persistent(BucketConfig) andalso
    storage_mode(BucketConfig) =/= magma.

is_ephemeral_bucket(BucketConfig) ->
    case storage_mode(BucketConfig) of
        ephemeral -> true;
        couchstore -> false;
        magma -> false
    end.

names_conflict(BucketNameA, BucketNameB) ->
    string:to_lower(BucketNameA) =:= string:to_lower(BucketNameB).

%% @doc Check if a bucket name exists in the list. Case insensitive.
name_conflict(BucketName, ListOfBuckets) ->
    BucketNameLower = string:to_lower(BucketName),
    lists:any(fun ({Name, _}) -> BucketNameLower == string:to_lower(Name) end,
              ListOfBuckets).

%% @doc Check if a bucket exists. Case insensitive.
name_conflict(BucketName) ->
    name_conflict(BucketName, get_buckets()).

node_bucket_names(Node, BucketsConfigs) ->
    [B || {B, C} <- BucketsConfigs,
          lists:member(Node, get_servers(C))].

node_bucket_names(Node) ->
    node_bucket_names(Node, get_buckets()).

-spec node_bucket_names_of_type(node(), bucket_type_mode()) -> list().
node_bucket_names_of_type(Node, Type) ->
    node_bucket_names_of_type(Node, Type, get_buckets()).

-spec node_bucket_names_of_type(node(), bucket_type_mode(), list()) -> list().
node_bucket_names_of_type(Node, {Type, Mode}, BucketConfigs) ->
    [B || {B, C} <- BucketConfigs,
          lists:member(Node, get_servers(C)),
          bucket_type(C) =:= Type,
          storage_mode(C) =:= Mode];
node_bucket_names_of_type(Node, persistent, BucketConfigs) ->
    [B || {B, C} <- BucketConfigs,
          lists:member(Node, get_servers(C)),
          is_persistent(C)];
node_bucket_names_of_type(Node, auto_compactable, BucketConfigs) ->
    [B || {B, C} <- BucketConfigs,
          lists:member(Node, get_servers(C)),
          is_auto_compactable(C)];
node_bucket_names_of_type(Node, Type, BucketConfigs) ->
    [B || {B, C} <- BucketConfigs,
          lists:member(Node, get_servers(C)),
          bucket_type(C) =:= Type].

%% All the vbuckets (active or replica) on a node
-spec all_node_vbuckets(term()) -> list(integer()).
all_node_vbuckets(BucketConfig) ->
    VBucketMap = couch_util:get_value(map, BucketConfig, []),
    Node = node(),
    [Ordinal-1 ||
        {Ordinal, VBuckets} <- misc:enumerate(VBucketMap),
        lists:member(Node, VBuckets)].

config_to_map_options(Config) ->
    [{max_slaves, proplists:get_value(max_slaves, Config, 10)},
     {replication_topology, proplists:get_value(replication_topology, Config, star)}].

get_vbmap_history_size() ->
    %% Not set in config through any means, but gives us a tunable parameter.
    ns_config:read_key_fast(vbmap_history_size, ?VBMAP_HISTORY_SIZE).

update_vbucket_map_history(Map, SanifiedOptions) ->
    History = past_vbucket_maps(),
    NewEntry = {Map, SanifiedOptions},
    HistorySize = get_vbmap_history_size(),
    History1 = [NewEntry | lists:delete(NewEntry, History)],
    History2 = case length(History1) > HistorySize of
                   true -> lists:sublist(History1, HistorySize);
                   false -> History1
               end,
    ns_config:set(vbucket_map_history, History2).

past_vbucket_maps() ->
    past_vbucket_maps(ns_config:latest()).

past_vbucket_maps(Config) ->
    case ns_config:search(Config, vbucket_map_history) of
        {value, V} ->
            lists:filter(
              fun ({_Map, Options}) ->
                      %% A a map with no replication_topology is a map
                      %% generated for chain replication. We stopped using
                      %% them long ago, but theoretically it's possible to
                      %% stumble upon one here through a series of
                      %% upgrades. Don't return it here so the code elsewhere
                      %% need not know how to handle them.
                      proplists:is_defined(replication_topology, Options)
              end, V);
        false -> []
    end.

num_replicas_changed(BucketConfig) ->
    num_replicas_changed(num_replicas(BucketConfig),
                         proplists:get_value(map, BucketConfig)).

num_replicas_changed(_NumReplicas, undefined) ->
    false;
num_replicas_changed(NumReplicas, Map) ->
    ExpectedChainLength = NumReplicas + 1,
    lists:any(?cut(ExpectedChainLength =/= length(_)), Map).

needs_rebalance(BucketConfig, Nodes) ->
    Servers = get_servers(BucketConfig),
    case proplists:get_value(type, BucketConfig) of
        membase ->
            case Servers of
                [] ->
                    false;
                _ ->
                    Map = proplists:get_value(map, BucketConfig),
                    Map =:= undefined orelse
                        num_replicas_changed(BucketConfig) orelse
                        lists:sort(Nodes) =/= lists:sort(Servers) orelse
                        ns_rebalancer:map_options_changed(BucketConfig) orelse
                        (ns_rebalancer:unbalanced(Map, BucketConfig) andalso
                         not is_compatible_past_map(Nodes, BucketConfig, Map))
            end;
        memcached ->
            lists:sort(Nodes) =/= lists:sort(Servers)
    end.

is_compatible_past_map(Nodes, BucketConfig, Map) ->
    History = past_vbucket_maps(),
    MapOpts = ns_rebalancer:generate_vbucket_map_options(Nodes, BucketConfig),
    Matching = mb_map:find_matching_past_maps(Nodes, Map,
                                              MapOpts, History, [trivial]),

    lists:member(Map, Matching).

can_have_views(BucketConfig) ->
    storage_mode(BucketConfig) =:= couchstore orelse
    storage_mode(BucketConfig) =:= magma.

get_view_nodes(BucketConfig) ->
    case can_have_views(BucketConfig) of
        true ->
            lists:sort(get_servers(BucketConfig));
        false ->
            []
    end.

bucket_uuid(BucketConfig) ->
    UUID = proplists:get_value(uuid, BucketConfig),
    true = is_binary(UUID),
    UUID.

bucket_uuid(Name, BucketConfigs) ->
    {ok, BucketConfig} = get_bucket_from_configs(Name, BucketConfigs),
    bucket_uuid(BucketConfig).

filter_out_unknown_buckets(BucketsWithUUIDs, BucketConfigs) ->
    lists:filter(fun ({Name, UUID}) ->
                         case get_bucket_from_configs(Name, BucketConfigs) of
                             {ok, BucketConfig} ->
                                 bucket_uuid(BucketConfig) =:= UUID;
                             not_present ->
                                 false
                         end
                 end, BucketsWithUUIDs).

buckets_with_data_key(Node) ->
    {node, Node, buckets_with_data}.

buckets_with_data_key_filter(Node) ->
    {chronicle_compat:backend(), [{node, Node, buckets_with_data}]}.

buckets_with_data_on_this_node() ->
    Node = node(),
    Snapshot = chronicle_compat:get_snapshot(
                 [key_filter(), buckets_with_data_key_filter(Node)]),
    BucketConfigs = get_buckets(Snapshot),
    Stored = membase_buckets_with_data_on_node(Snapshot, Node),
    Filtered = filter_out_unknown_buckets(Stored, BucketConfigs),
    [B || {B, _} <- Filtered] ++
        get_bucket_names_of_type(memcached, BucketConfigs).

membase_buckets_with_data_on_node(Snapshot, Node) ->
    chronicle_compat:get(Snapshot, buckets_with_data_key(Node),
                         #{default => []}).

activate_bucket_data_on_this_node(Name) ->
    activate_bucket_data_on_this_node(chronicle_compat:backend(), Name).

txn_on_all_buckets(Fun, ExtraKeys) ->
    chronicle_kv:txn(
      kv,
      fun (Txn) ->
              Snapshot = chronicle_kv:txn_get_many([ns_bucket:root()], Txn),
              Fun(chronicle_kv:txn_get_many(
                    [ns_bucket:root() |
                     [sub_key(B, props) || B <- get_bucket_names(Snapshot)]] ++
                        ExtraKeys, Txn))
      end).

activate_bucket_data_on_this_node(chronicle, Name) ->
    NodeKey = buckets_with_data_key(node()),
    case txn_on_all_buckets(
           fun (Snapshot) ->
                   case activate_bucket_data_on_this_node_txn(Name, Snapshot) of
                       not_changed ->
                           {abort, not_changed};
                       {ok, Value} ->
                           {commit, [{set, NodeKey, Value}]}
                   end
           end, [NodeKey]) of
        not_changed ->
            ok;
        {ok, _} ->
            ok
    end;
activate_bucket_data_on_this_node(ns_config, Name) ->
    case ns_config:run_txn(
           fun (Config, Set) ->
                   case activate_bucket_data_on_this_node_txn(Name, Config) of
                       not_changed ->
                           {abort, not_changed};
                       {ok, Value} ->
                           {commit, Set(buckets_with_data_key(node()),
                                        Value, Config)}
                   end
           end) of
        {commit, _} ->
            ok;
        {abort, not_changed} ->
            ok
    end.

activate_bucket_data_on_this_node_txn(Name, Snapshot) ->
    BucketConfigs = get_buckets(Snapshot),
    BucketsWithData = membase_buckets_with_data_on_node(Snapshot, node()),
    NewBuckets = lists:keystore(Name, 1, BucketsWithData,
                                {Name, bucket_uuid(Name, BucketConfigs)}),
    case filter_out_unknown_buckets(NewBuckets, BucketConfigs) of
        BucketsWithData ->
            not_changed;
        Other ->
            {ok, Other}
    end.

deactivate_bucket_data_on_this_node(Name) ->
    deactivate_bucket_data_on_this_node(chronicle_compat:backend(), Name).

deactivate_bucket_data_on_this_node(chronicle, Name) ->
    case chronicle_kv:update(kv, buckets_with_data_key(node()),
                             lists:keydelete(Name, 1, _)) of
        {error, not_found} ->
            ok;
        {ok, _} ->
            ok
    end;
deactivate_bucket_data_on_this_node(ns_config, Name) ->
    ns_config:update_key(buckets_with_data_key(node()),
                         lists:keydelete(Name, 1, _), []).

upgrade_buckets(Config, Fun) ->
    Buckets = get_buckets(Config),
    NewBuckets = [{Name, Fun(Name, BucketConfig)} ||
                  {Name, BucketConfig} <-Buckets],
    [{set, buckets, [{configs, NewBuckets}]}].

config_upgrade_to_65(Config) ->
    MaxBuckets = case ns_config:search(Config, max_bucket_count) of
                     false ->
                         ?MAX_BUCKETS_SUPPORTED;
                     {value, V} ->
                         erlang:max(V, ?MAX_BUCKETS_SUPPORTED)
                 end,
    [{set, max_bucket_count, MaxBuckets}].

config_upgrade_to_66(Config) ->
    upgrade_buckets(Config,
          fun (_Name, BCfg) ->
                  case ns_bucket:bucket_type(BCfg) of
                      membase ->
                          lists:keystore(durability_min_level, 1, BCfg,
                                         {durability_min_level, none});
                      memcached ->
                          BCfg
                  end
          end).

-ifdef(TEST).
min_live_copies_test() ->
    ?assertEqual(min_live_copies([node1], []), undefined),
    ?assertEqual(min_live_copies([node1], [{map, undefined}]), undefined),
    Map1 = [[node1, node2], [node2, node1]],
    ?assertEqual(2, min_live_copies([node1, node2], [{map, Map1}])),
    ?assertEqual(1, min_live_copies([node1], [{map, Map1}])),
    ?assertEqual(0, min_live_copies([node3], [{map, Map1}])),
    Map2 = [[undefined, node2], [node2, node1]],
    ?assertEqual(1, min_live_copies([node1, node2], [{map, Map2}])),
    ?assertEqual(0, min_live_copies([node1, node3], [{map, Map2}])).
-endif.
