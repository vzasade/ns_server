%% @author Northscale <info@northscale.com>
%% @copyright 2010 NorthScale, Inc.
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
% A module for retrieving & configuring per-server storage paths,
% storage quotas, mem quotas, etc.
%
-module(ns_storage_conf).

-include_lib("eunit/include/eunit.hrl").

-include("ns_common.hrl").
-include("ns_config.hrl").

-export([setup_disk_storage_conf/3,
         storage_conf_from_node_status/1,
         query_storage_conf/0,
         this_node_dbdir/0, this_node_ixdir/0, this_node_logdir/0,
         this_node_bucket_dbdir/1,
         delete_unused_buckets_db_files/0,
         delete_old_2i_indexes/0,
         setup_db_and_ix_paths/0,
         this_node_cbas_dirs/0]).

-export([cluster_storage_info/0, nodes_storage_info/1]).

-export([this_node_memory_data/0]).

-export([extract_disk_stats_for_path/2]).

-export([check_quotas/3]).
-export([check_this_node_quotas/2]).
-export([get_memory_quota/1, get_memory_quota/2, get_memory_quota/3]).
-export([set_quotas/2]).
-export([default_quotas/1, default_quotas/2]).

setup_db_and_ix_paths() ->
    setup_db_and_ix_paths(ns_couchdb_api:get_db_and_ix_paths()),
    ignore.

setup_db_and_ix_paths(Dirs) ->
    ?log_debug("Initialize db_and_ix_paths variable with ~p", [Dirs]),
    application:set_env(ns_server, db_and_ix_paths, Dirs).

get_db_and_ix_paths() ->
    case application:get_env(ns_server, db_and_ix_paths) of
        undefined ->
            ns_couchdb_api:get_db_and_ix_paths();
        {ok, Paths} ->
            Paths
    end.

couch_storage_path(Field) ->
    try get_db_and_ix_paths() of
        PList ->
            {Field, RV} = lists:keyfind(Field, 1, PList),
            {ok, RV}
    catch T:E ->
            ?log_debug("Failed to get couch storage config field: ~p due to ~p:~p", [Field, T, E]),
            {error, iolist_to_binary(io_lib:format("couch config access failed: ~p:~p", [T, E]))}
    end.

-spec this_node_dbdir() -> {ok, string()} | {error, binary()}.
this_node_dbdir() ->
    couch_storage_path(db_path).

-spec this_node_bucket_dbdir(bucket_name()) -> {ok, string()}.
this_node_bucket_dbdir(BucketName) ->
    {ok, DBDir} = ns_storage_conf:this_node_dbdir(),
    DBSubDir = filename:join(DBDir, BucketName),
    {ok, DBSubDir}.

-spec this_node_ixdir() -> {ok, string()} | {error, binary()}.
this_node_ixdir() ->
    couch_storage_path(index_path).

-spec this_node_logdir() -> {ok, string()} | {error, any()}.
this_node_logdir() ->
    logdir(ns_config:latest(), node()).

-spec logdir(any(), atom()) -> {ok, string()} | {error, any()}.
logdir(Config, Node) ->
    read_path_from_conf(Config, Node, ns_log, filename).

%% @doc read a path from the configuration, following symlinks
-spec read_path_from_conf(any(), atom(), atom(), atom()) ->
    {ok, string()} | {error, any()}.
read_path_from_conf(Config, Node, Key, SubKey) ->
    {value, PropList} = ns_config:search_node(Node, Config, Key),
    case proplists:get_value(SubKey, PropList) of
        undefined ->
            {error, undefined};
        DBDir ->
            {ok, Base} = file:get_cwd(),
            case misc:realpath(DBDir, Base) of
                {error, Atom, _, _} -> {error, Atom};
                {ok, _} = X -> X
            end
    end.


%% @doc sets db and index path of this node.
%%
%% NOTE: ns_server restart is required to make this fully effective.
-spec setup_disk_storage_conf(DbPath::string(), IxDir::string(), CBASDirs::list()) ->
                                     ok | restart | not_changed | {errors, [Msg :: binary()]}.
setup_disk_storage_conf(DbPath, IxPath, CBASDirs) ->
    NewDbDir = misc:absname(DbPath),
    NewIxDir = misc:absname(IxPath),

    case {prepare_db_ix_dirs(NewDbDir, NewIxDir),
          prepare_cbas_dirs(CBASDirs)} of
        {{errors, Errors1}, {errors, Errors2}} ->
            {errors, Errors1 ++ Errors2};
        {{errors, Errors}, _} ->
            {errors, Errors};
        {_, {errors, Errors}} ->
            {errors, Errors};
        {OK1, OK2} ->
            case {update_db_ix_dirs(OK1, NewDbDir, NewIxDir),
                  update_cbas_dirs(OK2)} of
                {restart, _} ->
                    restart;
                {_, ok} ->
                    ok;
                {not_changed, not_changed} ->
                    not_changed
            end
    end.

prepare_db_ix_dirs(NewDbDir, NewIxDir) ->
    [{db_path, CurrentDbDir},
     {index_path, CurrentIxDir}] = lists:sort(ns_couchdb_api:get_db_and_ix_paths()),

    case NewDbDir =/= CurrentDbDir orelse NewIxDir =/= CurrentIxDir of
        true ->
            case misc:ensure_writable_dirs([NewDbDir, NewIxDir]) of
                ok ->
                    case NewDbDir =:= CurrentDbDir of
                        true ->
                            ok;
                        false ->
                            ?log_info("Removing all unused database files"),
                            case delete_unused_buckets_db_files() of
                                ok ->
                                    ok;
                                Error ->
                                    Msg = iolist_to_binary(
                                            io_lib:format("Could not delete unused database files: ~p",
                                                          [Error])),
                                    {errors, [Msg]}
                            end
                    end;
                error ->
                    {errors, [<<"Could not set the storage path. It must be a directory writable by 'couchbase' user.">>]}
            end;
        false ->
            not_changed
    end.

update_db_ix_dirs(not_changed, _NewDbDir, _NewIxDir) ->
    not_changed;
update_db_ix_dirs(ok, NewDbDir, NewIxDir) ->
    ale:info(?USER_LOGGER,
             "Setting database directory path to ~s and index directory path to ~s",
             [NewDbDir, NewIxDir]),

    ns_couchdb_api:set_db_and_ix_paths(NewDbDir, NewIxDir),
    setup_db_and_ix_paths([{db_path, NewDbDir},
                           {index_path, NewIxDir}]),
    restart.

prepare_cbas_dirs(CBASDirs) ->
    AbsDirs = [misc:absname(Dir) || Dir <- CBASDirs],
    case misc:ensure_writable_dirs(AbsDirs) of
        ok ->
            RealDirs =
                lists:usort(
                  lists:map(fun (Dir) ->
                                    {ok, RealPath} = misc:realpath(Dir, "/"),
                                    RealPath
                            end, AbsDirs)),
            case this_node_cbas_dirs() of
                RealDirs ->
                    not_changed;
                _ ->
                    {ok, RealDirs}
            end;
        error ->
            {errors,
             [<<"Could not set analytics storage. All directories must be writable by 'couchbase' user.">>]}
    end.

update_cbas_dirs(not_changed) ->
    not_changed;
update_cbas_dirs({ok, CBASDirs}) ->
    ns_config:update_key({node, node(), cbas_dirs},
                         fun (_OldVal) ->
                                 CBASDirs
                         end, CBASDirs),
    ok.

this_node_cbas_dirs() ->
    case ns_config:search_node(cbas_dirs) of
        {value, V} ->
            V;
        false ->
            {ok, Default} = this_node_ixdir(),
            [Default]
    end.

% Returns a proplist of lists of proplists.
%
% A quotaMb of none means no quota. Disks can get full, disappear,
% etc, so non-ok state is used to signal issues.
%
% NOTE: in current implementation node disk quota is supported and
% state is always ok
%
% NOTE: current node supports only single storage path and does not
% support dedicated ssd (versus hdd) path
%
% NOTE: 1.7/1.8 nodes will not have storage conf returned in current
% implementation.
%
% [{ssd, []},
%  {hdd, [[{path, /some/nice/disk/path}, {quotaMb, 1234}, {state, ok}],
%         [{path", /another/good/disk/path}, {quotaMb, 5678}, {state, ok}]]}]
%
storage_conf_from_node_status(NodeStatus) ->
    StorageConf = proplists:get_value(node_storage_conf, NodeStatus, []),
    HDDInfo = case proplists:get_value(db_path, StorageConf) of
                  undefined -> [];
                  DBDir ->
                      [{path, DBDir},
                       {index_path, proplists:get_value(index_path, StorageConf, DBDir)},
                       {quotaMb, none},
                       {state, ok}]
              end,
    [{ssd, []},
     {hdd, [HDDInfo]}].

query_storage_conf() ->
    StorageConf = get_db_and_ix_paths(),
    lists:map(
      fun ({Key, Path}) ->
              %% db_path and index_path are guaranteed to be absolute
              {ok, RealPath} = misc:realpath(Path, "/"),
              {Key, RealPath}
      end, StorageConf).

extract_node_storage_info(NodeInfo) ->
    {RAMTotal, RAMUsed, _} = proplists:get_value(memory_data, NodeInfo),
    DiskStats = proplists:get_value(disk_data, NodeInfo),
    StorageConf = proplists:get_value(node_storage_conf, NodeInfo, []),
    DiskPaths = [X || {PropName, X} <- StorageConf,
                      PropName =:= db_path orelse PropName =:= index_path],
    {DiskTotal, DiskUsed} = extract_disk_totals(DiskPaths, DiskStats),
    [{ram, [{total, RAMTotal},
            {used, RAMUsed}
           ]},
     {hdd, [{total, DiskTotal},
            {quotaTotal, DiskTotal},
            {used, DiskUsed},
            {free, DiskTotal - DiskUsed}
           ]}].

-spec extract_disk_totals(list(), list()) -> {integer(), integer()}.
extract_disk_totals(DiskPaths, DiskStats) ->

    F = fun (Path, {UsedMounts, ATotal, AUsed} = Tuple) ->
                case extract_disk_stats_for_path(DiskStats, Path) of
                    none -> Tuple;
                    {ok, {MPoint, KBytesTotal, Cap}} ->
                        case lists:member(MPoint, UsedMounts) of
                            true -> Tuple;
                            false ->
                                Total = KBytesTotal * 1024,
                                Used = (Total * Cap) div 100,
                                {[MPoint | UsedMounts], ATotal + Total,
                                 AUsed + Used}
                        end
                end
        end,
    {_UsedMounts, DiskTotal, DiskUsed} = lists:foldl(F, {[], 0, 0}, DiskPaths),
    {DiskTotal, DiskUsed}.

%% returns cluster_storage_info for subset of nodes
nodes_storage_info(NodeNames) ->
    NodesDict = ns_doctor:get_nodes(),
    NodesInfos = lists:foldl(fun (N, A) ->
                                     case dict:find(N, NodesDict) of
                                         {ok, V} -> [{N, V} | A];
                                         _ -> A
                                     end
                             end, [], NodeNames),
    do_cluster_storage_info(NodesInfos).

%% returns cluster storage info. This is aggregation of various
%% storage related metrics across active nodes of cluster.
%%
%% total - total amount of this resource (ram or hdd) in bytes
%%
%% free - amount of this resource free (ram or hdd) in bytes
%%
%% used - amount of this resource used (for any purpose (us, OS, other
%% processes)) in bytes
%%
%% quotaTotal - amount of quota for this resource across cluster
%% nodes. Note hdd quota is not really supported.
%%
%% quotaUsed - amount of quota already allocated
%%
%% usedByData - amount of this resource used by our data
cluster_storage_info() ->
    nodes_storage_info(ns_cluster_membership:service_active_nodes(kv)).

extract_subprop(NodeInfos, Key, SubKey) ->
    [proplists:get_value(SubKey, proplists:get_value(Key, NodeInfo, [])) ||
     NodeInfo <- NodeInfos].

interesting_stats_total_rec([], _Key, Acc) ->
    Acc;
interesting_stats_total_rec([ThisStats | RestStats], Key, Acc) ->
    case lists:keyfind(Key, 1, ThisStats) of
        false ->
            interesting_stats_total_rec(RestStats, Key, Acc);
        {_, V} ->
            interesting_stats_total_rec(RestStats, Key, Acc + V)
    end.

get_total_buckets_ram_quota(Config) ->
    AllBuckets = ns_bucket:get_buckets(Config),
    lists:foldl(fun ({_, BucketConfig}, RAMQuota) ->
                                       ns_bucket:raw_ram_quota(BucketConfig) + RAMQuota
                               end, 0, AllBuckets).

do_cluster_storage_info([]) -> [];
do_cluster_storage_info(NodeInfos) ->
    Config = ns_config:get(),
    NodesCount = length(NodeInfos),
    RAMQuotaUsedPerNode = get_total_buckets_ram_quota(Config),
    RAMQuotaUsed = RAMQuotaUsedPerNode * NodesCount,

    RAMQuotaTotalPerNode =
        case get_memory_quota(Config, kv) of
            {ok, MemQuotaMB} ->
                MemQuotaMB * ?MIB;
            _ ->
                0
        end,

    StorageInfos = [extract_node_storage_info(NodeInfo)
                    || {_Node, NodeInfo} <- NodeInfos],
    HddTotals = extract_subprop(StorageInfos, hdd, total),
    HddUsed = extract_subprop(StorageInfos, hdd, used),

    AllInterestingStats = [proplists:get_value(interesting_stats, PList, []) || {_N, PList} <- NodeInfos],

    BucketsRAMUsage = interesting_stats_total_rec(AllInterestingStats, mem_used, 0),
    BucketsDiskUsage = interesting_stats_total_rec(AllInterestingStats, couch_docs_actual_disk_size, 0)
        + interesting_stats_total_rec(AllInterestingStats, couch_views_actual_disk_size, 0)
        + interesting_stats_total_rec(AllInterestingStats, couch_spatial_disk_size, 0),

    RAMUsed = erlang:max(lists:sum(extract_subprop(StorageInfos, ram, used)),
                         BucketsRAMUsage),
    HDDUsed = erlang:max(lists:sum(HddUsed),
                         BucketsDiskUsage),

    [{ram, [{total, lists:sum(extract_subprop(StorageInfos, ram, total))},
            {quotaTotal, RAMQuotaTotalPerNode * NodesCount},
            {quotaUsed, RAMQuotaUsed},
            {used, RAMUsed},
            {usedByData, BucketsRAMUsage},
            {quotaUsedPerNode, RAMQuotaUsedPerNode},
            {quotaTotalPerNode, RAMQuotaTotalPerNode}
           ]},
     {hdd, [{total, lists:sum(HddTotals)},
            {quotaTotal, lists:sum(HddTotals)},
            {used, HDDUsed},
            {usedByData, BucketsDiskUsage},
            {free, lists:min(lists:zipwith(fun (A, B) -> A - B end,
                                           HddTotals, HddUsed)) * length(HddUsed)} % Minimum amount free on any node * number of nodes
           ]}].

extract_disk_stats_for_path_rec([], _Path) ->
    none;
extract_disk_stats_for_path_rec([{MountPoint0, _, _} = Info | Rest], Path) ->
    MountPoint = filename:join([MountPoint0]),  % normalize path. See filename:join docs
    MPath = case lists:reverse(MountPoint) of
                %% ends of '/'
                "/" ++ _ -> MountPoint;
                %% doesn't. Append it
                X -> lists:reverse("/" ++ X)
            end,
    case MPath =:= string:substr(Path, 1, length(MPath)) of
        true -> {ok, Info};
        _ -> extract_disk_stats_for_path_rec(Rest, Path)
    end.

%% Path0 must be an absolute path with all symlinks resolved.
extract_disk_stats_for_path(StatsList, Path0) ->
    Path = case filename:join([Path0]) of
               "/" -> "/";
               X -> X ++ "/"
           end,
    %% we sort by decreasing length so that first match is 'deepest'
    LessEqFn = fun (A,B) ->
                       length(element(1, A)) >= length(element(1, B))
               end,
    SortedList = lists:sort(LessEqFn, StatsList),
    extract_disk_stats_for_path_rec(SortedList, Path).

%% scan data directory for bucket names
bucket_names_from_disk() ->
    {ok, DbDir} = this_node_dbdir(),
    Files = filelib:wildcard("*", DbDir),
    lists:foldl(
      fun (MaybeBucket, Acc) ->
              Path = filename:join(DbDir, MaybeBucket),
              case filelib:is_dir(Path) of
                  true ->
                      case ns_bucket:is_valid_bucket_name(MaybeBucket) of
                          true ->
                              [MaybeBucket | Acc];
                          {error, _} ->
                              Acc
                      end;
                  false ->
                      Acc
              end
      end, [], Files).

delete_disk_buckets_databases(Pred) ->
    Buckets = lists:filter(Pred, bucket_names_from_disk()),
    delete_disk_buckets_databases_loop(Pred, Buckets).

delete_disk_buckets_databases_loop(_Pred, []) ->
    ok;
delete_disk_buckets_databases_loop(Pred, [Bucket | Rest]) ->
    case ns_couchdb_api:delete_databases_and_files(Bucket) of
        ok ->
            delete_disk_buckets_databases_loop(Pred, Rest);
        Error ->
            Error
    end.

%% deletes all databases files for buckets not defined for this node
%% note: this is called remotely
%%
%% it's named a bit differently from other functions here; but this function
%% is rpc called by older nodes; so we must keep this name unchanged
delete_unused_buckets_db_files() ->
    Config = ns_config:get(),
    Services = ns_cluster_membership:node_services(Config, node()),
    BCfgs = ns_bucket:get_buckets(Config),
    BucketNames =
        case lists:member(kv, Services) of
            true ->
                ns_bucket:node_bucket_names_of_type(node(), membase, couchstore, BCfgs)
                    ++ ns_bucket:node_bucket_names_of_type(node(), membase, ephemeral, BCfgs);
            false ->
                case ns_cluster_membership:get_cluster_membership(node(), Config) of
                    active ->
                        ns_bucket:get_bucket_names_of_type(membase, couchstore, BCfgs)
                            ++ ns_bucket:get_bucket_names_of_type(membase, ephemeral, BCfgs);
                    _ ->
                        []
                end
        end,
    delete_disk_buckets_databases(
      fun (Bucket) ->
              RV = not(lists:member(Bucket, BucketNames)),
              case RV of
                  true ->
                      ale:info(?USER_LOGGER, "Deleting old data files of bucket ~p", [Bucket]);
                  _ ->
                      ok
              end,
              RV
      end).

%% deletes @2i subdirectory in index directory of this node.
%%
%% NOTE: rpc-called remotely from ns_rebalancer prior to activating
%% new nodes at the start of rebalance.
%%
%% Since 4.0 compat mode.
delete_old_2i_indexes() ->
    {ok, IxDir} = this_node_ixdir(),
    Dir = filename:join(IxDir, "@2i"),
    misc:rm_rf(Dir).

-ifdef(EUNIT).
extract_disk_stats_for_path_test() ->
    DiskSupStats = [{"/",297994252,97},
             {"/lib/init/rw",1921120,1},
             {"/dev",10240,2},
             {"/dev/shm",1921120,0},
             {"/var/separate",1921120,0},
             {"/media/p2",9669472,81}],
    ?assertEqual({ok, {"/media/p2",9669472,81}},
                 extract_disk_stats_for_path(DiskSupStats,
                                             "/media/p2/mbdata")),
    ?assertEqual({ok, {"/", 297994252, 97}},
                 extract_disk_stats_for_path(DiskSupStats, "/")),
    ?assertEqual({ok, {"/", 297994252, 97}},
                 extract_disk_stats_for_path(DiskSupStats, "/lib/init")),
    ?assertEqual({ok, {"/dev", 10240, 2}},
                 extract_disk_stats_for_path(DiskSupStats, "/dev/sh")),
    ?assertEqual({ok, {"/dev", 10240, 2}},
                 extract_disk_stats_for_path(DiskSupStats, "/dev")).


-endif.

this_node_memory_data() ->
    case os:getenv("MEMBASE_RAM_MEGS") of
        false -> memsup:get_memory_data();
        X ->
            RAMBytes = list_to_integer(X) * ?MIB,
            {RAMBytes, 0, 0}
    end.

allowed_memory_usage_max(MemSupData) ->
    {MaxMemoryBytes0, _, _} = MemSupData,
    MinusMegs = ?MIN_FREE_RAM,

    MaxMemoryMBPercent = (MaxMemoryBytes0 * ?MIN_FREE_RAM_PERCENT) div (100 * ?MIB),
    MaxMemoryMB = lists:max([(MaxMemoryBytes0 div ?MIB) - MinusMegs, MaxMemoryMBPercent]),
    MaxMemoryMB.

-type quota_result() :: ok | {error, quota_error()}.
-type quota_error() ::
        {total_quota_too_high, node(), Value :: integer(), MaxAllowed :: pos_integer()} |
        {service_quota_too_low, service(), Value :: integer(), MinRequired :: pos_integer()}.
-type quotas() :: [{service(), integer()}].

-spec check_quotas([NodeInfo], ns_config(), quotas()) -> quota_result() when
      NodeInfo :: {node(), [service()], MemoryData :: term()}.
check_quotas(NodeInfos, Config, UpdatedQuotas) ->
    case check_service_quotas(UpdatedQuotas, Config) of
        ok ->
            AllQuotas = get_all_quotas(Config, UpdatedQuotas),
            check_quotas_loop(NodeInfos, AllQuotas);
        Error ->
            Error
    end.

quota_aware_services(CompatVersion) ->
    [S || S <- ns_cluster_membership:supported_services_for_version(CompatVersion),
          lists:member(S, [kv, index, fts, cbas])].

get_all_quotas(Config, UpdatedQuotas) ->
    CompatVersion = cluster_compat_mode:get_compat_version(Config),
    Services = quota_aware_services(CompatVersion),
    lists:map(
      fun (Service) ->
              Value =
                  case lists:keyfind(Service, 1, UpdatedQuotas) of
                      false ->
                          {ok, V} = get_memory_quota(Config, Service),
                          V;
                      {_, V} ->
                          V
                  end,
              {Service, Value}
      end, Services).

check_quotas_loop([], _) ->
    ok;
check_quotas_loop([{Node, Services, MemoryData} | Rest], AllQuotas) ->
    TotalQuota = lists:sum([Q || {S, Q} <- AllQuotas, lists:member(S, Services)]),
    case check_node_total_quota(Node, TotalQuota, MemoryData) of
        ok ->
            check_quotas_loop(Rest, AllQuotas);
        Error ->
            Error
    end.

check_node_total_quota(Node, TotalQuota, MemoryData) ->
    Max = allowed_memory_usage_max(MemoryData),
    case TotalQuota =< Max of
        true ->
            ok;
        false ->
            {error, {total_quota_too_high, Node, TotalQuota, Max}}
    end.

check_service_quotas([], _) ->
    ok;
check_service_quotas([{Service, Quota} | Rest], Config) ->
    case check_service_quota(Service, Quota, Config) of
        ok ->
            check_service_quotas(Rest, Config);
        Error ->
            Error
    end.

-define(MIN_BUCKET_QUOTA, 256).
-define(MAX_DEFAULT_FTS_QUOTA, 512).

min_quota(index) ->
    256;
min_quota(fts) ->
    256;
min_quota(cbas) ->
    1024.

check_service_quota(kv, Quota, Config) ->
    MinMemoryMB0 = ?MIN_BUCKET_QUOTA,

    BucketsQuota = get_total_buckets_ram_quota(Config) div ?MIB,
    MinMemoryMB = erlang:max(MinMemoryMB0, BucketsQuota),
    check_min_quota(kv, MinMemoryMB, Quota);
check_service_quota(Service, Quota, _) ->
    check_min_quota(Service, min_quota(Service), Quota).

check_min_quota(_Service, MinQuota, Quota) when Quota >= MinQuota ->
    ok;
check_min_quota(Service, MinQuota, Quota) ->
    {error, {service_quota_too_low, Service, Quota, MinQuota}}.

%% check that the node has enough memory for the quotas; note that we do not
%% validate service quota values because we expect them to be validated by the
%% calling side
-spec check_this_node_quotas([service()], quotas()) -> quota_result().
check_this_node_quotas(Services, Quotas0) ->
    Quotas = [{S, Q} || {S, Q} <- Quotas0, lists:member(S, Services)],
    MemoryData = this_node_memory_data(),
    TotalQuota = lists:sum([Q || {_, Q} <- Quotas]),

    check_node_total_quota(node(), TotalQuota, MemoryData).

get_memory_quota(Service) ->
    get_memory_quota(ns_config:latest(), Service).

service_to_quota_key(kv) ->
    memory_quota;
service_to_quota_key(fts) ->
    fts_memory_quota;
service_to_quota_key(cbas) ->
    cbas_memory_quota.

get_memory_quota(Config, index) ->
    NotFound = make_ref(),
    case index_settings_manager:get_from_config(Config, memoryQuota, NotFound) of
        NotFound ->
            not_found;
        Quota ->
            {ok, Quota}
    end;
get_memory_quota(Config, Service) ->
    case ns_config:search(Config, service_to_quota_key(Service)) of
        {value, Quota} ->
            {ok, Quota};
        false ->
            not_found
    end.

get_memory_quota(Config, Service, Default) ->
    case get_memory_quota(Config, Service) of
        {ok, Quota} ->
            Quota;
        not_found ->
            Default
    end.

set_quotas(Config, Quotas) ->
    RV = ns_config:run_txn_with_config(
           Config,
           fun (Cfg, SetFn) ->
                   NewCfg =
                       lists:foldl(
                         fun ({Service, Quota}, Acc) ->
                                 do_set_memory_quota(Service, Quota, Acc, SetFn)
                         end, Cfg, Quotas),
                   {commit, NewCfg}
           end),

    case RV of
        {commit, _} ->
            ok;
        retry_needed ->
            retry_needed
    end.

do_set_memory_quota(index, Quota, Cfg, SetFn) ->
    Txn = index_settings_manager:update_txn([{memoryQuota, Quota}]),

    {commit, NewCfg, _} = Txn(Cfg, SetFn),
    NewCfg;
do_set_memory_quota(Service, Quota, Cfg, SetFn) ->
    SetFn(service_to_quota_key(Service), Quota, Cfg).

default_quota(Service, Memory, Max) ->
    {Min, Quota} = do_default_quota(Service, Memory),

    %% note that this prefers enforcing minimum quota which for very small
    %% amounts of RAM can result in combined quota be actually larger than RAM
    %% size; but we don't really support such small machines anyway
    if Quota < Min ->
            Min;
       Quota > Max ->
            Max;
       true ->
            Quota
    end.

do_default_quota(kv, Memory) ->
    KvQuota = (Memory * 7) div 20,
    {?MIN_BUCKET_QUOTA, KvQuota};
do_default_quota(index, Memory) ->
    IndexQuota = (Memory * 3) div 5,
    {min_quota(index), IndexQuota};
do_default_quota(fts, Memory) ->
    FTSQuota = min(Memory div 5, ?MAX_DEFAULT_FTS_QUOTA),
    {min_quota(fts), FTSQuota};
do_default_quota(cbas, Memory) ->
    CBASQuota = (Memory * 7) div 13,
    {min_quota(cbas), CBASQuota}.

services_ranking() ->
    [kv, cbas, index, fts].

default_quotas(Services) ->
    %% this is actually bogus, because nodes can be heterogeneous; but that's
    %% best we can do
    MemSupData = this_node_memory_data(),
    default_quotas(Services, MemSupData).

default_quotas(Services, MemSupData) ->
    {MemoryBytes, _, _} = MemSupData,
    Memory = MemoryBytes div ?MIB,
    MemoryMax = allowed_memory_usage_max(MemSupData),

    {_, _, Result} =
        lists:foldl(
          fun (Service, {AccMem, AccMax, AccResult} = Acc) ->
                  case lists:member(Service, Services) of
                      true ->
                          Quota = default_quota(Service, AccMem, AccMax),
                          AccMem1 = AccMem - Quota,
                          AccMax1 = AccMax - Quota,
                          AccResult1 = [{Service, Quota} | AccResult],

                          {AccMem1, AccMax1, AccResult1};
                      false ->
                          Acc
                  end
          end, {Memory, MemoryMax, []}, services_ranking()),

    Result.
