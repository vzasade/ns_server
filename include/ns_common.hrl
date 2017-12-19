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
%% @doc Macros used all over the place.
%%

-ifndef(_NS_COMMON__HRL_).
-define(_NS_COMMON__HRL_,).

-type bucket_name() :: nonempty_string().
-type bucket_type() :: memcached | membase.
-type histogram() :: [{atom(), non_neg_integer()}].
-type vbucket_map() :: [[atom(), ...], ...].
-type mc_error_atom() :: key_enoent | key_eexists | e2big | einval |
                         not_stored | delta_badval | not_my_vbucket |
                         unknown_command | enomem | not_supported | internal |
                         ebusy | etmpfail | auth_error | auth_continue.
-type mc_error() :: {memcached_error, mc_error_atom(), binary()}.
-type moves() :: [{non_neg_integer(), atom(), atom()}].
-type vbucket_id() :: non_neg_integer().
-type vbucket_state() :: active | dead | replica | pending.
-type rev_id() :: <<_:128>>.
-type seq_no() :: non_neg_integer().
-type rev() :: {seq_no(), rev_id()}.
-type rebalance_vbucket_state() :: passive | undefined | paused.
-type janitor_item() :: services | {bucket, bucket_name()}.

-type ext_bucket_name() :: bucket_name() | binary().
%% ext vbucket id is vbucket id (potentially as binary) or <<"master">>
-type ext_vbucket_id() :: vbucket_id() | binary().

-type version() :: {list(integer()), candidate | release, integer()}.

-type checkpoint_id() :: non_neg_integer().

-type bucket_replication_type() :: tap | dcp.

-type dcp_error() :: {dcp_error, mc_error_atom(), binary()}.
-type dcp_conn_name() :: nonempty_string().
-type dcp_conn_type() :: consumer | producer | notifier.

-type service() :: kv | index | n1ql | fts | eventing | cbas | example.

-define(MULTICALL_DEFAULT_TIMEOUT, 30000).

-define(MIB, 1048576).

-define(VBMAP_HISTORY_SIZE, 10).
-define(NUM_NS_MEMCACHED_DATA_INSTANCES, 4).

-define(DEFAULT_LOG_FILENAME, "info.log").
-define(ERRORS_LOG_FILENAME, "error.log").
-define(VIEWS_LOG_FILENAME, "views.log").
-define(MAPREDUCE_ERRORS_LOG_FILENAME, "mapreduce_errors.log").
-define(COUCHDB_LOG_FILENAME, "couchdb.log").
-define(DEBUG_LOG_FILENAME, "debug.log").
-define(XDCR_LOG_FILENAME, "xdcr.log").
-define(XDCR_ERRORS_LOG_FILENAME, "xdcr_errors.log").
-define(STATS_LOG_FILENAME, "stats.log").
-define(BABYSITTER_LOG_FILENAME, "babysitter.log").
-define(SSL_PROXY_LOG_FILENAME, "ssl_proxy.log").
-define(NS_COUCHDB_LOG_FILENAME, "ns_couchdb.log").
-define(REPORTS_LOG_FILENAME, "reports.log").
-define(XDCR_TRACE_LOG_FILENAME, "xdcr_trace.log").
-define(ACCESS_LOG_FILENAME, "http_access.log").
-define(INT_ACCESS_LOG_FILENAME, "http_access_internal.log").
-define(GOXDCR_LOG_FILENAME, "goxdcr.log").
-define(QUERY_LOG_FILENAME, "query.log").
-define(PROJECTOR_LOG_FILENAME, "projector.log").
-define(INDEXER_LOG_FILENAME, "indexer.log").
-define(METAKV_LOG_FILENAME, "metakv.log").
-define(FTS_LOG_FILENAME, "fts.log").
-define(JSON_RPC_LOG_FILENAME, "json_rpc.log").
-define(EVENTING_LOG_FILENAME, "eventing.log").
-define(CBAS_LOG_FILENAME, "analytics.log").

-define(NS_SERVER_LOGGER, ns_server).
-define(COUCHDB_LOGGER, couchdb).
-define(USER_LOGGER, user).
-define(MENELAUS_LOGGER, menelaus).
-define(NS_DOCTOR_LOGGER, ns_doctor).
-define(STATS_LOGGER, stats).
-define(REBALANCE_LOGGER, rebalance).
-define(CLUSTER_LOGGER, cluster).
-define(VIEWS_LOGGER, views).
%% The mapreduce logger is used by the couchdb component, hence don't wonder
%% if you can't find any calls to it in ns_server
-define(MAPREDUCE_ERRORS_LOGGER, mapreduce_errors).
-define(XDCR_LOGGER, xdcr).
-define(XDCR_TRACE_LOGGER, xdcr_trace).
-define(ACCESS_LOGGER, access).
-define(METAKV_LOGGER, metakv).
-define(JSON_RPC_LOGGER, json_rpc).

-define(LOGGERS, [?NS_SERVER_LOGGER,
                  ?USER_LOGGER, ?MENELAUS_LOGGER,
                  ?NS_DOCTOR_LOGGER, ?STATS_LOGGER,
                  ?REBALANCE_LOGGER, ?CLUSTER_LOGGER,
                  ?XDCR_LOGGER, ?XDCR_TRACE_LOGGER,
                  ?METAKV_LOGGER, ?JSON_RPC_LOGGER]).

-define(NS_COUCHDB_LOGGERS, [?NS_SERVER_LOGGER,
                             ?COUCHDB_LOGGER,
                             ?VIEWS_LOGGER,
                             ?MAPREDUCE_ERRORS_LOGGER,
                             ?XDCR_LOGGER]).

-define(LOG(Level, Format, Args),
        ale:log(?NS_SERVER_LOGGER, Level, Format, Args)).

-define(log_debug(Format, Args), ale:debug(?NS_SERVER_LOGGER, Format, Args)).
-define(log_debug(Msg), ale:debug(?NS_SERVER_LOGGER, Msg)).

-define(log_info(Format, Args), ale:info(?NS_SERVER_LOGGER, Format, Args)).
-define(log_info(Msg), ale:info(?NS_SERVER_LOGGER, Msg)).

-define(log_warning(Format, Args), ale:warn(?NS_SERVER_LOGGER, Format, Args)).
-define(log_warning(Msg), ale:warn(?NS_SERVER_LOGGER, Msg)).

-define(log_error(Format, Args), ale:error(?NS_SERVER_LOGGER, Format, Args)).
-define(log_error(Msg), ale:error(?NS_SERVER_LOGGER, Msg)).

%% Log to user visible logs using combination of ns_log and ale routines.
-define(user_log(Code, Msg), ?user_log_mod(?MODULE, Code, Msg)).
-define(user_log_mod(Module, Code, Msg),
        ale:xlog(?USER_LOGGER, ns_log_sink:get_loglevel(Module, Code),
                 {Module, Code}, Msg)).

-define(user_log(Code, Fmt, Args), ?user_log_mod(?MODULE, Code, Fmt, Args)).
-define(user_log_mod(Module, Code, Fmt, Args),
        ale:xlog(?USER_LOGGER, ns_log_sink:get_loglevel(Module, Code),
                 {Module, Code}, Fmt, Args)).

-define(rebalance_debug(Format, Args),
        ale:debug(?REBALANCE_LOGGER, Format, Args)).
-define(rebalance_debug(Msg), ale:debug(?REBALANCE_LOGGER, Msg)).

-define(rebalance_info(Format, Args),
        ale:info(?REBALANCE_LOGGER, Format, Args)).
-define(rebalance_info(Msg), ale:info(?REBALANCE_LOGGER, Msg)).

-define(rebalance_warning(Format, Args),
        ale:warn(?REBALANCE_LOGGER, Format, Args)).
-define(rebalance_warning(Msg), ale:warn(?REBALANCE_LOGGER, Msg)).

-define(rebalance_error(Format, Args),
        ale:error(?REBALANCE_LOGGER, Format, Args)).
-define(rebalance_error(Msg), ale:error(?REBALANCE_LOGGER, Msg)).

-define(views_debug(Format, Args), ale:debug(?VIEWS_LOGGER, Format, Args)).
-define(views_debug(Msg), ale:debug(?VIEWS_LOGGER, Msg)).

-define(views_info(Format, Args), ale:info(?VIEWS_LOGGER, Format, Args)).
-define(views_info(Msg), ale:info(?VIEWS_LOGGER, Msg)).

-define(views_warning(Format, Args), ale:warn(?VIEWS_LOGGER, Format, Args)).
-define(views_warning(Msg), ale:warn(?VIEWS_LOGGER, Msg)).

-define(views_error(Format, Args), ale:error(?VIEWS_LOGGER, Format, Args)).
-define(views_error(Msg), ale:error(?VIEWS_LOGGER, Msg)).

-define(xdcr_debug(Format, Args), ale:debug(?XDCR_LOGGER, Format, Args)).
-define(xdcr_debug(Msg), ale:debug(?XDCR_LOGGER, Msg)).

-define(xdcr_info(Format, Args), ale:info(?XDCR_LOGGER, Format, Args)).
-define(xdcr_info(Msg), ale:info(?XDCR_LOGGER, Msg)).

-define(xdcr_warning(Format, Args), ale:warn(?XDCR_LOGGER, Format, Args)).
-define(xdcr_warning(Msg), ale:warn(?XDCR_LOGGER, Msg)).

-define(xdcr_error(Format, Args), ale:error(?XDCR_LOGGER, Format, Args)).
-define(xdcr_error(Msg), ale:error(?XDCR_LOGGER, Msg)).

-define(metakv_debug(Format, Args), ale:debug(?METAKV_LOGGER, Format, Args)).
-define(metakv_debug(Msg), ale:debug(?METAKV_LOGGER, Msg)).

-define(get_timeout(Op, Default), ns_config:get_timeout({?MODULE, Op}, Default)).
-define(get_param(Param, Default),
        ns_config:search_node_with_default({?MODULE, Param}, Default)).

-define(i2l(V), integer_to_list(V)).

-define(UI_AUTH_EXPIRATION_SECONDS, 600).

%% XDCR_CHECKPOINT_STORE is the name of the simple-store where
%% metakv stores XDCR checkpoints.
-define(XDCR_CHECKPOINT_STORE, xdcr_ckpt_data).

%% Pattern used to identify XDCR checkpoints.
-define(XDCR_CHECKPOINT_PATTERN, list_to_binary("/ckpt/")).

%% Metakv tag for values storing sensitive information
%% If this tag is changed to something else, then do not forget
%% to change its value in ns_server/scripts/dump-guts as well.
-define(METAKV_SENSITIVE, metakv_sensitive).

-define(MIN_FREE_RAM, misc:get_env_default(quota_min_free_ram, 1024)).
-define(MIN_FREE_RAM_PERCENT, 80).

-define(DEFAULT_EPHEMERAL_PURGE_INTERVAL_DAYS, 3).

%% Index storage mode values.
-define(INDEX_STORAGE_MODE_MEMORY_OPTIMIZED, <<"memory_optimized">>).
-define(INDEX_STORAGE_MODE_FORESTDB, <<"forestdb">>).
-define(INDEX_STORAGE_MODE_PLASMA, <<"plasma">>).

-define(VERSION_25, [2, 5]).
-define(VERSION_30, [3, 0]).
-define(VERSION_40, [4, 0]).
-define(VERSION_41, [4, 1]).
-define(VERSION_45, [4, 5]).
-define(VERSION_46, [4, 6]).
-define(VERSION_50, [5, 0]).
-define(VERSION_51, [5, 1]).

-define(VULCAN_VERSION_NUM, [5, 5]).

%% Points to latest release
-define(LATEST_VERSION_NUM, ?VULCAN_VERSION_NUM).
-define(MASTER_ADVERTISED_VERSION, [5, 5, 0]).

-define(DEFAULT_UI_COMPAT_VERSION, ?VERSION_45).

-endif.
