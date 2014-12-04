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
-module(ns_server_sup).

-behaviour(supervisor).

-include("ns_common.hrl").

-export([node_name_changed/0,
         start_link/0]).

-export([init/1]).

-export([doc/0]).

%% @doc Notify the supervisor that the node's name has changed so it
%% can restart children that care.
node_name_changed() ->
    ok = supervisor2:terminate_child(?MODULE, ns_doctor),
    {ok, _} = supervisor2:restart_child(?MODULE, ns_doctor),
    ok = supervisor2:terminate_child(?MODULE, mb_master),
    {ok, _} = supervisor2:restart_child(?MODULE, mb_master),
    ok.

doc() ->
    {supervisor, ?MODULE, {mode, one_for_one},
     [
      ns_disksup:doc(),
      {work_queue, diag_handler_worker},
      dir_size:doc(),
      request_throttler:doc(),
      ns_log:doc(),
      ns_log:doc_crash_consumer(),
      ns_config_isasl_sync:doc(),
      {gen_event, ns_log_events, "ns_log logged events are broadcasted here. ns_mail_log uses it"},
      ns_node_disco_sup:doc(),
      vbucket_map_mirror:doc(),
      bucket_info_cache:doc(),
      {gen_event, ns_tick_event,
       "local tick event. Singleton ns_tick on master node will fire tick events on all nodes"},
      {gen_event, buckets_events,
       "fired when bucket is started/warmed-up/shutdown. Also fired by ns_doctor" ++
           " when remote bucket startup/warmup/shutdown discovered"},
      ns_mail_sup:doc(),
      {gen_event, ns_stats_event,
       "various stats collectors (for all buckets) fire newly gathered stats" ++
           " samples via this guy. NOTE: 1.8.1 spawns this a bit later"},
      samples_loader_tasks:doc(),
      ns_heart_sup:doc(),
      ns_doctor:doc(),
      remote_clusters_info:doc(),
      master_activity_events:doc(),
      mb_master:doc(),
      master_activity_events:doc_ingress(),
      master_activity_events:doc_timestamper(),
      master_activity_events_pids_watcher:doc(),
      master_activity_events_keeper:doc(),
      menelaus_sup:doc(),
      ns_ports_setup:doc(),
      ns_memcached_sockets_pool:doc(),
      memcached_config_mgr:doc(),
      ns_ports_setup:doc_memcached_force_killer(),
      ns_memcached_log_rotator:doc(),
      memcached_clients_pool:doc(),
      proxied_memcached_clients_pool:doc(),
      {gen_server, xdc_lhttpc_pool,
       "instance of lhttpc_manager used by xdcr lhttp requests"},
      ns_null_connection_pool:doc(),
      xdcr_sup:doc(),
      xdcr_dcp_sockets_pool:doc(),
      ns_bucket_worker_sup:doc(single_bucket_sup),
      system_stats_collector:doc(),
      stats_archiver:doc("@system"),
      stats_reader:doc("@system"),
      compaction_daemon:doc(),
      compaction_new_daemon:doc(),
      cluster_logs_sup:doc()
     ]}.

start_link() ->
    supervisor2:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    pre_start(),
    {ok, {{one_for_one,
           misc:get_env_default(max_r, 3),
           misc:get_env_default(max_t, 10)},
          child_specs()}}.

pre_start() ->
    misc:make_pidfile(),
    misc:ping_jointo().

child_specs() ->
    [{ns_disksup, {ns_disksup, start_link, []},
      {permanent, 4}, 1000, worker, []},

     {diag_handler_worker, {work_queue, start_link, [diag_handler_worker]},
      permanent, 1000, worker, []},

     {dir_size, {dir_size, start_link, []},
      permanent, 1000, worker, [dir_size]},

     {request_throttler, {request_throttler, start_link, []},
      permanent, 1000, worker, [request_throttler]},

     %% ns_log starts after ns_config because it needs the config to
     %% find where to persist the logs
     {ns_log, {ns_log, start_link, []},
      permanent, 1000, worker, [ns_log]},

     {ns_crash_log_consumer, {ns_log, start_link_crash_consumer, []},
      {permanent, 4}, 1000, worker, []},

     %% Track bucket configs and ensure isasl is sync'd up
     {ns_config_isasl_sync, {ns_config_isasl_sync, start_link, []},
      permanent, 1000, worker, []},

     {ns_log_events, {gen_event, start_link, [{local, ns_log_events}]},
      permanent, 1000, worker, dynamic},

     {ns_node_disco_sup, {ns_node_disco_sup, start_link, []},
      permanent, infinity, supervisor,
      [ns_node_disco_sup]},

     {vbucket_map_mirror, {vbucket_map_mirror, start_link, []},
      permanent, brutal_kill, worker, []},

     {bucket_info_cache, {bucket_info_cache, start_link, []},
      permanent, brutal_kill, worker, []},

     %% Start ns_tick_event before mb_master as auto_failover needs it
     {ns_tick_event, {gen_event, start_link, [{local, ns_tick_event}]},
      permanent, 1000, worker, dynamic},

     {buckets_events, {gen_event, start_link, [{local, buckets_events}]},
      permanent, 1000, worker, dynamic},

     {ns_mail_sup, {ns_mail_sup, start_link, []},
      permanent, infinity, supervisor, [ns_mail_sup]},

     {ns_stats_event, {gen_event, start_link, [{local, ns_stats_event}]},
      permanent, 1000, worker, dynamic},

     {samples_loader_tasks, {samples_loader_tasks, start_link, []},
      permanent, 1000, worker, []},

     {ns_heart_sup, {ns_heart_sup, start_link, []},
      permanent, infinity, supervisor, [ns_heart_sup]},

     {ns_doctor, {ns_doctor, start_link, []},
      permanent, 1000, worker, [ns_doctor]},

     {remote_clusters_info, {remote_clusters_info, start_link, []},
      permanent, 1000, worker, [remote_servers_info]},

     {master_activity_events, {gen_event, start_link, [{local, master_activity_events}]},
      permanent, brutal_kill, worker, dynamic},

     %% Starts mb_master_sup, which has all processes that start on the master
     %% node.
     {mb_master, {mb_master, start_link, []},
      permanent, infinity, supervisor, [mb_master]},

     {master_activity_events_ingress, {gen_event, start_link, [{local, master_activity_events_ingress}]},
      permanent, brutal_kill, worker, dynamic},

     {master_activity_events_timestamper, {master_activity_events, start_link_timestamper, []},
      permanent, brutal_kill, worker, dynamic},

     {master_activity_events_pids_watcher, {master_activity_events_pids_watcher, start_link, []},
      permanent, brutal_kill, worker, dynamic},

     {master_activity_events_keeper, {master_activity_events_keeper, start_link, []},
      permanent, brutal_kill, worker, dynamic},

     {menelaus, {menelaus_sup, start_link, []},
      permanent, infinity, supervisor,
      [menelaus_sup]},

     %% Note: cert and private key files are set up as part of
     %% menelaus_sup. So ns_ports_setup needs to go after it.
     {ns_ports_setup, {ns_ports_setup, start, []},
      {permanent, 4}, brutal_kill, worker, []},

     {ns_memcached_sockets_pool, {ns_memcached_sockets_pool, start_link, []},
      permanent, 1000, worker, []},

     {memcached_config_mgr, {memcached_config_mgr, start_link, []},
      {permanent, 4}, 1000, worker, []},

     {ns_port_memcached_killer, {ns_ports_setup, start_memcached_force_killer, []},
      permanent, brutal_kill, worker, []},

     {ns_memcached_log_rotator, {ns_memcached_log_rotator, start_link, []},
      permanent, 1000, worker, [ns_memcached_log_rotator]},

     {memcached_clients_pool, {memcached_clients_pool, start_link, []},
      permanent, 1000, worker, []},

     {proxied_memcached_clients_pool, {proxied_memcached_clients_pool, start_link, []},
      permanent, 1000, worker, []},

     %% this may spuriously fails sometimes when our xdcr workers
     %% crash and exit message is sent to this guy supposedly via
     %% socket links (there's brief window of time when both
     %% lhttpc_client process and this guy are linked to socket as
     %% part of passing ownership to client).
     %%
     %% When this crash happens we usually end up crashing
     %% continuously because all clients are sending done back to new
     %% process and new process, unaware of old clients, crashes. And
     %% it happens continuously and may cause
     %% reached_max_restart_intensity.
     %%
     %% So in order to at least prevent max restart intensity
     %% condition we just use supervisor2 feature to keep restarting
     %% this sucker. Due to restart delay we should reach condition
     %% when all clients have died and new guy won't receive done-s.
     {xdc_lhttpc_pool, {lhttpc_manager, start_link, [[{name, xdc_lhttpc_pool}, {connection_timeout, 120000}, {pool_size, 200}]]},
      {permanent, 1}, 10000, worker, [lhttpc_manager]},

     {ns_null_connection_pool, {ns_null_connection_pool, start_link, [ns_null_connection_pool]},
      permanent, 1000, worker, []},

     %% xdcr subsystem supervisor
     {xdcr_sup,
      {xdcr_sup, start_link, []},
      permanent, infinity, supervisor, []},

     {xdcr_dcp_sockets_pool, {xdcr_dcp_sockets_pool, start_link, []},
      permanent, 1000, worker, []},

     {ns_bucket_worker_sup, {ns_bucket_worker_sup, start_link, [single_bucket_sup]},
      permanent, infinity, supervisor, [ns_bucket_worker_sup]},

     {system_stats_collector, {system_stats_collector, start_link, []},
      permanent, 1000, worker, [system_stats_collector]},

     {{stats_archiver, "@system"}, {stats_archiver, start_link, ["@system"]},
      permanent, 1000, worker, [stats_archiver]},

     {{stats_reader, "@system"}, {stats_reader, start_link, ["@system"]},
      permanent, 1000, worker, [start_reader]},

     {compaction_daemon, {compaction_daemon, start_link, []},
      permanent, 1000, worker, [compaction_daemon]},

     {compaction_new_daemon, {compaction_new_daemon, start_link, []},
      {permanent, 4}, 86400000, worker, [compaction_new_daemon]},

     {cluster_logs_sup, {cluster_logs_sup, start_link, []},
      permanent, infinity, supervisor, []}
    ].
