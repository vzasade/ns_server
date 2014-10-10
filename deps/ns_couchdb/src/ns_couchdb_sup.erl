%% @author Couchbase <info@couchbase.com>
%% @copyright 2014 Couchbase, Inc.
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
%% @doc main supervisor for ns_couchdb node
%%

-module(ns_couchdb_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    {ok, { {one_for_one,
            misc:get_env_default(max_r, 3),
            misc:get_env_default(max_t, 10)}, child_specs()} }.

child_specs() ->
    [
     {cb_couch_sup, {cb_couch_sup, start_link, []},
      permanent, 5000, supervisor, [cb_couch_sup]},

     %% this must be placed after cb_couch_sup since couchdb starts
     %% sasl application
     {cb_init_loggers, {cb_init_loggers, start_link, []},
      transient, 1000, worker, [cb_init_loggers]},

     {ns_memcached_sockets_pool, {ns_memcached_sockets_pool, start_link, []},
      permanent, 1000, worker, []},

     {ns_couchdb_config_sup, {ns_couchdb_config_sup, start_link, []},
      permanent, infinity, supervisor,
      [ns_couchdb_config_sup]},

     {xdc_rdoc_manager, {xdc_rdoc_manager, start_link, []},
      permanent, 1000, worker, [xdc_rdoc_manager]},

     {request_throttler, {request_throttler, start_link, []},
      permanent, 1000, worker, [request_throttler]},

     {vbucket_map_mirror, {vbucket_map_mirror, start_link, []},
      permanent, brutal_kill, worker, []},

     {ns_bucket_worker_sup, {ns_bucket_worker_sup, start_link, [ns_couchdb_single_bucket_sup]},
      permanent, infinity, supervisor, [ns_bucket_worker_sup]},

     {set_view_update_daemon, {set_view_update_daemon, start_link, []},
      permanent, 1000, worker, [set_view_update_daemon]}
    ].
