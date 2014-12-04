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
%% @doc suprevisor for starting couchdb related processes for bucket on ns_couchdb node
%%      and views replication
%%
-module(docs_sup).

-behavior(supervisor).

-include("ns_common.hrl").

-export([start_link/1, init/1]).

-export([doc/1]).

doc(Bucket) ->
    {supervisor, ?MODULE, {bucket, Bucket}, {mode, one_for_all},
     "suprevisor for starting couchdb related processes for bucket on ns_couchdb node" ++
         " and views replication",
     [
      remote_monitors:doc_wait_for_net_kernel(),
      doc_replicator:doc(Bucket),
      doc_replication_srv:doc(Bucket),
      capi_set_view_manager:doc('ns_couchdb_node@ip', Bucket),
      couch_stats_reader:doc('ns_couchdb_node@ip', Bucket)
     ]
    }.

start_link(Bucket) ->
    supervisor:start_link(?MODULE, [Bucket]).

init([BucketName]) ->
    {ok, {{one_for_all,
           misc:get_env_default(max_r, 3),
           misc:get_env_default(max_t, 10)},
          child_specs(BucketName)}}.

child_specs(BucketName) ->
    [{wait_for_net_kernel,
      {remote_monitors, wait_for_net_kernel, []},
      transient, brutal_kill, worker, []},
     {doc_replicator,
      {doc_replicator, start_link, [BucketName]},
      permanent, 1000, worker, [doc_replicator]},
     {doc_replication_srv,
      {doc_replication_srv, start_link, [BucketName]},
      permanent, 1000, worker, [doc_replication_srv]},
     {capi_set_view_manager,
      {capi_set_view_manager, start_link_remote, [ns_node_disco:couchdb_node(), BucketName]},
      permanent, 1000, worker, []},
     {couch_stats_reader,
      {couch_stats_reader, start_link_remote, [ns_node_disco:couchdb_node(), BucketName]},
      permanent, 1000, worker, []}].
