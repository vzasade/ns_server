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
-module(ns_heart_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-export([doc/0]).

doc() ->
    {supervisor, ?MODULE, {mode, rest_for_one},
     [
      ns_heart:doc(),
      ns_heart:doc_slow_updater()
     ]}.

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, {{rest_for_one,
           misc:get_env_default(max_r, 3),
           misc:get_env_default(max_t, 10)},
          child_specs()}}.

child_specs() ->
    [{ns_heart, {ns_heart, start_link, []},
      permanent, 1000, worker, [ns_heart]},
     {ns_heart_slow_updater, {ns_heart, start_link_slow_updater, []},
      permanent, 1000, worker, [ns_heart]}].
