%% @author Couchbase, Inc <info@couchbase.com>
%% @copyright 2015 Couchbase, Inc.
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
%% A module for any not per-bucket remote calls.
-module(remote_api).

-behavior(gen_server).

-include("ns_common.hrl").

-define(DEFAULT_TIMEOUT, ns_config:get_timeout(remote_api_default, 10000)).

%% remote calls
-export([get_indexes/1, get_fts_indexes/1]).

%% gen_server callbacks and functions
-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% remote calls

%% introduced in 4.0
get_indexes(Node) ->
    do_call(Node, get_indexes).

%% introduced in watson
get_fts_indexes(Node) ->
    do_call(Node, get_fts_indexes).

%% gen_server callbacks and functions
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    {ok, {}}.

handle_call(get_indexes, _From, State) ->
    {reply, index_status_keeper:get_indexes(index), State};
handle_call(get_fts_indexes, _From, State) ->
    {reply, index_status_keeper:get_indexes(fts), State};
handle_call(Request, {Pid, _} = _From, State) ->
    ?log_warning("Got unknown call ~p from ~p (node ~p)", [Request, Pid, node(Pid)]),
    {reply, unhandled, State}.

handle_cast(Msg, State) ->
    ?log_warning("Got unknown cast ~p", [Msg]),
    {noreply, State}.

handle_info(Info, State) ->
    ?log_warning("Got unknown message ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% internal
get_timeout(Request) when is_atom(Request) ->
    ns_config:get_timeout({remote_api, Request}, ?DEFAULT_TIMEOUT).

do_call(Node, Request) ->
    gen_server:call({?MODULE, Node}, Request, get_timeout(Request)).
