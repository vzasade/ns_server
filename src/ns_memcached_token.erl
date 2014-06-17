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
%% @doc service that maintains memcached session token
%%
-module(ns_memcached_token).

-behaviour(gen_server).

-include("ns_common.hrl").
-include("mc_constants.hrl").
-include("mc_entry.hrl").

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-export([start_link/0]).

-export([get_token/0, release_token/0]).

-record(state, {
          token = undefined :: integer() | undefined,
          memcacheds = dict:new() % Pid => MonRef
         }).

-define(SET_TOKEN_POLL_INTERVAL, 100).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    ?log_debug("Starting ns_memcached_token service."),
    {ok, #state{}}.

get_token() ->
    gen_server:call(?MODULE, get_token, infinity).

release_token() ->
    gen_server:call(?MODULE, release_token, infinity).

handle_info({'DOWN', MonRef, process, Pid, _Reason}, #state{memcacheds = Memcacheds} = State) ->
    MonRef = dict:fetch(Pid, Memcacheds),
    {noreply, State#state{memcacheds = dict:erase(Pid, Memcacheds),
                          token = undefined}};
handle_info(_, State) ->
    {noreply, State}.

handle_cast(_, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

handle_call(get_token, {Pid, _}, State) ->
    State1 = maybe_monitor_memcached(Pid, State),
    case maybe_create_token(State1) of
        {ok, State2} ->
            {reply, {ok, State2#state.token}, State2};
        {Error, State2} ->
            {reply, Error, State2}
    end;
handle_call(release_token, {Pid, _}, #state{memcacheds = Memcacheds} = State) ->
    true = erlang:demonitor(dict:fetch(Pid, Memcacheds), [flush]),
    {reply, ok, State#state{memcacheds = dict:erase(Pid, Memcacheds)}}.

maybe_monitor_memcached(Pid, #state{memcacheds = Memcacheds} = State) ->
    Memcacheds1 =
        case dict:is_key(Pid, Memcacheds) of
            false ->
                MonRef = erlang:monitor(process, Pid),
                dict:store(Pid, MonRef, Memcacheds);
            true ->
                Memcacheds
        end,
    State#state{memcacheds = Memcacheds1}.

maybe_create_token(#state{token = undefined} = State) ->
    misc:executing_on_new_process(
      fun () ->
              case ns_memcached_sockets_pool:take_socket() of
                  {ok, Sock} ->
                      Result = case create_token(Sock) of
                                   {ok, Token} ->
                                       {ok, State#state{token = Token}};
                                   Error ->
                                       {Error, State}
                               end,
                      ns_memcached_sockets_pool:put_socket(Sock),
                      Result;
                  Error ->
                      {Error, State}
              end
      end);
maybe_create_token(State) ->
    {ok, State}.

create_token(Sock) ->
    case mc_client_binary:get_ctrl_token(Sock) of
        {ok, OldToken} ->
            ?log_debug("Got token ~p from ep-engine.", [OldToken]),
            {MegaSecs,Secs,MicroSecs} = erlang:now(),
            create_token(Sock, OldToken, (MegaSecs*1000000 + Secs)*1000000 + MicroSecs);
        Error ->
            Error
    end.

create_token(Sock, OldToken, NewToken) ->
    ?log_debug("Set token ~p to ep-engine. Old token = ~p", [NewToken, OldToken]),
    case mc_client_binary:set_ctrl_token(Sock, OldToken, NewToken) of
        {memcached_error, key_eexists, NewOldToken} ->
            ?log_debug("Ep-engine token mismatch. Retry. Old token = p, New token = ~p",
                       [OldToken, NewToken]),
            create_token(Sock, NewOldToken, NewToken);
        {memcached_error, ebusy, _} ->
            ?log_debug("Got ebusy from set_ctrl_token. Retry after ~p ms", [?SET_TOKEN_POLL_INTERVAL]),
            timer:sleep(?SET_TOKEN_POLL_INTERVAL),
            create_token(Sock, OldToken, NewToken);
        Other ->
            Other
    end.
