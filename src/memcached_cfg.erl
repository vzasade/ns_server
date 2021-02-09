%% @author Couchbase <info@couchbase.com>
%% @copyright 2016-2020 Couchbase, Inc.
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
%% @doc behavior for maintaining memcached configuration files

-module(memcached_cfg).

-behaviour(gen_server).

-include("cut.hrl").

-export([start_link/2, sync/1]).

%% gen_event callbacks
-export([init/1, handle_cast/2, handle_call/3,
         handle_info/2, terminate/2, code_change/3]).

-export([rename_and_refresh/3]).

-export([format_status/2]).

-callback init() -> term().
-callback filter_event(term()) -> boolean().
-callback handle_event(term(), term()) -> {changed, term()} | unchanged.
-callback producer(term()) -> pipes:producer(iolist()).
-callback refresh() -> term().

-include("ns_common.hrl").

-record(state, {stuff,
                module,
                write_pending,
                path,
                tmp_path}).

format_status(_Opt, [_PDict, #state{module = Mod, stuff = Stuff} = State]) ->
    case erlang:function_exported(Mod, format_status, 1) of
        true ->
            State#state{stuff = Mod:format_status(Stuff)};
        false ->
            State
    end.

sync(Module) ->
    ns_config:sync_announcements(),
    gen_server:call(Module, sync, infinity).

start_link(Module, Path) ->
    gen_server:start_link({local, Module}, ?MODULE, [Module, Path], []).

init([Module, Path]) ->
    ?log_debug("Init config writer for ~p, ~p", [Module, Path]),
    Pid = self(),
    EventHandler =
        fun (Key) ->
                case Module:filter_event(Key) of
                    true ->
                        gen_server:cast(Pid, {event, Key});
                    false ->
                        ok
                end
        end,
    chronicle_compat:subscribe_to_key_change(EventHandler),
    ns_pubsub:subscribe_link(user_storage_events,
                             fun ({Key, _}) -> EventHandler(Key) end),

    Stuff = Module:init(),
    State = #state{path = Path,
                   tmp_path = Path ++ ".tmp",
                   stuff = Stuff,
                   module = Module,
                   write_pending = false},

    ok = write_cfg(State),
    {ok, State}.

terminate(_Reason, _State)     -> ok.
code_change(_OldVsn, State, _) -> {ok, State}.

handle_cast(write_cfg, State) ->
    ok = write_cfg(State),
    {noreply, State#state{write_pending = false}};
handle_cast({event, Key}, State = #state{module = Module,
                                         stuff = Stuff}) ->
    case Module:handle_event(Key, Stuff) of
        {changed, NewStuff} ->
            {noreply, initiate_write(State#state{stuff = NewStuff})};
        unchanged ->
            {noreply, State}
    end.

handle_call(sync, _From, State) ->
    {reply, ok, State}.

handle_info(_Info, State) ->
    {noreply, State}.

initiate_write(#state{write_pending = true} = State) ->
    State;
initiate_write(#state{module = Module} = State) ->
    gen_server:cast(Module, write_cfg),
    State#state{write_pending = true}.

write_cfg(#state{path = Path,
                 tmp_path = TmpPath,
                 stuff = Stuff,
                 module = Module} = State) ->
    case Module:producer(Stuff) of
        undefined ->
            ok;
        Producer ->
            ok = filelib:ensure_dir(TmpPath),
            ?log_debug("Writing config file for: ~p", [Path]),
            misc:write_file(TmpPath,
                            ?cut(pipes:run(Producer, pipes:write_file(_)))),
            rename_and_refresh(State, 5, 101)
    end.

rename_and_refresh(#state{path = Path,
                          tmp_path = TmpPath,
                          module = Module} = State, Tries, SleepTime) ->
    case file:rename(TmpPath, Path) of
        ok ->
            case (catch Module:refresh()) of
                ok ->
                    ok;
                %% in case memcached is not yet started
                {error, couldnt_connect_to_memcached} ->
                    ok;
                Error ->
                    ?log_error("Failed to force update of memcached configuration for ~p:~p",
                               [Path, Error])
            end;
        {error, Reason} ->
            ?log_warning("Error renaming ~p to ~p: ~p", [TmpPath, Path, Reason]),
            case Tries of
                0 ->
                    {error, Reason};
                _ ->
                    ?log_info("Trying again after ~p ms (~p tries remaining)",
                              [SleepTime, Tries]),
                    {ok, _TRef} = timer:apply_after(SleepTime, ?MODULE, rename_and_refresh,
                                                    [State, Tries - 1, SleepTime * 2.0])
            end
    end.
