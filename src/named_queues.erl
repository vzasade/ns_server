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
%% @doc this module implements the collection of named queues
%% that insures that the tasks with the same name are never
%% executed in parallel
%% execute_task - call that blocks for the duration of waiting
%% in the queue and task execution

-module(named_queues).

-include("ns_common.hrl").

-behaviour(gen_server).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-export([start_link/1, execute_task/4]).

start_link(ServerName) ->
    gen_server:start_link({local, ServerName}, ?MODULE, [], []).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

init([]) ->
    {ok, orddict:new(), hibernate}.

handle_call({execute_task, Name, Task, Params}, From, Queues) ->
    case orddict:find(Name, Queues) of
        {ok, _} ->
            ?log_debug("Task ~p is already running. Enqueue.", [Name]),
            {noreply, orddict:append(Name, {From, Task, Params}, Queues)};
        _ ->
            self() ! {do_execute, Name, Task, Params},
            {noreply, orddict:store(Name, [{From, Task, Params}], Queues)}
    end.

handle_info({do_execute, Name, Task, Params}, Queues) ->
    Parent = self(),
    proc_lib:spawn_link(
      fun () ->
              Result = try
                           Task(Params)
                       catch
                           exit:normal ->
                               ok;
                           _Class:Reason ->
                               Reason
                       end,
              Parent ! {done, Name, Result}
      end),
    {noreply, Queues};

handle_info({done, Name, Result}, Queues) ->
    case orddict:find(Name, Queues) of
        {ok, [{From, _Task, _Params}]} ->
            gen_server:reply(From, Result),
            {noreply, orddict:erase(Name, Queues), hibernate};
        {ok, [{From, _Task, _Params} | [{_From, Task, Params} | _Rest] = OtherTasks]} ->
            gen_server:reply(From, Result),
            self() ! {do_execute, Name, Task, Params},
            {noreply, orddict:store(Name, OtherTasks, Queues)};
        _ ->
            ?log_warning("Got reply ~p from the task ~p that was not scheduled",
                         [Result, Name]),
            {noreply, Queues}
    end.

handle_cast(_, State) ->
    {noreply, State}.

execute_task(ServerName, Name, Task, Params) ->
    gen_server:call(ServerName, {execute_task, Name, Task, Params}, infinity).
