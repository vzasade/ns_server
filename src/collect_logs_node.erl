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
%% @doc Server responsible for invoking cbcollect_info and gathering the result
%% of it.
%%
%% Possible states (and transitions):
%%
%%          [idle]
%%             |
%%             | collect
%%             |
%%             V
%%        [collecting]
%%             |
%%             | <<cb_collect_info terminates>>
%%             |
%%             V
%%        [complete]
%%
%%    global:cancel
%%      --------------> [cancelled]


-module(collect_logs_node).
-behaviour(gen_server).

-include("ns_common.hrl").

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/3, cancel/1, collect/1]).

%% Start the server on the given node, with the given Upload options.
start_link(Node, Upload, Timestamp) ->
    case rpc:call(Node, gen_server, start,
                  [{local, ?MODULE}, ?MODULE, [Upload, Timestamp], []]) of
        {ok, Pid} ->
            true = link(Pid),
            {ok, Pid};
        X ->
            X
    end.

%% Cancel log collection (whereever or not we are already collecting,
%% and terminate the process.
cancel(Pid) ->
    gen_server:call(Pid, cancel).

%% Begin collecting logs. Needs the Caller's Pid to later notify it the results
%% of the collection.
collect(Pid) ->
    gen_server:call(Pid, collect).

%% ====================================================================
%% Behavioural functions
%% ====================================================================

-record(state, {status,      % current status of node (idle, collecting).
                port,        % port for cbcollect_info external process.
                manager,     % pid of manager process to reply to when we complete.
                upload=[],   % proplist of upload options.
                result=[],   % proplist of result of the collection.
                timestamp}). % timestamp to prepend to zip file name.



init([Upload, TimeStamp]) ->
    process_flag(trap_exit, true),
    State = #state{status = idle, upload=Upload, timestamp=TimeStamp},
    {ok, State}.

handle_call(cancel, _From, State) ->
    {stop, normal, cancelled, State};

handle_call(collect, {FromPid, _Tag}, State = #state{status = idle}) ->
    %% Start the external cbcollect_info process.
    try
        Port = run_cbcollect_info(State),
        {reply, collecting, State#state{status = collecting,
                                        manager=FromPid,
                                        port=Port}}
    catch error:enoent ->
        {reply, {error, "Missing cbcollect_info program"}, State}
    end.

handle_cast(Msg, State) ->
    ?log_warning("Unexpected async message: ~p~n", [Msg]),
    {noreply, State}.

handle_info({Port,{exit_status,Status}}, #state{manager = Mgr,
                                                result  = Result,
                                                port    = Port} = State) ->
    % Should have had a status. If we don't already have a status, create one.
    Result1 = case proplists:is_defined(<<"status">>, Result) of
                  true  -> Result;
                  false ->
                      Details = list_to_bitstring("cbcollect_info exited without outputting a status. Exit value " ++ integer_to_list(Status)),
                      lists:merge([{<<"status">>, <<"failed">>},
                                   {<<"details">>, Details}], Result)
              end,
    try
        collect_logs_manager:report_node_result(Mgr, Result1, self())
    catch exit:{noproc, _} ->
        % Manager already terminated, not much else we can do.
            ok
    end,
    {stop, normal, State#state{result = Result1}};

handle_info({Port,{data, Data}}, #state{port=Port, result=Result} = State) ->
    {Flag, Line} = Data,
    case Flag of
        eol ->
            case parse_cbcollect_line(Line) of
                {_K, _V} = KVPair ->
                    {noreply, State#state{result = [KVPair | Result]}};
                _ ->
                    ?log_warning("Unexpected line received - ignoring: ~s", [Line]),
                    {noreply, State}
            end;
        noeol ->
            ?log_warning("recieved line > maximum expected - ignoring: ~s", [Line]),
            {noreply, State}
    end;
handle_info({'EXIT', Port, _Reason}, #state{port=Port} = State) ->
    % Nothing to do here - we use {exit_status} case above to actually act
    % on cbcollect finishing. This clause present to filter out 'EXIT' from
    % our expected Pid, which is expected and so we can safely consume it.
    {noreply, State}.

terminate(_Reason, #state{ port = Port}) ->
    % Shut down cbcollect_info if still running
    try
        port_command(Port, ["shutdown", 10])
    catch error:badarg ->
        % port already closed.
        ok
    end,
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% ====================================================================
%% Internal functions
%% ====================================================================

run_cbcollect_info(#state{upload = Upload} = State) ->
    Cmd = get_cbcollect_exe(Upload),
    Args = format_cbcollect_args(State),
    JoinedArgs = lists:foldl(fun(X, Acc) -> Acc ++ " " ++ X end, "", Args),
    TmpDir = misc:find_writable_tempdir(),
    ?log_debug("spawning ~p in cwd ~p with args:~s~n", [Cmd, TmpDir, JoinedArgs]),
    open_port({spawn_executable, Cmd},
              [exit_status,
               {args, Args},
               {line, 1024},
               {cd, TmpDir},
               binary, use_stdio]).

%% Returns a list of arguments to pass to cbcollect_info based on the Upload
%% arguments
format_cbcollect_args(#state{upload = Upload} = State) when Upload =:= false ->
    ["--script"]
    ++ [State#state.timestamp ++ "-" ++ atom_to_list(node()) ++ ".zip"];
format_cbcollect_args(#state{upload = Upload} = State) when is_list(Upload) ->
    Opts = case proplists:get_value(host, Upload) of
               undefined -> [];
               Host      -> ["--upload", "--upload-host=" ++ binary_to_list(Host)]
           end
    ++ case proplists:get_value(customer, Upload) of
           undefined -> [];
           Customer  -> ["--customer=" ++ binary_to_list(Customer)]
       end
    ++ case proplists:get_value(ticket, Upload) of
           undefined -> [];
           Ticket    -> ["--ticket=" ++ binary_to_list(Ticket)]
       end
    %% Unit test support - append any arguments specified in 'test_args'
    ++ case proplists:get_value(extra_args, Upload) of
           undefined -> [];
           Extra     -> Extra
    end,
    ["--script"] ++ Opts
    ++ [State#state.timestamp ++ "-" ++ atom_to_list(node()) ++ ".zip"].


%% Return the path of the cbcollect executable.
%% For unit test support - allow exe name to be overridden by 'cbcollect_name'
%% / cbcollect_path properties in Upload.
get_cbcollect_exe(false) ->
    filename:join(default_dir(), default_exe());
get_cbcollect_exe(Upload) when is_list(Upload) ->
    Name = case proplists:get_value(cbcollect_name, Upload) of
                 undefined -> default_exe();
                 N         -> N
           end,

    BinDir = case proplists:get_value(bin_dir, Upload) of
                  undefined -> default_dir();
                  P         -> P
             end,
    filename:join(BinDir, Name).

default_exe() ->
    "cbcollect_info".

default_dir() ->
    path_config:component_path(bin).

% Parses a line of output from cbcollect_info. Expect the form "key:value"
parse_cbcollect_line(Line) when is_binary(Line) ->
    case re:split(Line, ":", [{parts, 2}, {return, binary}]) of
        [Key, Value]  ->
            {Key, trimre(Value)};
        M ->
            ?log_debug("malformed:~p~n", [M]),
            malformed
    end.

%% Given a binary, remove any leading or training whitespace.
trimre(Bin) ->
    re:replace(Bin, "^\\s+|\\s+$", "", [{return, binary}, global]).
