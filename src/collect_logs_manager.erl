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
%% @doc Top-level task for collecting logs from one or more cluster nodes.
%% When logs are to be collected, one instance of collect_logs_manager is
%% started, which in turn will start collect_logs processes on each node where
%% logs are to be collected. When each collect_logs process completes it reports
%% information back to the manager before terminating.

-module(collect_logs_manager).

-behaviour(gen_server).

-include("ns_common.hrl").
-include_lib("eunit/include/eunit.hrl").

%% ====================================================================
%% API functions
%% ====================================================================

-export([start/0, start_link/0, stop/0, begin_collection/2, cancel_collection/0,
         get_status/0, report_node_result/3]).

-ifdef(EUNIT).
-export([test/0]).
-endif.

%% gen_server API
-export([code_change/3, handle_call/3, handle_cast/2, handle_info/2, init/1,
         terminate/2]).

%% external functions =================================================

start() ->
    gen_server:start({local, ?MODULE}, ?MODULE, [], []).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
    cancel_collection(),
    gen_server:call(?MODULE, terminate).

begin_collection(Node, Upload) ->
    gen_server:call(?MODULE, {begin_collection, Node, Upload}).

cancel_collection() ->
    gen_server:call(?MODULE, cancel_collection).

get_status() ->
    gen_server:call(?MODULE, get_status).

%% Called by collect_logs_node processes when they have completed to inform
%$ the manager of the result.
report_node_result(Pid, Result, From) ->
    gen_server:cast(Pid, {report_node_result, Result, From}).

%% gen_server API =====================================================

%% Sub-record of this processes' state. Holds information on the state of the
%% per-node child processes which actually perform log collection. Consists
%% of three lists which each process will move through in it's lifetime. A process
%% only ever exists in one list (state) :-
%% - pending (not yet collecting logs)
%% - in_progress (cbcollect_info running, awaiting the result)
%% - finished (cbcollect finished, both successfully and failed)
-record(children, {pending = []     :: [proplists:proplist()],
                   in_progress = [] :: [proplists:proplist()],
                   finished = []    :: [proplists:proplist()]}).

%% Main state of the process.
-record(state, {upload = []   :: proplists:proplist(),
                children = #children{},
                cancelled = false :: true | false,
                last_updated = 0 :: non_neg_integer()}).


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_call({begin_collection, Nodes, Upload}, _From, State=#state{}) ->
    case get_status(State) of
        idle ->
            start_log_collection(Nodes, Upload, State);
        in_progress ->
            {reply, {error, already_running}, State}
    end;

handle_call(cancel_collection, _From, State=#state{}) ->
    case get_status(State) of
        idle ->
            % Already idle; nothing to do.
            {reply, ok, State};
        in_progress ->
            ?log_info("Cancelling in-progress collection"),
            State1 = State#state{cancelled = true},
            % Kill all children still running.
            shutdown_children(State1),
            {reply, ok, State1}
    end;

handle_call(get_status, _From, State=#state{children=Children,
                                            last_updated = LastUpdate}) ->
    {reply,
     [{status, get_status(State)},
      {last_updated, LastUpdate},
      {nodes, format_node_status(Children)}],
      State};

handle_call(terminate, _From, State) ->
    State1 = State#state{cancelled = true},
    shutdown_children(State1),
    {stop, normal, ok, State1}.

handle_cast({report_node_result, Result, From}, State) ->
    ?log_info("{report_node_result, ~p} from ~p~n", [Result, From]),
    State1 = record_node_result(Result, From, State),
    {noreply, State1}.

handle_info({'EXIT', Pid, normal}, State) ->
    % normal shutdown of client. Check that we have had a status update from it.
    State1 = child_exited(Pid, State),
    {noreply, State1}.

init([]) ->
    process_flag(trap_exit, true),
    {ok, #state{last_updated = misc:now_int()}}.

terminate(_Reason, _State) ->
    ok.

%% ====================================================================
%% Internal functions
%% ====================================================================

% Start collecting logs
start_log_collection(Nodes, Upload, State) ->
    ?log_info("Nodes: ~p Upload: ~p~n", [Nodes, Upload]),
    Timestamp = misc:iso_8601_fmt(erlang:localtime()),
    Children = [{N, collect_logs_node:start_link(N, Upload, Timestamp)}
                || N <- Nodes],

    % Check if any of the nodes reported that collect_logs_node was already
    % running - if so this informs us that an existing log collection is already
    % in progress, and so return an error indicating a collection is already
    % running.
    case lists:any(fun({_Node, {error, {already_started, _Pid}}}) -> true;
                      (_)                                         -> false
                   end, Children) of
        true ->
            ?log_error("Log collection already in progress - refusing to start another. Child status: ~p~n",
                       [Children]),
            {reply, {error, already_running}, State};
        false ->
            % Partition children into those nodes which were successfully started,
            % and those which were not (and hence unreachable).
            {Started, Unreachable} =
                lists:partition(fun({_Node, {ok, _Pid}}) -> true;
                                   (_)                   -> false
                                end, Children),

            % Deal with the Unreachable ones first - they go straight to finished
            % list.
            Finished = [[{name, Node},
                         {<<"status">>, <<"failed">>},
                         {<<"details">>, <<"Node unreachable, unable to collect logs. Please run cbcollect_info manually on this node.">>}]
                        || {Node, {_Status, _Pid}} <- Unreachable],

            % Started children form the Pending list.
            Pending = [[{name, Node}, {pid, Pid}] || {Node, {ok, Pid}} <- Started],

            State1 = State#state{children = #children{pending     = Pending,
                                                      in_progress = [],
                                                      finished    = Finished},
                                 cancelled = false,
                                 last_updated = misc:now_int()},
            % Start the first child collecting.
            State2 = collect_next_child(State1),

            {reply, in_progress, State2}
    end.


% Tell all children to shutdown - cancel all pending and in_progress children.
shutdown_children(#state{children = Children}) ->
    #children{ pending = Pending, in_progress = InProgress} = Children,
    [collect_logs_node:cancel(proplists:get_value(pid, Node)) || Node <- Pending],
    [collect_logs_node:cancel(proplists:get_value(pid, Node)) || Node <- InProgress].

%% Start collecting logs from the head of the pending children list,
%% moving that node to the in_progress list.
collect_next_child(State = #state{cancelled = true}) ->
    % Collection cancelled, don't start anything else
    State;
collect_next_child(State = #state{children = Children, cancelled = false}) ->
    #children{pending     = Pending,
              in_progress = InProgress,
              finished    = Finished} = Children,

    {Pending1, InProgress1, Finished1} =
        collect_next_child1(Pending, InProgress, Finished),

    State#state{children = #children{pending     = Pending1,
                                     in_progress = InProgress1,
                                     finished    = Finished1}}.

%% Inner (recursive) helper for collect_next_child - actually starts one child
%% node collecting.
collect_next_child1([], InProgress, Finished)  ->
    {[], InProgress, Finished};
collect_next_child1(Pending, InProgress, Finished) ->
    % Pop the head of the pending list off and try to collect it.
    [PHead | PTail] = Pending,
    Pid = proplists:get_value(pid, PHead),
    case collect_logs_node:collect(Pid) of
        collecting ->
            % Success - move this child to the in-progress list.
            {PTail, [PHead | InProgress], Finished};
        {error, Error} ->
            % Failure, move to Finished list and try the next.
            collect_next_child1(PTail,
                                InProgress,
                                [PHead ++ [{error, Error}] | Finished])
    end.

%% Record the result provided by a child.
record_node_result(Result, FromPid, #state{children = Children} = State) ->
    #children{ in_progress = InProgress} = Children,

    %% Extract the node from the in-progress array.
    {Match, InProgress1} = find_pid_in_list(InProgress, FromPid),
    case Match of
        [M] ->
            % Single element match - expected. Add result to it and add
            % back to in-progress
            InProgress2 = [M ++ Result | InProgress1],
            Children1 = Children#children{ in_progress = InProgress2 },
            State#state{children = Children1};
        [] ->
            % Empty list - i.e. didn't find a matching process
            ?log_warning("Received result from ~p which is not in in_progress: ~p - ignoring.", [FromPid, InProgress]),
            State
    end.

%% For each element in the three child lists, form a proplist of the nodes'
%% status. Proplist contains 'status' and optionally 'artifact' and 'details'.
format_node_status(Children) ->
    #children{ pending     = Pending,
               in_progress = InProgress,
               finished    = Finished} = Children,

    [ format_node(pending, N) || N <- Pending]
    ++ [ format_node(in_progress, N) || N <- InProgress]
    ++ [ format_node(finished, N) || N <- Finished].


format_node(pending, Node) ->
    [{ node_name, proplists:get_value(name, Node)},
     { result, <<"pending">> },
     { details, ""}];
format_node(in_progress, Node) ->
    [{ node_name, proplists:get_value(name, Node)},
     { result, <<"in_progress">> },
     { details, ""}];
format_node(finished, Node) ->
    NameKV = [{ node_name, proplists:get_value(name, Node)}],
    ResVal = check_status(proplists:get_value(<<"status">>, Node)),
    ResKV = [{result, ResVal}],
    ArtifactKV = case proplists:get_value(<<"artifact">>, Node) of
                    undefined -> [];
                    A         -> [{artifact, A}]
                 end,
    DetailsKV = case proplists:get_value(<<"details">>, Node) of
                     undefined -> [];
                     D         -> [{details, D}]
                end,
    lists:append(lists:append(lists:append(NameKV, ResKV), ArtifactKV), DetailsKV).

%% Sanitize/check/normalize status value returned from from cbcollect_info
check_status(S = <<"collected">>)      -> S;
check_status(S = <<"uploaded">>)       -> S;
check_status(S = <<"upload-failed">>)  -> S;
check_status(S = <<"failed">>)         -> S;
check_status( <<"badargs">>)           -> <<"failed">>;
check_status(S = <<"collect-failed">>) -> S;
check_status(S = <<"cancelled">>)      -> S.
%%check_status(_) -> <<"failed">>.

%% Handle a child exiting. Move the child from in_progress -> finished, and
%% start the next one (if necessary).
child_exited(Pid, State = #state{children = Children}) ->

    #children{ pending     = Pending,
               in_progress = InProgress,
               finished    = Finished} = Children,

    % See if child was in progress; if so move from in_progress -> finished
    {Match, InProgress1} = find_pid_in_list(InProgress, Pid),

    State1 = case Match of
                [M] ->
                    % Single element match - expected. Move child to Finished.
                    M0 = ensure_child_has_status(M),
                    Finished1 = [M0 | Finished],
                    Children1 = Children#children{ in_progress = InProgress1,
                                                   finished = Finished1},
                    State#state{last_updated = misc:now_int(),
                                children = Children1};
                [] ->
                    % Empty list - i.e. didn't find a matching in_progress -
                    % check pending.
                    {Match1, Pending1} = find_pid_in_list(Pending, Pid),
                    case Match1 of
                        [M1] ->
                            % Single element in pending; move to finished.
                            M2 = ensure_child_has_status(M1),
                            Finished2 = [M2 | Finished],
                            Children2 = Children#children{ pending = Pending1,
                                                           finished = Finished2},
                            State#state{last_updated = misc:now_int(),
                                        children = Children2};
                        [] ->
                            ?log_warning("Received result from ~p which is not in in_progress or pending: - ignoring.",
                                         [Pid]),
                            State
                    end
             end,

    % start the next (if one exists).
    collect_next_child(State1).

%% Helper function to search for Pid in List of child nodes. Returns a tuple
%% {Matched, NotMatched} containing the Pid(s) matching that specified, and
%% the non-matching elements, respectively.
find_pid_in_list(List, Pid) ->
    {Match, NotMatched} =
        lists:partition(fun(Props) ->
                            case proplists:get_value(pid, Props) of
                                Pid -> true;
                                _   -> false
                            end
                        end,
                        List),
    {Match, NotMatched}.


%% Gets the current status of the collect_logs_manager (idle | in_progress)
get_status(#state{} = #state{children = Children}) ->
    #children{ pending     = Pending,
               in_progress = InProgress,
               finished    = Finished} = Children,
    get_status(Pending, InProgress, Finished).

get_status(Pending, InProgress, _Finished) when Pending =:= [], InProgress =:= [] ->
    idle;
get_status(_Pending, _InProgress, _Finished) ->
    in_progress.


%% Given a proplist representing a child node, ensure is has a <<"status">>
%% property, defaulting to "cancelled" if missing.
ensure_child_has_status(Node) ->
    case proplists:is_defined(<<"status">>, Node) of
        true ->
            Node;
        false ->
            lists:merge([{<<"status">>, <<"cancelled">>}], Node)
    end.

%% ====================================================================
%% Tests
%% ====================================================================

-ifdef(EUNIT).

test() ->
    eunit:test({module, ?MODULE}, [verbose]).

%% Test descriptions ==================================================

idle_test_() ->
    {"Ensure manager starts in the idle state.",
        {setup, fun start_single/0, fun stop/1, fun idle/1}}.

begin_cancel_test_() ->
    {"Check collect_logs can be started and immediately cancelled",
        {setup, fun start_single/0, fun stop/1, fun start_stop_single/1}}.

begin_cancel_begin_test_() ->
    {"Check collect_logs can be started, cancelled and started again",
        {setup, fun start_single/0, fun stop/1, fun start_stop_start_single/1}}.

begin_cancel_hang_test_() ->
    {"Check collect_logs can be started and immediately cancelled; when cbcollect_info hangs.",
        {setup, fun start_single/0, fun stop/1, fun start_stop_hang/1}}.

double_begin_test_() ->
    {"Ensure we can't start the same collect_logs twice",
        {setup, fun start_single/0, fun stop/1, fun double_begin/1}}.

single_node_no_upload_test_() ->
    {"Single node without upload succeeds",
        {setup, fun start_single/0, fun stop/1, fun single_node_no_upload/1}}.

single_node_upload_test_() ->
    {"Single node with upload succeeds",
        {setup, fun start_single/0, fun stop/1, fun single_node_upload/1}}.

single_node_upload_no_ticket_test_() ->
    {"Single node with upload, no ticket succeeds",
        {setup, fun start_single/0, fun stop/1, fun single_node_upload_no_ticket/1}}.

cbcollect_upload_test_() ->
    {"Ensure uploaded status from cbcollect_info is handled",
        {setup, fun start_single/0, fun stop/1, fun cbcollect_upload/1}}.

cbcollect_bad_result_test_() ->
    {"Ensure bad result code from cbcollect_info is handled",
        {setup, fun start_single/0, fun stop/1, fun cbcollect_bad_result/1}}.

cbcollect_bad_output_test_() ->
    {"Ensure unexpected, bad output from cbcollect_info is handled",
        {setup, fun start_single/0, fun stop/1, fun cbcollect_bad_output/1}}.

cbcollect_badargs_test_() ->
    {"Ensure 'badargs' error from cbcollect_info is handled",
        {setup, fun start_single/0, fun stop/1, fun cbcollect_badargs/1}}.

cbcollect_collect_failed_test_() ->
    {"Ensure 'collect-failed' error from cbcollect_info is handled",
        {setup, fun start_single/0, fun stop/1, fun cbcollect_collect_failed/1}}.

cbcollect_upload_failed_test_() ->
    {"Ensure 'collect-failed' error from cbcollect_info is handled",
        {setup, fun start_single/0, fun stop/1, fun cbcollect_upload_failed/1}}.

cbcollect_cancelled_test_() ->
    {"Ensure 'cancelled' error from cbcollect_info is handled",
        {setup, fun start_single/0, fun stop/1, fun cbcollect_cancelled/1}}.


%% Multi-node (distributed) tests

multi_begin_cancel_test_() ->
    {"Check multi-node collect_logs can be started and immediately cancelled",
        {setup, fun start_distrib/0, fun stop_distrib/1, fun multi_begin_cancel/1}}.

multi_collect_test_() ->
    {"Ensure multiple nodes can start and complete successfully",
        {setup, fun start_distrib/0, fun stop_distrib/1, fun multi_collect/1}}.

multi_collect_node_down_test_() ->
    {"Ensure multiple nodes can start when a node is down",
        {setup, fun start_distrib/0, fun stop_distrib/1, fun multi_collect_node_down/1}}.

multi_double_begin_test_() ->
    {"Ensure attempting to start collection from multiple nodes reports an errror",
        {setup, fun start_distrib/0, fun stop_distrib/1, fun multi_double_begin/1}}.


%% Setup functions ====================================================

start_single() ->
    {ok, Pid} = collect_logs_manager:start_link(),
    Pid.

stop(_Pid) ->
    collect_logs_manager:stop(),
    ok.

%% Distributed (multi-node) versions of the above.
start_distrib() ->
    make_distrib(collect_test, shortnames),
    {ok, Slave1} = start_slave_node(collect_slave1),
    {ok, Slave2} = start_slave_node(collect_slave2),
    {ok, _Pid} = collect_logs_manager:start_link(),
    [Slave1, Slave2].

stop_distrib(Slaves) ->
    collect_logs_manager:stop(),
    [ ok = slave:stop(S) || S <- Slaves],
    ok.

%% Actual Tests =======================================================

idle(_) ->
    Status = collect_logs_manager:get_status(),
    [?_assertEqual(idle, proplists:get_value(status, Status)),
     ?_assertEqual([], proplists:get_value(nodes, Status))].

start_stop_single(_) ->
    Opts = enable_cbcollect_mock(collect),
    Result = collect_logs_manager:begin_collection( [node()], Opts),
    [?_assertEqual(in_progress, Result),
     ?_assertEqual(ok, collect_logs_manager:cancel_collection())].

start_stop_start_single(_) ->
    Opts = enable_cbcollect_mock(collect),
    Result1 = collect_logs_manager:begin_collection( [node()], Opts),
    Result2 = collect_logs_manager:cancel_collection(),
    Result3 = collect_logs_manager:begin_collection( [node()], Opts),
    [?_assertEqual(in_progress, Result1),
     ?_assertEqual(ok, Result2),
     ?_assertEqual(in_progress, Result3)].

start_stop_hang(_) ->
    Opts = enable_cbcollect_mock() ++ [{extra_args, ["--mock_hang"]}],
    Result = collect_logs_manager:begin_collection( [node()], Opts),
    WaitResult = wait_for_status(in_progress, 1000),
    CancelResult = collect_logs_manager:cancel_collection(),
    [?_assertEqual(in_progress, Result),
     ?_assertEqual({ok, in_progress}, WaitResult),
     ?_assertEqual(ok, CancelResult)].

double_begin(_) ->
    Opts = enable_cbcollect_mock() ++ [{extra_args, ["--mock_hang"]}],
    Result1 = collect_logs_manager:begin_collection( [node()], Opts),
    Result2 = collect_logs_manager:begin_collection( [node()], Opts),
    [?_assertEqual(in_progress, Result1),
     ?_assertEqual({error, already_running}, Result2)].

single_node_no_upload(_) ->
    Opts = enable_cbcollect_mock(collect),
    Result = collect_logs_manager:begin_collection( [node()], Opts),
    WaitResult = wait_for_status(idle, 1000),
    Status = collect_logs_manager:get_status(),
    [?_assertEqual(in_progress, Result),
     ?_assertEqual({ok, idle}, WaitResult),
     ?_assertEqual(idle, proplists:get_value(status, Status)),
     ?_assertMatch([[{node_name, _},
                     {result,   <<"collected">>},
                     {artifact, <<"/tmp/node-01.zip">>}]],
                   proplists:get_value(nodes, Status))].

single_node_upload(_) ->
    Opts = enable_cbcollect_mock()
           ++ [{host,     <<"https://host.domain">>},
               {customer, <<"Example_inc.">>},
               {ticket,   <<"12345">>}],
    Result = collect_logs_manager:begin_collection( [node()], Opts),
    WaitResult = wait_for_status(idle, 1000),
    Status = collect_logs_manager:get_status(),
    ExpectedPrefix = "https://host.domain/Example_inc./12345/",
    ExpectedSuffix = atom_to_list(node()) ++ ".zip",
    NodeDetails = proplists:get_value(nodes, Status),
    Artifact = bitstring_to_list(proplists:get_value(artifact,
                                                     lists:nth(1, NodeDetails))),
    [?_assertEqual(in_progress, Result),
     ?_assertEqual({ok, idle}, WaitResult),
     ?_assertEqual(idle, proplists:get_value(status, Status)),
     ?_assertMatch([[{node_name, _}, {result, <<"uploaded">>}, {artifact, _}]],
                   NodeDetails),
     % Check the artifact has the correct prefix and suffix (we ignore the
     % timestamp in the middle).
     ?_assert(string:equal(ExpectedPrefix,
                           string:substr(Artifact, 1, string:len(ExpectedPrefix)))),
     ?_assert(string:equal(lists:reverse(ExpectedSuffix),
                           string:substr(lists:reverse(Artifact), 1,
                                         string:len(ExpectedSuffix))))].

single_node_upload_no_ticket(_) ->
    Opts = enable_cbcollect_mock()
        ++ [{host,     <<"https://host.domain">>},
            {customer, <<"Example_inc.">>}],
    Result = collect_logs_manager:begin_collection( [node()], Opts),
    WaitResult = wait_for_status(idle, 1000),
    Status = collect_logs_manager:get_status(),
    ExpectedPrefix = "https://host.domain/Example_inc./",
    ExpectedSuffix = atom_to_list(node()) ++ ".zip",
    NodeDetails = proplists:get_value(nodes, Status),
    Artifact = bitstring_to_list(proplists:get_value(artifact,
                                 lists:nth(1, NodeDetails))),
    [?_assertEqual(in_progress, Result),
     ?_assertEqual({ok, idle}, WaitResult),
     ?_assertMatch([[{node_name, _}, {result, <<"uploaded">>}, {artifact, _}]],
                   NodeDetails),
     % Check the artifact has the correct prefix and suffix (we ignore the
     % timestamp in the middle).
        ?_assert(string:equal(ExpectedPrefix,
                 string:substr(Artifact, 1, string:len(ExpectedPrefix)))),
        ?_assert(string:equal(lists:reverse(ExpectedSuffix),
                 string:substr(lists:reverse(Artifact), 1,
                               string:len(ExpectedSuffix))))].

cbcollect_upload(_) ->
    Opts = enable_cbcollect_mock()
        ++ [{extra_args, ["--mock_output=status: uploaded\nartifact: http://host.domain/directory/node-01.zip",
                          "--mock_result=0"]}],
    Result = collect_logs_manager:begin_collection( [node()], Opts),
    WaitResult = wait_for_status(idle, 1000),
    Status = collect_logs_manager:get_status(),
    [?_assertEqual(in_progress, Result),
     ?_assertEqual({ok, idle}, WaitResult),
     ?_assertMatch([[{node_name, _},
                     {result,   <<"uploaded">>},
                     {artifact, <<"http://host.domain/directory/node-01.zip">>}]],
                   proplists:get_value(nodes, Status))].

cbcollect_bad_output(_) ->
    Opts = enable_cbcollect_mock()
           ++ [{extra_args, ["--mock_output=random string with a : in the middle"]}],
    Result = collect_logs_manager:begin_collection( [node()], Opts),
    WaitResult = wait_for_status(idle, 1000),
    Status = collect_logs_manager:get_status(),
    [?_assertEqual(in_progress, Result),
     ?_assertEqual({ok, idle}, WaitResult),
     ?_assertMatch([[{node_name, _},
                     {result,  <<"failed">>},
                     {details, <<"cbcollect_info exited without outputting a status. Exit value 0">>}]],
                   proplists:get_value(nodes, Status))].

cbcollect_bad_result(_) ->
    Opts = enable_cbcollect_mock() ++ [{extra_args, ["--mock_result=1"]}],
    Result = collect_logs_manager:begin_collection( [node()], Opts),
    WaitResult = wait_for_status(idle, 1000),
    Status = collect_logs_manager:get_status(),
    [?_assertEqual(in_progress, Result),
     ?_assertEqual({ok, idle}, WaitResult),
     ?_assertMatch([[{node_name, _},
                     {result,  <<"failed">>},
                     {details, <<"cbcollect_info exited without outputting a status. Exit value 1">>}]],
                   proplists:get_value(nodes, Status))].

cbcollect_badargs(_) ->
    Opts = enable_cbcollect_mock()
           ++ [{extra_args, ["--mock_output=status: badargs\ndetails: Customer not specified",
                             "--mock_result=1"]}],
    Result = collect_logs_manager:begin_collection( [node()], Opts),
    WaitResult = wait_for_status(idle, 1000),
    Status = collect_logs_manager:get_status(),
    [?_assertEqual(in_progress, Result),
     ?_assertEqual({ok, idle}, WaitResult),
     ?_assertMatch([[{node_name, _},
                     {result,  <<"failed">>},
                     {details, <<"Customer not specified">>}]],
                   proplists:get_value(nodes, Status))].

cbcollect_collect_failed(_) ->
    Opts = enable_cbcollect_mock()
           ++ [{extra_args, ["--mock_output=status: collect-failed\ndetails: Random error",
                             "--mock_result=1"]}],
    Result = collect_logs_manager:begin_collection( [node()], Opts),
    WaitResult = wait_for_status(idle, 1000),
    Status = collect_logs_manager:get_status(),
    [?_assertEqual(in_progress, Result),
     ?_assertEqual({ok, idle}, WaitResult),
     ?_assertMatch([[{node_name, _},
                     {result,  <<"collect-failed">>},
                     {details, <<"Random error">>}]],
                   proplists:get_value(nodes, Status))].

cbcollect_upload_failed(_) ->
    Opts = enable_cbcollect_mock()
           ++ [{extra_args, ["--mock_output=status: upload-failed\ndetails: No route to host",
                             "--mock_result=1"]}],
    Result = collect_logs_manager:begin_collection( [node()], Opts),
    WaitResult = wait_for_status(idle, 1000),
    Status = collect_logs_manager:get_status(),
    [?_assertEqual(in_progress, Result),
     ?_assertEqual({ok, idle}, WaitResult),
     ?_assertMatch([[{node_name, _},
                     {result,  <<"upload-failed">>},
                     {details, <<"No route to host">>}]],
                   proplists:get_value(nodes, Status))].

cbcollect_cancelled(_) ->
    Opts = enable_cbcollect_mock()
           ++ [{extra_args, ["--mock_output=status: cancelled\ndetails: Cancelled by user",
                             "--mock_result=1"]}],
    Result = collect_logs_manager:begin_collection( [node()], Opts),
    WaitResult = wait_for_status(idle, 1000),
    Status = collect_logs_manager:get_status(),
    [?_assertEqual(in_progress, Result),
     ?_assertEqual({ok, idle}, WaitResult),
     ?_assertMatch([[{node_name, _},
                     {result,  <<"cancelled">>},
                     {details, <<"Cancelled by user">>}]],
                   proplists:get_value(nodes, Status))].

multi_begin_cancel(Slaves) ->
    Opts = enable_cbcollect_mock(collect),
    Nodes = [node() | Slaves],
    Result1 = collect_logs_manager:begin_collection( Nodes, Opts),
    WaitResult = wait_for_status(in_progress, 1000),
    collect_logs_manager:cancel_collection(),
    WaitResult2 = wait_for_status(idle, 1000),
    [?_assertEqual(in_progress, Result1),
     ?_assertEqual({ok, in_progress}, WaitResult),
     ?_assertEqual({ok, idle}, WaitResult2)].

multi_collect(Slaves) ->
    Opts = enable_cbcollect_mock(collect),
    Nodes = [node() | Slaves],
    Result1 = collect_logs_manager:begin_collection( Nodes, Opts),
    WaitResult = wait_for_status(idle, 1000),
    Status = collect_logs_manager:get_status(),
    [?_assertEqual(in_progress, Result1),
     ?_assertEqual({ok, idle}, WaitResult),
     ?_assertMatch([[{node_name,_}, {result,<<"collected">>}, {artifact,_}],
                    [{node_name,_}, {result,<<"collected">>}, {artifact,_}],
                    [{node_name,_}, {result,<<"collected">>}, {artifact,_}]],
                   proplists:get_value(nodes, Status))].

multi_collect_node_down(Slaves) ->
    Opts = enable_cbcollect_mock(collect),
    [Head | _Tail] = Slaves,
    ok = slave:stop(Head),
    Nodes = [node() | Slaves],
    Result1 = collect_logs_manager:begin_collection( Nodes, Opts),
    WaitResult = wait_for_status(idle, 1000),
    Status = collect_logs_manager:get_status(),
    [?_assertEqual(in_progress, Result1),
     ?_assertEqual({ok, idle}, WaitResult),
     ?_assertMatch([[{node_name,_}, {result,<<"collected">>}, {artifact,_}],
                    [{node_name,_}, {result,<<"collected">>}, {artifact,_}],
                    [{node_name, _}, {result, <<"failed">>}, {details, _}]],
                   proplists:get_value(nodes, Status))].

multi_double_begin(Slaves) ->
    Opts = enable_cbcollect_mock(collect),
    Nodes = [node() | Slaves],
    [Head | _Tail] = Slaves,
    % Start from first (remote) node
    Result1 = rpc:call(Head, collect_logs_manager, begin_collection,
                       [Nodes, Opts]),

    % Attempt start from second node which should fail.
    Result2 = collect_logs_manager:begin_collection(Nodes, Opts),

    % Cleanup
    ok = rpc:call(Head, collect_logs_manager, cancel_collection, []),

    [?_assertEqual(in_progress, Result1),
     ?_assertEqual({error, already_running}, Result2)].


%% Test helper functions ===================================================


%% Turn this node into a distributed one if it is not already distributed.
make_distrib(NodeName, NodeType) ->
    case node() of
        'nonode@nohost' ->
            case net_kernel:start([NodeName, NodeType]) of
                {ok, _Pid} -> node()
            end;
        CurrNode -> CurrNode
    end.

%% Start a slave node (suitable for running cbcollect_node on).
start_slave_node(NodeName) ->
    PathArgs = proplists:lookup_all(pa, init:get_arguments()),
    PathVals = lists:append([ V || {_K, V} <- PathArgs]),
    SlaveArgs = "-pa" ++ lists:foldl(fun(X, Acc) -> Acc ++ " " ++ X end,
                                     "", PathVals),
    case slave:start_link(erlang:list_to_atom(net_adm:localhost()),
                          NodeName,
                          SlaveArgs) of
        {ok, SlaveName} ->
            ok = rpc:call(SlaveName, t, fake_loggers, []),
            {ok, _Pid} = rpc:call(SlaveName, collect_logs_manager, start, []),
            {ok, SlaveName};
        {error, Reason} ->
            {error, Reason}
    end.

%% Returns options to enable the mock cbcollect_info program.
enable_cbcollect_mock(collect) ->
    enable_cbcollect_mock() ++
    [{extra_args, ["--mock_output=status: collected\nartifact: /tmp/node-01.zip",
                   "--mock_result=0"]}].

%% Returns options to run using a mock cbcollect_info.
enable_cbcollect_mock() ->
    {ok, Cwd} = file:get_cwd(),
    [{bin_dir, filename:join(Cwd, "test")},
     {cbcollect_name, "cbcollect_info_mock"}].

%% Waits for the status of collect_logs_manager to become the specified Status;
%% until a maxumum of Timeout milliseconds. Returns {ok, Status} if the
%% specified Status is found, else timeout.
wait_for_status(Status, Timeout) when Timeout > 10 ->
    case proplists:get_value(status, get_status()) of
        Status ->
            {ok, Status};
        _      ->
            timer:sleep(10),
            Timeout1 = Timeout - 10,
            wait_for_status(Status, Timeout1)
    end;
wait_for_status(_Status, Timeout) when Timeout =< 10 ->
    timeout.

-endif.
