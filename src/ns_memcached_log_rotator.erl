-module(ns_memcached_log_rotator).

-behaviour(gen_server).

-include("ns_common.hrl").
-include_lib("kernel/include/file.hrl").

-export([start_link/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([doc/0]).

doc() ->
    {gen_server, ?MODULE,
     "periodically sweeps too old memcached.log.XXX-s"}.

-record(state, {dir, prefix}).

log_params() ->
    Conf = ns_config:get(),
    {ns_config:search_node_prop(Conf, memcached, log_generations),
     ns_config:search_node_prop(Conf, memcached, log_rotation_period)}.

start_link() ->
    Conf = ns_config:get(),
    Dir = ns_config:search_node_prop(Conf, memcached, log_path),
    Prefix = ns_config:search_node_prop(Conf, memcached, log_prefix),
    gen_server:start_link(?MODULE, [Dir, Prefix], []).

init([Dir, Prefix]) ->
    {_Generations, Period} = log_params(),
    ?log_info("Starting log rotator on ~p/~p* with an initial period of ~pms",
              [Dir, Prefix, Period]),
    self() ! timeout,
    {ok, #state{dir=Dir, prefix=Prefix}, Period}.

handle_call(Request, From, State) ->
    ?log_warning("Unexpected handle_call(~p, ~p, ~p)", [Request, From, State]),
    exit(invalid_message).

handle_cast(Msg, State) ->
    ?log_warning("Unexpected handle_cast(~p, ~p)", [Msg, State]),
    exit(invalid_message).

handle_info(timeout, State) ->
    {Generations, Period} = log_params(),
    N = remove(State#state.dir, State#state.prefix, Generations),
    if N > 0 ->
            ?log_info("Removed ~p ~p log files from ~p (retaining up to ~p)",
                      [N, State#state.prefix, State#state.dir, Generations]);
       true ->
            ok
    end,
    {noreply, State, Period}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Older versions of lists:nthtail crash on N where N > length of the
%% list.  The newer version returns an empty list.  As there are mixed
%% deployments, I copied the newer version implementation here.
nthtail(0, A) -> A;
nthtail(N, [_ | A]) -> nthtail(N-1, A);
nthtail(_, _) -> [].

%% Remove the oldest N files from the given directory that match the
%% given prefix.
-spec remove(string(), string(), pos_integer()) -> integer().
remove(Dir, Prefix, N) ->
    {ok, Names} = file:list_dir(Dir),

    AbsNames = [filename:join(Dir, Fn) || Fn <- Names,
                                          string:str(Fn, Prefix) == 1],

    TimeList = lists:map(fun(Fn) ->
                      {ok, Fi} = file:read_file_info(Fn),
                                 {Fn, Fi#file_info.mtime}
                         end, AbsNames),

    Sorted = lists:reverse(lists:keysort(2, TimeList)),

    Oldest = [Fn || {Fn, _} <- nthtail(N, Sorted)],

    lists:foldl(fun(Fn, I) -> ok = file:delete(Fn), I + 1 end, 0, Oldest).
