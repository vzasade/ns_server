%% @author Northscale <info@northscale.com>
%% @copyright 2009 NorthScale, Inc.
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
%% @doc Web server for menelaus.

-module(menelaus_util).
-author('Northscale <info@northscale.com>').

-include_lib("eunit/include/eunit.hrl").

-include("ns_common.hrl").
-include("menelaus_web.hrl").

-ifdef(EUNIT).
-export([test_under_debugger/0, debugger_apply/2]).
-endif.

-export([redirect_permanently/2,
         respond/2,
         reply/2,
         reply/3,
         reply_ok/3,
         reply_ok/4,
         reply_text/3,
         reply_text/4,
         reply_json/2,
         reply_json/3,
         reply_json/4,
         parse_json/1,
         reply_not_found/1,
         serve_file/3,
         serve_file/4,
         serve_static_file/4,
         parse_boolean/1,
         expect_config/1,
         get_option/2,
         local_addr/1,
         remote_addr_and_port/1,
         concat_url_path/1,
         concat_url_path/2,
         bin_concat_path/1,
         bin_concat_path/2,
         parse_validate_number/3,
         parse_validate_number/4,
         parse_validate_port_number/1,
         validate_email_address/1,
         insecure_pipe_through_command/2,
         encode_json/1,
         is_valid_positive_integer/1,
         is_valid_positive_integer_in_range/3,
         validate_boolean/2,
         validate_dir/2,
         validate_writable_dir/2,
         validate_integer/2,
         validate_range/4,
         validate_range/5,
         validate_unsupported_params/1,
         validate_has_params/1,
         execute_if_validated/3]).

%% used by parse_validate_number
-export([list_to_integer/1, list_to_float/1]).

-export([java_date/0,
         string_hash/1,
         my_seed/1]).

-export([stateful_map/3,
         stateful_takewhile/3,
         low_pass_filter/2]).

%% External API

server_headers() ->
    [{"Cache-Control", "no-cache"},
     {"Pragma", "no-cache"},
     {"Server", "Couchbase Server"}].

%% mostly extracted from mochiweb_request:maybe_redirect/3
redirect_permanently(Path, Req) ->
    Scheme = case Req:get(socket) of
                 {ssl, _} ->
                     "https://";
                 _ ->
                     "http://"
             end,
    Location =
        case Req:get_header_value("host") of
            undefined -> Path;
            X -> Scheme ++ X ++ Path
        end,
    LocationBin = list_to_binary(Location),
    Top = <<"<!DOCTYPE HTML PUBLIC \"-//IETF//DTD HTML 2.0//EN\">"
           "<html><head>"
           "<title>301 Moved Permanently</title>"
           "</head><body>"
           "<h1>Moved Permanently</h1>"
           "<p>The document has moved <a href=\"">>,
    Bottom = <<">here</a>.</p></body></html>\n">>,
    Body = <<Top/binary, LocationBin/binary, Bottom/binary>>,
    reply_inner(Req, Body, 301, [{"Location", Location}, {"Content-Type", "text/html"}]).

reply_not_found(Req) ->
    reply_not_found(Req, []).

reply_not_found(Req, ExtraHeaders) ->
    reply_inner(Req, "Requested resource not found.\r\n", 404, [{"Content-Type", "text/plain"} | ExtraHeaders]).

reply_text(Req, Message, Code) ->
    reply_inner(Req, Message, Code, [{"Content-Type", "text/plain"}]).

reply_text(Req, Message, Code, ExtraHeaders) ->
    reply_inner(Req, Message, Code, [{"Content-Type", "text/plain"} | ExtraHeaders]).

reply_json(Req, Body) ->
    reply_ok(Req, "application/json", encode_json(Body)).

reply_json(Req, Body, Code) ->
    reply_inner(Req, encode_json(Body), Code, [{"Content-Type", "application/json"}]).

reply_json(Req, Body, Code, ExtraHeaders) ->
    reply_inner(Req, encode_json(Body), Code, [{"Content-Type", "application/json"} | ExtraHeaders]).

reply_ok(Req, ContentType, Body) ->
    Peer = Req:get(peer),
    Resp = Req:ok({ContentType, server_headers(), Body}),
    ?log_web_hit(Peer, Req, Resp),
    Resp.

reply_ok(Req, ContentType, Body, ExtraHeaders) ->
    Peer = Req:get(peer),
    Resp = Req:ok({ContentType, extend_server_headers(ExtraHeaders), Body}),
    ?log_web_hit(Peer, Req, Resp),
    Resp.

reply(Req, Code, ExtraHeaders) ->
    reply_inner(Req, [], Code, ExtraHeaders).

reply(Req, Code) ->
    reply_inner(Req, [], Code).

reply_inner(Req, Body, Code, ExtraHeaders) ->
    respond(Req, {Code, extend_server_headers(ExtraHeaders), Body}).

reply_inner(Req, Body, Code) ->
    respond(Req, {Code, server_headers(), Body}).

respond(Req, RespTuple) ->
    Peer = Req:get(peer),
    Resp = Req:respond(RespTuple),
    ?log_web_hit(Peer, Req, Resp),
    Resp.

extend_server_headers(ExtraHeaders) ->
    misc:ukeymergewith(fun ({K, A}, {K, _}) -> {K, A} end, 1,
                       lists:keysort(1, ExtraHeaders), server_headers()).

-include_lib("kernel/include/file.hrl").

%% Originally from mochiweb_request.erl maybe_serve_file/2
%% and modified to handle user-defined content-type
serve_static_file(Req, {DocRoot, Path}, ContentType, ExtraHeaders) ->
    serve_static_file(Req, filename:join(DocRoot, Path), ContentType, ExtraHeaders);
serve_static_file(Req, File, ContentType, ExtraHeaders) ->
    case file:read_file_info(File) of
        {ok, FileInfo} ->
            LastModified = httpd_util:rfc1123_date(FileInfo#file_info.mtime),
            case Req:get_header_value("if-modified-since") of
                LastModified ->
                    reply(Req, 304, ExtraHeaders);
                _ ->
                    case file:open(File, [raw, binary]) of
                        {ok, IoDevice} ->
                            Res = reply_ok(Req, ContentType,
                                           {file, IoDevice},
                                           [{"last-modified", LastModified}
                                            | ExtraHeaders]),
                            file:close(IoDevice),
                            Res;
                        _ ->
                            reply_not_found(Req, ExtraHeaders)
                    end
            end;
        {error, _} ->
            reply_not_found(Req, ExtraHeaders)
    end.

serve_file(Req, File, Root) ->
    serve_file(Req, File, Root, []).

serve_file(Req, File, Root, ExtraHeaders) ->
    Peer = Req:get(peer),
    Resp = Req:serve_file(File, Root, ExtraHeaders),
    ?log_web_hit(Peer, Req, Resp),
    Resp.

expect_config(Key) ->
    {value, RV} = ns_config:search_node(Key),
    RV.

%% milliseconds since 1970 Jan 1 at UTC
java_date() ->
    {MegaSec, Sec, Micros} = erlang:now(),
    (MegaSec * 1000000 + Sec) * 1000 + (Micros div 1000).

string_hash(String) ->
    lists:foldl((fun (Val, Acc) -> (Acc * 31 + Val) band 16#0fffffff end),
                0,
                String).

my_seed(Number) ->
    {Number*31, Number*13, Number*113}.

%% applies F to every InList element and current state.
%% F must return pair of {new list element value, new current state}.
%% returns pair of {new list, current state}
full_stateful_map(F, InState, InList) ->
    {RV, State} = full_stateful_map_rec(F, InState, InList, []),
    {lists:reverse(RV), State}.

full_stateful_map_rec(_F, State, [], Acc) ->
    {Acc, State};
full_stateful_map_rec(F, State, [H|Tail], Acc) ->
    {Value, NewState} = F(H, State),
    full_stateful_map_rec(F, NewState, Tail, [Value|Acc]).

%% same as full_stateful_map/3, but discards state and returns only transformed list
stateful_map(F, InState, InList) ->
    element(1, full_stateful_map(F, InState, InList)).

low_pass_filter(Alpha, List) ->
    Beta = 1 - Alpha,
    F = fun (V, Prev) ->
                RV = Alpha*V + Beta*Prev,
                {RV, RV}
        end,
    case List of
        [] -> [];
        [H|Tail] -> [H | stateful_map(F, H, Tail)]
    end.

-ifdef(EUNIT).

string_hash_test() ->
    ?assertEqual(string_hash("hi"), $h*31+$i).

debugger_apply(Fun, Args) ->
    i:im(),
    {module, _} = i:ii(?MODULE),
    i:iaa([break]),
    ok = i:ib(?MODULE, Fun, length(Args)),
    apply(?MODULE, Fun, Args).

test_under_debugger() ->
    i:im(),
    {module, _} = i:ii(?MODULE),
    i:iaa([init]),
    eunit:test({spawn, {timeout, infinity, {module, ?MODULE}}}, [verbose]).

-endif.

get_option(Option, Options) ->
    {proplists:get_value(Option, Options),
     proplists:delete(Option, Options)}.

stateful_takewhile_rec(_F, [], _State, App) ->
    App;
stateful_takewhile_rec(F, [H|Tail], State, App) ->
    case F(H, State) of
        {true, NewState} ->
            stateful_takewhile_rec(F, Tail, NewState, [H|App]);
        _ -> App
    end.

stateful_takewhile(F, List, State) ->
    lists:reverse(stateful_takewhile_rec(F, List, State, [])).

parse_json(Req) ->
    mochijson2:decode(Req:recv_body()).

parse_boolean(Value) ->
    case Value of
        true -> true;
        false -> false;
        <<"true">> -> true;
        <<"false">> -> false;
        <<"1">> -> true;
        <<"0">> -> false;
        1 -> true;
        0 -> false
    end.

url_path_iolist(Path, Props) when is_binary(Path) ->
    do_url_path_iolist(Path, Props);
url_path_iolist(Segments, Props) ->
    Path = [[$/, mochiweb_util:quote_plus(S)] || S <- Segments],
    do_url_path_iolist(Path, Props).

do_url_path_iolist(Path, Props) ->
    case Props of
        [] ->
            Path;
        _ ->
            QS = mochiweb_util:urlencode(Props),
            [Path, $?, QS]
    end.

concat_url_path(Segments) ->
    concat_url_path(Segments, []).
concat_url_path(Segments, Props) ->
    lists:flatten(url_path_iolist(Segments, Props)).

bin_concat_path(Segments) ->
    bin_concat_path(Segments, []).
bin_concat_path(Segments, Props) ->
    iolist_to_binary(url_path_iolist(Segments, Props)).

-spec parse_validate_number(string(), (integer() | undefined), (integer() | undefined)) ->
                                   invalid | too_small | too_large | {ok, integer()}.
parse_validate_number(String, Min, Max) ->
    parse_validate_number(String, Min, Max, list_to_integer).

list_to_integer(A) -> erlang:list_to_integer(A).

list_to_float(A) -> try erlang:list_to_integer(A)
                    catch _:_ ->
                            erlang:list_to_float(A)
                    end.

-spec parse_validate_number(string(), (number() | undefined), (number() | undefined),
                            list_to_integer | list_to_float) ->
                                   invalid | too_small | too_large | {ok, integer()}.
parse_validate_number(String, Min, Max, Fun) ->
    Parsed = (catch menelaus_util:Fun(string:strip(String))),
    if
        is_number(Parsed) ->
            if
                Min =/= undefined andalso Parsed < Min -> too_small;
                Max =/= undefined andalso Max =/= infinity andalso
                  Parsed > Max -> too_large;
                true -> {ok, Parsed}
            end;
       true -> invalid
    end.

parse_validate_port_number(StringPort) ->
    case parse_validate_number(StringPort, 1024, 65535) of
        {ok, Port} ->
            Port;
        invalid ->
            throw({error, [<<"Port must be a number.">>]});
        _ ->
            throw({error, [<<"The port number must be greater than 1023 and less than 65536.">>]})
    end.

%% does a simple email address validation
validate_email_address(Address) ->
    {ok, RE} = re:compile("^[^@]+@.+$", [multiline]), %%" "hm, even erlang-mode is buggy :("),
    RV = re:run(Address, RE),
    case RV of
        {match, _} -> true;
        _ -> false
    end.

%% Extract the local address of the socket used for the request
local_addr(Req) ->
    Socket = Req:get(socket),
    Address = case Socket of
                  {ssl, SSLSock} ->
                      {ok, {AV, _Port}} = ssl:sockname(SSLSock),
                      AV;
                  _ ->
                      {ok, {AV, _Port}} = inet:sockname(Socket),
                      AV
              end,
    string:join(lists:map(fun integer_to_list/1, tuple_to_list(Address)), ".").

remote_addr_and_port(Req) ->
    case inet:peername(Req:get(socket)) of
        {ok, {Address, Port}} ->
            string:join(lists:map(fun integer_to_list/1, tuple_to_list(Address)), ".") ++ ":" ++ integer_to_list(Port);
        Error ->
            ?log_error("remote_addr failed: ~p", Error),
            "unknown"
    end.

pipe_through_command_rec(Port, Acc) ->
    receive
        {Port, {data, Data}} ->
            pipe_through_command_rec(Port, [Data | Acc]);
        {Port, {exit_status, _}} ->
            lists:reverse(Acc);
        X when is_tuple(X) andalso element(1, X) =:= Port ->
            io:format("ignoring port message: ~p~n", [X]),
            pipe_through_command_rec(Port, Acc)
    end.

%% this is NOT secure, because I cannot make erlang ports work as
%% popen. We're missing ability to close write side of the port.
insecure_pipe_through_command(Command, IOList) ->
    TmpFile = filename:join(path_config:component_path(tmp),
                            "pipethrough." ++ integer_to_list(erlang:phash2([self(), os:getpid(), timestamp]))),
    filelib:ensure_dir(TmpFile),
    misc:write_file(TmpFile, IOList),
    Port = open_port({spawn, Command ++ " <" ++ mochiweb_util:shell_quote(TmpFile)}, [binary, in, exit_status]),
    RV = pipe_through_command_rec(Port, []),
    file:delete(TmpFile),
    RV.

strip_json_struct({struct, Pairs}) -> {strip_json_struct(Pairs)};
strip_json_struct(List) when is_list(List) -> [strip_json_struct(E) || E <- List];
strip_json_struct({Key, Value}) -> {Key, strip_json_struct(Value)};
strip_json_struct(Other) -> Other.

encode_json(JSON) ->
    Stripped = try strip_json_struct(JSON)
               catch T1:E1 ->
                       ?log_debug("errored while stripping:~n~p", [JSON]),
                       Stack1 = erlang:get_stacktrace(),
                       erlang:raise(T1, E1, Stack1)
               end,

    try
        ejson:encode(Stripped)
    catch T:E ->
            ?log_debug("errored while sending:~n~p~n->~n~p", [JSON, Stripped]),
            Stack = erlang:get_stacktrace(),
            erlang:raise(T, E, Stack)
    end.

is_valid_positive_integer(String) ->
    Int = (catch erlang:list_to_integer(String)),
    (is_integer(Int) andalso (Int > 0)).

is_valid_positive_integer_in_range(String, Min, Max) ->
    Int = (catch erlang:list_to_integer(String)),
    (is_integer(Int) andalso (Int >= Min) andalso (Int =< Max)).

return_value(Name, Value, {OutList, InList, Errors}) ->
    {lists:keydelete(atom_to_list(Name), 1, OutList), [{Name, Value} | InList], Errors}.

return_error(Name, Error, {OutList, InList, Errors}) ->
    {lists:keydelete(atom_to_list(Name), 1, OutList), InList,
     [{Name, iolist_to_binary(Error)} | Errors]}.

validate_boolean(Name, {OutList, _, _} = State) ->
    Value = proplists:get_value(atom_to_list(Name), OutList),
    case Value of
        "true" ->
            return_value(Name, true, State);
        "false" ->
            return_value(Name, false, State);
        undefined ->
            State;
        _ ->
            return_error(Name, io_lib:format("The value of ~p must be true or false",
                                             [Name]), State)
    end.

validate_dir(Name, {OutList, _, _} = State) ->
    Value = proplists:get_value(atom_to_list(Name), OutList),
    case Value of
        undefined ->
            State;
        _ ->
            case filelib:is_dir(Value) of
                true ->
                    return_value(Name, Value, State);
                false ->
                    return_error(Name, io_lib:format("The value of ~p must be a valid directory",
                                                     [Name]), State)
            end
    end.

validate_writable_dir(Name, {_, InList, _} = State) ->
    Value = proplists:get_value(Name, InList),
    case Value of
        undefined ->
            State;
        _ ->
            case misc:is_dir_writable(Value) of
                true ->
                    State;
                false ->
                    return_error(Name, io_lib:format("The directory ~p must be writable",
                                                     [Name]), State)
            end
    end.

validate_integer(Name, {OutList, _, _} = State) ->
    Value = proplists:get_value(atom_to_list(Name), OutList),
    case Value of
        undefined ->
            State;
        _ ->
            Int = (catch erlang:list_to_integer(Value)),
            case is_integer(Int) of
                true ->
                    return_value(Name, Int, State);
                false ->
                    return_error(Name, io_lib:format("The value of ~p must be integer",
                                                     [Name]), State)
            end
    end.

validate_range(Name, Min, Max, State) ->
    ErrorFun = fun (NameArg, MinArg, MaxArg) ->
                       io_lib:format("The value of ~p must be in range from ~p to ~p",
                                     [NameArg, MinArg, MaxArg])
               end,
    validate_range(Name, Min, Max, ErrorFun, State).

validate_range(Name, Min, Max, ErrorFun, {_, InList, _} = State) ->
    Value = proplists:get_value(Name, InList),
    case Value of
        undefined ->
            State;
        _ ->
            case (Value >= Min) andalso (Value =< Max) of
                true ->
                    State;
                false ->
                    return_error(Name, ErrorFun(Name, Min, Max), State)
            end
    end.

validate_unsupported_params({OutList, InList, Errors}) ->
    NewErrors = [{list_to_binary(Key),
                  iolist_to_binary(["Found unsupported key ", Key])} || {Key, _} <- OutList] ++ Errors,
    {OutList, InList, NewErrors}.

validate_has_params({[], InList, Errors}) ->
    {[], InList, [{<<"_">>, <<"Request should have form parameters">>} | Errors]};
validate_has_params(State) ->
    State.

execute_if_validated(Fun, Req, {_, Values, Errors}) ->
    ValidateOnly = proplists:get_value("just_validate", Req:parse_qs()) =:= "1",
    case {ValidateOnly, Errors} of
        {true, _} ->
            reply_json(Req, {struct, [{errors, {struct, Errors}}]}, 200);
        {false, []} ->
            Fun(Values);
        {false, _} ->
            reply_json(Req, {struct, [{errors, {struct, Errors}}]}, 400)
    end.
