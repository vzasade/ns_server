%% @author Couchbase <info@couchbase.com>
%% @copyright 2011 Couchbase, Inc.
%%
%% Licensed under the Apache License, Version 2.0 (the "License"); you may not
%% use this file except in compliance with the License. You may obtain a copy of
%% the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
%% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
%% License for the specific language governing permissions and limitations under
%% the License.

%% XDC Replicator Checkpoint Functions
-module(xdc_vbucket_rep_ckpt).

%% public functions
-export([start_timer/1, cancel_timer/1]).
-export([do_checkpoint/1]).
-export([read_validate_checkpoint/3]).
-export([get_local_vbuuid/2]).
-export([get_failover_uuid/2]).
-export([build_request_base/4]).

-include("xdc_replicator.hrl").

-define(HTTP_RETRIES, 5).

start_timer(#rep_state{rep_details=#rep{options=Options}} = State) ->
    AfterSecs = proplists:get_value(checkpoint_interval, Options),
    %% convert to milliseconds
    After = AfterSecs*1000,
    %% cancel old timer if exists
    cancel_timer(State),
    %% start a new timer
    case timer:apply_after(After, gen_server, cast, [self(), checkpoint]) of
        {ok, Ref} ->
            ?xdcr_trace("schedule next checkpoint in ~p seconds (ref: ~p)", [AfterSecs, Ref]),
            Ref;
        Error ->
            ?xdcr_error("Replicator, error scheduling checkpoint:  ~p", [Error]),
            nil
    end.

cancel_timer(#rep_state{timer = nil} = State) ->
    State;
cancel_timer(#rep_state{timer = Timer} = State) ->
    {ok, cancel} = timer:cancel(Timer),
    ?xdcr_trace("checkpoint timer has been cancelled (ref: ~p)", [Timer]),
    State#rep_state{timer = nil}.

bump_status_counter(OldStatus, State, Element) ->
    OldValue = element(Element, OldStatus),
    NewValue = OldValue + 1,
    NewStatus = setelement(Element, OldStatus, NewValue),
    State#rep_state{status = NewStatus}.

-spec do_checkpoint(#rep_state{}) -> {ok, binary(), #rep_state{}} |
                                     {checkpoint_commit_failure, binary(), #rep_state{}}.
do_checkpoint(State) ->
    case xdc_rep_utils:is_new_xdcr_path() of
        false ->
            do_checkpoint_old(State);
        _ ->
            do_checkpoint_new(State)
    end.

do_checkpoint_new(#rep_state{current_through_seq=Seq, committed_seq=Seq} = State) ->
    ?xdcr_debug("not checkpoint needed for vb: ~p", [State#rep_state.status#rep_vb_status.vb]),
    {ok, <<"no checkpoint">>, State};
do_checkpoint_new(#rep_state{remote_vbopaque = {old_node_marker, RemoteStartTime},
                         target = TargetDB} = State) ->
    ?xdcr_debug("Faking checkpoint into old node"),
    %% note: we're not bumping any counters or reporting anything to
    %% parent. I believe that's fine.
    %%
    %% We're faking actual checkpoint, but we must check remote
    %% instance_start_time to detect possible remote crash
    {ok, Props} = couch_api_wrap:get_db_info(TargetDB),
    NowStartTime = proplists:get_value(<<"instance_start_time">>, Props),
    case NowStartTime =/= undefined andalso NowStartTime =:= RemoteStartTime of
        true ->
            {ok, [], State};
        _ ->
            ?xdcr_debug("Detected remote ep-engine instance restart: ~s vs ~s", [RemoteStartTime, NowStartTime]),
            {checkpoint_commit_failure, {start_time_mismatch, RemoteStartTime, NowStartTime}, State}
    end;
do_checkpoint_new(#rep_state{current_through_seq = Seq,
                             current_through_snapshot_seq = SnapshotSeq,
                             current_through_snapshot_end_seq = SnapshotEndSeq,
                             status = OldStatus,
                             upr_failover_uuid = FailoverUUID} = State) ->
    #rep_vb_status{vb = Vb,
                   docs_checked = Checked,
                   docs_written = Written,
                   data_replicated = DataRepd,
                   total_docs_checked = TotalChecked,
                   total_docs_written = TotalWritten,
                   total_data_replicated = TotalDataRepd} = OldStatus,

    ?xdcr_info("checkpointing for vb: ~p at ~p",
               [Vb, {Seq, SnapshotSeq, SnapshotEndSeq, FailoverUUID}]),

    SourceBucketName = (State#rep_state.rep_details)#rep.source,

    %% NOTE: we don't need to check if source still has all the
    %% replicated stuff. upr failover id + seqnos already represent it
    %% well enough. And if any of that is lost, upr will automatically
    %% rollback to a place that's safe to restart replication from.

    CommitResult = (catch perform_commit_for_checkpoint(State#rep_state.remote_vbopaque,
                                                        State#rep_state.ckpt_api_request_base)),

    case CommitResult of
        {ok, RemoteCommitOpaque} ->
            CheckpointDocId = build_commit_doc_id(State#rep_state.rep_details, Vb),
            NewSeq = State#rep_state.current_through_seq,
            NewSnapshotSeq = State#rep_state.current_through_snapshot_seq,
            NewSnapshotEndSeq = State#rep_state.current_through_snapshot_end_seq,
            {seq_vs_snapshot, true} = {seq_vs_snapshot, (NewSnapshotSeq =< NewSeq)},
            CheckpointDoc = {[{<<"commitopaque">>, RemoteCommitOpaque},
                              {<<"start_time">>, ?l2b(State#rep_state.rep_starttime)},
                              {<<"end_time">>, ?l2b(httpd_util:rfc1123_date())},
                              {<<"failover_uuid">>, FailoverUUID},
                              {<<"seqno">>, NewSeq},
                              {<<"upr_snapshot_seqno">>, NewSnapshotSeq},
                              {<<"upr_snapshot_end_seqno">>, NewSnapshotEndSeq},
                              {<<"total_docs_checked">>, TotalChecked + Checked},
                              {<<"total_docs_written">>, TotalWritten + Written},
                              {<<"total_data_replicated">>, TotalDataRepd + DataRepd}]},
            DB = capi_utils:must_open_vbucket(SourceBucketName, <<"master">>),
            try
                ok = couch_db:update_doc(DB, #doc{id = CheckpointDocId,
                                                  body = CheckpointDoc})
            after
                couch_db:close(DB)
            end,
            NewState = State#rep_state{committed_seq = NewSeq,
                                       last_checkpoint_time = erlang:now()},
            update_checkpoint_status_to_parent(NewState, true),
            {ok, [], bump_status_counter(OldStatus, NewState, #rep_vb_status.num_checkpoints)};
        Other ->
            case Other of
                {mismatch, _} ->
                    ?xdcr_error("Checkpointing failed due to remote vbopaque mismatch: ~p", [Other]);
                _ ->
                    ?xdcr_error("Checkpointing failed unexpectedly (or could be network problem): ~p", [Other])
            end,
            update_checkpoint_status_to_parent(State, false),
            NewState = bump_status_counter(OldStatus, State, #rep_vb_status.num_failedckpts),
            {checkpoint_commit_failure, Other, NewState}
    end.

do_checkpoint_old(#rep_state{current_through_seq=Seq, committed_seq=Seq} = State) ->
    ?xdcr_debug("not checkpoint needed for vb: ~p", [State#rep_state.status#rep_vb_status.vb]),
    {ok, <<"no checkpoint">>, State};
do_checkpoint_old(#rep_state{remote_vbopaque = {old_node_marker, RemoteStartTime},
                         target = TargetDB} = State) ->
    ?xdcr_debug("Faking checkpoint into old node"),
    %% note: we're not bumping any counters or reporting anything to
    %% parent. I believe that's fine.
    %%
    %% We're faking actual checkpoint, but we must check remote
    %% instance_start_time to detect possible remote crash
    {ok, Props} = couch_api_wrap:get_db_info(TargetDB),
    NowStartTime = proplists:get_value(<<"instance_start_time">>, Props),
    case NowStartTime =/= undefined andalso NowStartTime =:= RemoteStartTime of
        true ->
            {ok, [], State};
        _ ->
            ?xdcr_debug("Detected remote ep-engine instance restart: ~s vs ~s", [RemoteStartTime, NowStartTime]),
            {checkpoint_commit_failure, {start_time_mismatch, RemoteStartTime, NowStartTime}, State}
    end;
do_checkpoint_old(State) ->
    ?xdcr_info("checkpointing for vb: ~p at ~p", [State#rep_state.status#rep_vb_status.vb, State#rep_state.current_through_seq]),
    #rep_vb_status{vb = Vb,
                   docs_checked = Checked,
                   docs_written = Written,
                   data_replicated = DataRepd,
                   total_docs_checked = TotalChecked,
                   total_docs_written = TotalWritten,
                   total_data_replicated = TotalDataRepd} = OldStatus = State#rep_state.status,

    SourceBucketName = (State#rep_state.rep_details)#rep.source,
    OldLocalVBUUID = State#rep_state.local_vbuuid,
    LocalVBUUID = get_local_vbuuid(SourceBucketName, Vb),

    CommitResult = case LocalVBUUID =:= OldLocalVBUUID of
                       true ->
                           catch perform_commit_for_checkpoint(State#rep_state.remote_vbopaque,
                                                               State#rep_state.ckpt_api_request_base);
                       false ->
                           {local_vbuuid_mismatch, LocalVBUUID, OldLocalVBUUID}
                   end,

    case CommitResult of
        {ok, RemoteCommitOpaque} ->


            CheckpointDocId = build_commit_doc_id(State#rep_state.rep_details, Vb),
            NewSeq = State#rep_state.current_through_seq,
            CheckpointDoc = {[{<<"commitopaque">>, RemoteCommitOpaque},
                              {<<"start_time">>, ?l2b(State#rep_state.rep_starttime)},
                              {<<"end_time">>, ?l2b(httpd_util:rfc1123_date())},
                              {<<"local_vbuuid">>, LocalVBUUID},
                              {<<"seqno">>, NewSeq},
                              {<<"total_docs_checked">>, TotalChecked + Checked},
                              {<<"total_docs_written">>, TotalWritten + Written},
                              {<<"total_data_replicated">>, TotalDataRepd + DataRepd}]},
            DB = capi_utils:must_open_vbucket(SourceBucketName, <<"master">>),
            try
                ok = couch_db:update_doc(DB, #doc{id = CheckpointDocId,
                                                  body = CheckpointDoc})
            after
                couch_db:close(DB)
            end,
            NewState = State#rep_state{committed_seq = NewSeq,
                                       last_checkpoint_time = erlang:now()},
            update_checkpoint_status_to_parent(NewState, true),
            {ok, [], bump_status_counter(OldStatus, NewState, #rep_vb_status.num_checkpoints)};
        Other ->
            case Other of
                {mismatch, _} ->
                    ?xdcr_error("Checkpointing failed due to remote vbopaque mismatch: ~p", [Other]);
                _ ->
                    ?xdcr_error("Checkpointing failed unexpectedly (or could be network problem): ~p", [Other])
            end,
            update_checkpoint_status_to_parent(State, false),
            NewState = bump_status_counter(OldStatus, State, #rep_vb_status.num_failedckpts),
            {checkpoint_commit_failure, Other, NewState}
    end.

%% update the checkpoint status to parent bucket replicator
update_checkpoint_status_to_parent(#rep_state{
                                      rep_details = RepDetails,
                                      parent = Parent,
                                      status = RepStatus}, Succ) ->

    VBucket = RepStatus#rep_vb_status.vb,
    RawTime = now(),
    LocalTime = calendar:now_to_local_time(RawTime),

    ?xdcr_debug("replicator (vb: ~p, source: ~p, dest: ~p) reports checkpoint "
                "status: {succ: ~p} to parent: ~p",
                [VBucket, RepDetails#rep.source, RepDetails#rep.target, Succ, Parent]),

    %% post to parent bucket replicator
    Parent ! {set_checkpoint_status, #rep_checkpoint_status{ts = RawTime,
                                                            time = LocalTime,
                                                            vb = VBucket,
                                                            succ = Succ}}.

build_request_base(HttpDB, Bucket, BucketUUID, VBucket) ->
    [Scheme, Host, _DbName] = string:tokens(HttpDB#httpdb.url, "/"),
    URL = Scheme ++ "//" ++ Host ++ "/",

    BodyBase = [{<<"vb">>, VBucket},
                {<<"bucket">>, Bucket},
                {<<"bucketUUID">>, BucketUUID}],
    {URL, BodyBase, HttpDB}.

send_post(Method, ExtraBody, {BaseURL, BodyBase, HttpDB}) ->
    URL = BaseURL ++ Method,
    Headers = [{"Content-Type", "application/json"}],
    BodyJSON = {BodyBase ++ ExtraBody},

    RV = send_retriable_http_request(URL, "POST", Headers, ejson:encode(BodyJSON),
                                     HttpDB#httpdb.timeout,
                                     HttpDB#httpdb.lhttpc_options),
    case RV of
        {ok, {{StatusCode, _ReasonPhrase}, _RespHeaders, RespBody}} ->
            case StatusCode of
                200 ->
                    {Props} = ejson:decode(RespBody),
                    {ok, Props};
                _ ->
                    {error, StatusCode, (catch ejson:decode(RespBody)), RespBody}
            end;
        {error, Reason} ->
            ?xdcr_error("Checkpointing related POST to ~s failed: ~p", [URL, Reason]),
            erlang:error({checkpoint_post_failed, Method, Reason})
    end.

send_retriable_http_request(URL, Method, Headers, Body, Timeout, HTTPOptions) ->
    do_send_retriable_http_request(URL, Method, Headers, Body, Timeout, HTTPOptions, ?HTTP_RETRIES).

do_send_retriable_http_request(URL, Method, Headers, Body, Timeout, HTTPOptions, Retries) ->
    RV = lhttpc:request(URL, Method, Headers, Body, Timeout, HTTPOptions),
    case RV of
        {ok, _} ->
            RV;
        {error, Reason} ->
            NewRetries = Retries - 1,
            case NewRetries < 0 of
                true ->
                    RV;
                _ ->
                    ?xdcr_debug("Got http error doing ~s to ~s. Will retry. Error: ~p", [Method, URL, Reason]),
                    do_send_retriable_http_request(URL, Method, Headers, Body, Timeout, HTTPOptions, NewRetries)
            end
    end.

build_commit_doc_id(Rep, Vb) ->
    CheckpointDocId0 = iolist_to_binary(couch_httpd:quote(iolist_to_binary([Rep#rep.id, $/, integer_to_list(Vb)]))),
    <<"_local/30-ck-", CheckpointDocId0/binary>>.


perform_commit_for_checkpoint(RemoteVBOpaque, ApiRequestBase) ->
    Body = case RemoteVBOpaque of
               undefined -> [];
               _ ->
                   [{<<"vbopaque">>, RemoteVBOpaque}]
           end,

    case send_post("_commit_for_checkpoint", Body, ApiRequestBase) of
        {ok, Props} ->
            case proplists:get_value(<<"commitopaque">>, Props) of
                undefined ->
                    erlang:error({missing_commitopaque_in_commit_for_checkpoint_response, Props});
                CommitOpaque ->
                    {ok, CommitOpaque}
            end;
        {error, 400 = _StatusCode, {JSON}, _} when is_list(JSON) ->
            {mismatch, extract_vbopaque(JSON)};
        {error, StatusCode, _, Body} ->
            {error, StatusCode, Body}
    end.

extract_vbopaque(Props) ->
    RemoteVBOpaque = proplists:get_value(<<"vbopaque">>, Props),
    case RemoteVBOpaque =:= undefined of
        true ->
            erlang:error({missing_vbopaque_in_pre_replicate_response, Props});
        _ -> ok
    end,
    RemoteVBOpaque.

perform_pre_replicate(RemoteCommitOpaque, {_, _, HttpDB} = ApiRequestBase) ->
    Body = case RemoteCommitOpaque of
               undefined -> [];
               _ ->
                   [{<<"commitopaque">>, RemoteCommitOpaque}]
           end,

    case send_post("_pre_replicate", Body , ApiRequestBase) of
        {ok, Props} ->
            {ok, extract_vbopaque(Props)};
        {error, 400 = _StatusCode, {JSON}, _} when is_list(JSON) ->
            ?xdcr_debug("_pre_replicate returned mismatch status: ~p", [JSON]),
            {mismatch, extract_vbopaque(JSON)};
        {error, 404, _, _} ->
            ?xdcr_debug("_pre_replicate returned 404. Assuming older node"),
            case couch_api_wrap:get_db_info(HttpDB) of
                {ok, Props} ->
                    {mismatch, {old_node_marker, proplists:get_value(<<"instance_start_time">>, Props)}};
                Error ->
                    ?xdcr_error("Failed to get dbinfo of remote node (~s): ~p",
                                [misc:sanitize_url(HttpDB#httpdb.url),
                                 Error]),
                    erlang:error({pre_replicate_failed, {get_db_info_failed, Error}})
            end;
        {error, StatusCode, _, Body} ->
            ?xdcr_error("_pre_replicate failed with unexpected status: ~B: ~s", [StatusCode, Body]),
            erlang:error({pre_replicate_failed, StatusCode})
    end.


read_validate_checkpoint(Rep, Vb, ApiRequestBase) ->
    DB = capi_utils:must_open_vbucket(Rep#rep.source, <<"master">>),
    DocId = build_commit_doc_id(Rep, Vb),
    case couch_db:open_doc_int(DB, DocId, [ejson_body]) of
        {ok, #doc{body = Body}} ->
            parse_validate_checkpoint_doc(Rep, Vb, Body, ApiRequestBase);
        {not_found, _} ->
            ?xdcr_debug("Found no local checkpoint document for vb: ~B. Will start from scratch", [Vb]),
            handle_no_checkpoint(ApiRequestBase)
    end.

handle_no_checkpoint(ApiRequestBase) ->
    {_, RemoteVBOpaque} = perform_pre_replicate(undefined, ApiRequestBase),
    handle_no_checkpoint_with_opaque(RemoteVBOpaque).

handle_no_checkpoint_with_opaque(RemoteVBOpaque) ->
    StartSeq = 0,
    TotalDocsChecked = 0,
    TotalDocsWritten = 0,
    TotalDataReplicated = 0,
    {StartSeq, 0, 0, 0,
     TotalDocsChecked,
     TotalDocsWritten,
     TotalDataReplicated,
     RemoteVBOpaque}.

parse_validate_checkpoint_doc(Rep, Vb, Body, ApiRequestBase) ->
    try
        do_parse_validate_checkpoint_doc(Rep, Vb, Body, ApiRequestBase)
    catch T:E ->
            S = erlang:get_stacktrace(),
            ?xdcr_debug("Got parse_validate_checkpoint_doc exception: ~p:~p~n~p", [T, E, S]),
            erlang:raise(T, E, S)
    end.

do_parse_validate_checkpoint_doc(Rep, Vb, Body0, ApiRequestBase) ->
    case xdc_rep_utils:is_new_xdcr_path() of
        false ->
            do_parse_validate_checkpoint_doc_old(Rep, Vb, Body0, ApiRequestBase);
        _ ->
            do_parse_validate_checkpoint_doc_new(Rep, Vb, Body0, ApiRequestBase)
    end.

do_parse_validate_checkpoint_doc_new(Rep, Vb, Body0, ApiRequestBase) ->
    Body = case Body0 of
               {XB} -> XB;
               _ -> []
           end,
    CommitOpaque = proplists:get_value(<<"commitopaque">>, Body),
    FailoverUUID = proplists:get_value(<<"failover_uuid">>, Body),
    Seqno = proplists:get_value(<<"seqno">>, Body),
    SnapshotSeq = proplists:get_value(<<"upr_snapshot_seqno">>, Body),
    SnapshotEndSeq = proplists:get_value(<<"upr_snapshot_end_seqno">>, Body),
    case (CommitOpaque =/= undefined andalso
          is_integer(FailoverUUID) andalso
          is_integer(Seqno) andalso
          is_integer(SnapshotSeq) andalso
          is_integer(SnapshotEndSeq)) of
        false ->
            handle_no_checkpoint(ApiRequestBase);
        true ->
            case get_failover_uuid(Rep#rep.source, Vb) =/= FailoverUUID of
                true ->
                    ?xdcr_debug("local checkpoint for vb ~B does not match due to local side. Checkpoint seqno: ~B. But will still let upr producer decide on true start seqno", [Vb, Seqno]),
                    ok;
                false ->
                    ok
            end,
            case perform_pre_replicate(CommitOpaque, ApiRequestBase) of
                {mismatch, RemoteVBOpaque} ->
                    ?xdcr_debug("local checkpoint for vb ~B does not match due to remote side. Checkpoint seqno: ~B. xdcr will start from scratch", [Vb, Seqno]),
                    handle_no_checkpoint_with_opaque(RemoteVBOpaque);
                {ok, RemoteVBOpaque} ->
                    ?xdcr_debug("local checkpoint for vb ~B matches. Seqno: ~B", [Vb, Seqno]),
                    StartSeq = Seqno,
                    TotalDocsChecked = proplists:get_value(<<"total_docs_checked">>, Body, 0),
                    TotalDocsWritten = proplists:get_value(<<"total_docs_written">>, Body, 0),
                    TotalDataReplicated = proplists:get_value(<<"total_data_replicated">>, Body, 0),
                    ?xdcr_debug("Checkpoint stats: ~p", [{TotalDocsChecked, TotalDocsWritten, TotalDataReplicated}]),
                    {StartSeq, SnapshotSeq, SnapshotEndSeq,
                     FailoverUUID,
                     TotalDocsChecked,
                     TotalDocsWritten,
                     TotalDataReplicated,
                     RemoteVBOpaque}
            end
    end.

do_parse_validate_checkpoint_doc_old(Rep, Vb, Body0, ApiRequestBase) ->
    Body = case Body0 of
               {XB} -> XB;
               _ -> []
           end,
    CommitOpaque = proplists:get_value(<<"commitopaque">>, Body),
    LocalVBUUID = proplists:get_value(<<"local_vbuuid">>, Body),
    Seqno = proplists:get_value(<<"seqno">>, Body),
    case (CommitOpaque =/= undefined andalso
          is_binary(LocalVBUUID) andalso
          is_integer(Seqno)) of
        false ->
            handle_no_checkpoint(ApiRequestBase);
        true ->
            case get_local_vbuuid(Rep#rep.source, Vb) =:= LocalVBUUID of
                false ->
                    ?xdcr_debug("local checkpoint for vb ~B does not match due to local side. Checkpoint seqno: ~B. xdcr will start from scratch", [Vb, Seqno]),
                    handle_no_checkpoint(ApiRequestBase);
                true ->
                    case perform_pre_replicate(CommitOpaque, ApiRequestBase) of
                        {mismatch, RemoteVBOpaque} ->
                            ?xdcr_debug("local checkpoint for vb ~B does not match due to remote side. Checkpoint seqno: ~B. xdcr will start from scratch", [Vb, Seqno]),
                            handle_no_checkpoint_with_opaque(RemoteVBOpaque);
                        {ok, RemoteVBOpaque} ->
                            ?xdcr_debug("local checkpoint for vb ~B matches. Seqno: ~B", [Vb, Seqno]),
                            StartSeq = Seqno,
                            TotalDocsChecked = proplists:get_value(<<"total_docs_checked">>, Body, 0),
                            TotalDocsWritten = proplists:get_value(<<"total_docs_written">>, Body, 0),
                            TotalDataReplicated = proplists:get_value(<<"total_data_replicated">>, Body, 0),
                            ?xdcr_debug("Checkpoint stats: ~p", [{TotalDocsChecked, TotalDocsWritten, TotalDataReplicated}]),
                            {StartSeq, 0, 0, 0,
                             TotalDocsChecked,
                             TotalDocsWritten,
                             TotalDataReplicated,
                             RemoteVBOpaque}
                    end
            end
    end.

get_local_vbuuid(BucketName, Vb) ->
    {ok, KV} = ns_memcached:stats(couch_util:to_list(BucketName), io_lib:format("vbucket-seqno ~B", [Vb])),
    Key = iolist_to_binary(io_lib:format("vb_~B:uuid", [Vb])),
    misc:expect_prop_value(Key, KV).

get_failover_uuid(BucketName, Vb) ->
    {U, _} = lists:last(xdcr_upr_streamer:get_failover_log(couch_util:to_list(BucketName), Vb)),
    U.
