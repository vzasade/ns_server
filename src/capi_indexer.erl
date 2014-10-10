%% @author Couchbase <info@couchbase.com>
%% @copyright 2013 Couchbase, Inc.
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
-module(capi_indexer).

-include("couch_db.hrl").
-include_lib("couch_index_merger/include/couch_index_merger.hrl").
-include_lib("couch_index_merger/include/couch_view_merger.hrl").
-include("ns_stats.hrl").                       % used for run_on_subset_according_to_stats/1

-export([do_handle_view_req/5, finalize_view_merge_params/2,
         vbucket_db_name/2]).

-import(couch_util, [
                     get_value/2,
                     get_value/3
                    ]).

-define(RETRY_INTERVAL, 5 * 1000).
-define(RETRY_ATTEMPTS, 20).


subset_design_doc_view(Mod, Req, BucketName, DesignName, ViewName,
                       [VBucket]) ->
    DDocId = <<"_design/", DesignName/binary>>,
    [Spec] = build_local_set_specs(BucketName, DDocId, ViewName, [VBucket]),
    Specs = [Spec#set_view_spec{category = dev}],
    MergeParams = finalize_view_merge_params(Req, Specs),
    set_active_partition(Mod, DDocId, BucketName, VBucket),
    query_index(Mod, MergeParams, Req).


full_design_doc_view(Mod, Req, DbName, DesignName, ViewName, VBucketsDict) ->
    DDocId = <<"_design/", DesignName/binary>>,
    design_doc_view_loop(Mod, Req, DbName, DDocId, ViewName, VBucketsDict,
                         ?RETRY_ATTEMPTS).

design_doc_view_loop(_Mod, _Req, _DbName, _DDocId, _ViewName, _, 0) ->
    throw({error, inconsistent_state});
design_doc_view_loop(Mod, Req, DbName, DDocId, ViewName, VBucketsDict,
                     Attempt) ->
    MergeParams = view_merge_params(
                    Mod, Req, DbName, DDocId, ViewName, VBucketsDict),
    try
        query_index(Mod, MergeParams, Req)
    catch
        throw:{error, set_view_outdated} ->
            ?views_debug("Got `set_view_outdated` error. Retrying."),
            timer:sleep(?RETRY_INTERVAL),
            NewVBucketsDict = vbucket_map_mirror:node_vbuckets_dict(?b2l(DbName)),
            design_doc_view_loop(Mod, Req, DbName, DDocId, ViewName,
                                 NewVBucketsDict, Attempt - 1)
    end.

%% @doc Returns a vBucket if it is run on a subset (single vBucket) only, else
%% it returns an atom called "full_set"
-spec run_on_subset(#httpd{}, binary()) ->  non_neg_integer()|full_set.
run_on_subset(#httpd{path_parts=[_, _, DName, _, _]}=Req, Name) ->
    case DName of
        <<"dev_", _/binary>> ->
            case get_value("full_set", (Req#httpd.mochi_req):parse_qs()) =/= "true"
                andalso run_on_subset_according_to_stats(Name) of
                true -> capi_frontend:first_vbucket(Name);
                false -> full_set;
                {error, no_stats} -> capi_frontend:first_vbucket(Name)
            end;
        _ ->
            full_set
    end.

-define(DEV_MULTIPLE, 20).

%% Decide whether to run a query on a subset of documents or a full cluster
%% depending on the number of items in the cluster
-spec run_on_subset_according_to_stats(binary()) -> true | false | {error, no_stats}.
run_on_subset_according_to_stats(Bucket) ->
    case catch stats_reader:latest(minute, node(), ?b2l(Bucket), 1) of
        {ok, [Stats|_]} ->
            {ok, Config} = ns_bucket:get_bucket(?b2l(Bucket)),
            NumVBuckets = proplists:get_value(num_vbuckets, Config, []),
            {ok, N} = orddict:find(curr_items_tot, Stats#stat_entry.values),
            N > NumVBuckets * ?DEV_MULTIPLE;
        _Error ->
            {error, no_stats}
    end.


do_handle_view_req(Mod, Req, DbName, DDocName, ViewName) ->
    VBucketsDict = vbucket_map_mirror:node_vbuckets_dict(binary_to_list(DbName)),
    case dict:find(ns_node_disco:ns_server_node(), VBucketsDict) of
        error ->
            capi_frontend:send_no_active_vbuckets(Req, DbName);
        _ ->
            case run_on_subset(Req, DbName) of
                full_set ->
                    full_design_doc_view(Mod, Req, DbName, DDocName, ViewName,
                                         VBucketsDict);
                VBucket ->
                    subset_design_doc_view(Mod, Req, DbName, DDocName,
                                           ViewName, [VBucket])
            end
    end.


view_merge_params(Mod, Req, BucketName, DDocId, ViewName, NodeToVBuckets) ->
    NSServerNode = ns_node_disco:ns_server_node(),
    ViewSpecs = dict:fold(
                  fun(Node, VBuckets, Acc) when Node =:= NSServerNode ->
                          build_local_set_specs(BucketName,
                                                DDocId, ViewName, VBuckets) ++ Acc;
                     (Node, VBuckets, Acc) ->
                          [build_remote_set_specs(Mod, Node, BucketName,
                                                  DDocId, ViewName, VBuckets) | Acc]
                  end, [], NodeToVBuckets),
    finalize_view_merge_params(Req, ViewSpecs).

finalize_view_merge_params(Req, ViewSpecs) ->
    case Req#httpd.method of
        'GET' ->
            Body = [],
            Keys = validate_keys_param(couch_httpd:qs_json_value(Req, "keys", nil));
        'POST' ->
            {Body} = couch_httpd:json_body_obj(Req),
            Keys = validate_keys_param(get_value(<<"keys">>, Body, nil))
    end,
    MergeParams0 = #index_merge{
      indexes = ViewSpecs,
      extra = #view_merge{
        keys = Keys
       },
      ddoc_revision = auto
     },
    couch_httpd_view_merger:apply_http_config(Req, Body, MergeParams0).


validate_keys_param(nil) ->
    nil;
validate_keys_param(Keys) when is_list(Keys) ->
    Keys;
validate_keys_param(_) ->
    throw({bad_request, "`keys` parameter is not an array."}).


vbucket_db_name(BucketName, VBucket) when is_binary(VBucket) ->
    iolist_to_binary([BucketName, $/, VBucket]);
vbucket_db_name(BucketName, VBucket) ->
    iolist_to_binary([BucketName, $/, integer_to_list(VBucket)]).

build_local_set_specs(BucketName, DDocId, ViewName, VBuckets) ->
    [#set_view_spec{
        name = BucketName,
        ddoc_id = DDocId,
        view_name = ViewName,
        partitions = VBuckets
       }].

build_remote_set_specs(Mod, Node, BucketName, DDocId, ViewName, VBuckets) ->
    DDocName = case DDocId of
                   <<"_design/", Rest/binary>> ->
                       Rest;
                   _ ->
                       DDocId
               end,
    FullViewName = iolist_to_binary(["_design/", couch_httpd:quote(DDocName),
                                     $/, couch_httpd:quote(ViewName)]),
    MergeHandler = case Mod of
                       mapreduce_view ->
                           <<"/_view_merge">>;
                       spatial_view ->
                           <<"/_spatial_merge">>
                   end,
    MergeURL = iolist_to_binary([vbucket_map_mirror:node_to_inner_capi_base_url(Node),
                                 MergeHandler]),

    Sets = {[
             {BucketName, {[{<<"view">>, FullViewName},
                            {<<"partitions">>, VBuckets}]}}
            ]},

    Props = {[
              {<<"views">>,
               {[{<<"sets">>, Sets}]}}
             ]},
    #merged_index_spec{url = MergeURL, ejson_spec = Props}.


-spec set_active_partition(mapreduce_view | spatial_view, binary(), binary(),
                           non_neg_integer()) -> ok.
set_active_partition(Mod, DDocId, BucketName, VBucket) ->
    try
        couch_set_view_dev:set_active_partition(
          Mod, BucketName, DDocId, VBucket)
    catch
        throw:{error, view_undefined} ->
            couch_set_view_dev:define_group(Mod, BucketName, DDocId, VBucket)
    end.



-spec query_index(mapreduce_view | spatial_view, #index_merge{}, #httpd{}) ->
                         ok.
query_index(mapreduce_view, MergeParams, Req) ->
    couch_index_merger:query_index(couch_view_merger, MergeParams, Req);
query_index(spatial_view, MergeParams, Req) ->
    couch_index_merger:query_index(spatial_merger, MergeParams, Req).
