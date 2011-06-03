-module (keepdocsalive).
-export ([start/1]).
-include("couchbeam.hrl").

start(Db) ->
  {_,_,DbId,_} = Db,
  {ok, ViewObj} = couchbeam:view(Db, {"jobdoc", "unclaimed"}, [{key, list_to_atom(DbId)}]),
  keep_docs_alive(ViewObj, Db).


keep_docs_alive(ViewObj, Db) ->
  timer:sleep(5000),
  io:format("I am keep alive ~p, have just sleept 5 sec.~n",[self()]),
  UnclaimedList = get_unclaimed_list(ViewObj),
  re_save_docs(UnclaimedList, Db),
  keep_docs_alive(ViewObj, Db).

get_unclaimed_list(ViewObj) ->
  {ok, {ViewList}} = couchbeam_view:fetch(ViewObj),
  {<<"rows">>, UnclaimedList} = lists:keyfind(<<"rows">>, 1, ViewList),
  UnclaimedList.

re_save_docs([], _Db) ->
  ok;  
re_save_docs([{H} | T], Db) ->
  {<<"id">>, DocId} = lists:keyfind(<<"id">>, 1, H),
  {ok, Doc} = couchbeam:open_doc(Db, binary_to_list(DocId)),
  io:format("The Doc: ~p~n", [Doc]),
  couchbeam:save_doc(Db, Doc),
  io:format("Okej, now I will resave the document ~p ~n", [DocId]),
  re_save_docs(T, Db).
  