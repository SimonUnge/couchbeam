-module (listener).

-include ("couchbeam.hrl").

-export ([start/3, start/1]).

start(Host, Port, Name) ->
  application:start(sasl),
  application:start(ibrowse),
  application:start(crypto),
  application:start(couchbeam),
  Server = couchbeam:server_connection(Host, Port),
  {ok, Db} = couchbeam:open_or_create_db(Server, Name),
  {ok, ReqId} = couchbeam:changes_wait(Db, self(), [{heartbeat, "1000"}]),
  print("StartRef ~p", [ReqId]),
  WorkManagerPid = start_workmanager(Db), %I do not use workmanagerpid, do I?
  get_changes(ReqId, Db, WorkManagerPid).

%%Just for testing purposes.
start(Database) when is_atom(Database)->
  case Database of
    reg_a ->
      start("localhost", 5002, "reg_a");
    reg_b ->
      start("localhost", 5003, "reg_b");
    global_node ->
      start("localhost", 5001, "global_node")
  end.

%% Process starters?
start_workmanager(Db) ->
  %process_flag(trap_exit, true), XXX
  WorkManagerPid = spawn_link(workmanager, work_manager, [5, Db]),
  WorkManagerPid.

%%Starts a continuous changes stream, and sends the change notification to a work manager.
get_changes(ReqId, Db, WorkManagerPid) ->
  receive
    {'EXIT', WorkManagerPid, Reson} ->
      print("~p died, and here is why: ~p",[WorkManagerPid, Reson]),
      NewPid = start_workmanager(Db),
      get_changes(ReqId, Db, NewPid);
    {ReqId, done} ->
      print("listener done?"),
      ok;
    {ReqId, {change, Change}} ->
      print("Listener got change."),
      WorkManagerPid ! {changes, Change, Db},
      get_changes(ReqId, Db, WorkManagerPid);
    {ReqId, {error, E}}->
      print("error ? ~p", [E])
  end.

%%== just to have a nicer fuckning print. Hates io:format
print(String) ->
  print(String,[]).
print(String, Argument_List) ->
  io:format(String ++ "~n", Argument_List).
