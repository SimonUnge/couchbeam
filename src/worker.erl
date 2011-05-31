-module (worker).
-export ([worker/0, do_work/2]).
-include("couchbeam.hrl").


worker() ->
  receive
    {work, From, DocInfo} ->
      Retries = DocInfo#document.retry_strategy,
      print("Retryis looks like this:~p",[Retries]),
      {ExecTime, Status} = timer:tc(worker, do_work, [DocInfo, Retries]),
      From ! {status, self(), DocInfo, {ExecTime, Status}},
      worker()
  end.

do_work(DocInfo, Retries) ->
  P = open_port({spawn, DocInfo#document.job_step_do}, [exit_status]),
  case get_status(P) of
    {exit_status, Status} when Status =:= 0 ->
      Status;%%Alternativ step
    {exit_status, _Status} when Retries > 0 ->
      print("work failed, retry"),
      do_work(DocInfo, Retries - 1);
    {exit_status, Status} ->
      Status
  end. 

get_status(P) ->
  receive
    {P, {exit_status, Status}} ->
      {exit_status, Status};
    {P, Any} ->
      print("This is what I got: ~p", [Any]),
      get_status(P)
  end.

%%== just to have a nicer, quicker, print. Hates io:format
print(String) ->
  print(String,[]).
print(String, Argument_List) ->
  io:format(String ++ "~n", Argument_List).