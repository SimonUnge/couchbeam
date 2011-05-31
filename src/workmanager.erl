-module (workmanager).
-export ([work_manager/1, setnth/3, save_doc/1]).
-include("couchbeam.hrl").

work_manager(N) ->
  Workers = start_workers(N, []),
  print("Length workers ~p", [length(Workers)]),
  work_manager_loop(Workers).

start_workers(N, Workers) when N>0 ->
  process_flag(trap_exit, true),
  WorkerPid = spawn_link(worker, worker, []),
  start_workers(N - 1, [{WorkerPid, free, null} | Workers]);
start_workers(0, Workers) ->
  Workers.

work_manager_loop(Workers) ->
  receive
    {status, WorkerPid, DocInfo, {ExecTime, Status}} ->
      print("Status from worker ~p: ~p, on doc: ~p",[WorkerPid, Status, DocInfo#document.doc_id]),
      UpdDocInfo1 = utils:set_step_finish_time(DocInfo),
      UpdDocInfo2 = utils:set_job_step_execution_time(UpdDocInfo1, ExecTime/1000000),
      UpdDocInfo3 = utils:set_job_step_status(UpdDocInfo2, "Finished"),
      NewWorkers = change_worker_status(WorkerPid, Workers, free, null),
      print("freed worker ~p, worker list is now: ~p",[WorkerPid, NewWorkers]),
      UpdatedDocInfo = increment_step(UpdDocInfo3),
      print("Saving complete job step for doc: ~p", [UpdatedDocInfo#document.doc_id]),
      save_doc(UpdatedDocInfo),
      work_manager_loop(NewWorkers);
    {changes, Change, Db} ->
      DocId = get_id(Change),
      print("WM got change for doc_id: ~p,~n Will try to open.",[DocId]),
      case couchbeam:open_doc(Db, DocId) of
        {ok, Doc} ->
          print("Doc was opened!"),
          CurrentStepNumber = get_field("step", Doc),
          print("The current step: ~p", [CurrentStepNumber]),
          %Här borde jag nästan läsa in allt.XXX och göra en funktion av det.
          DocInfo = init_docinfo(Db, Doc, DocId),
          print("Worker status list before handle job: ~p", [Workers]),
          UpdatedWorkers = handle_job(Workers, DocInfo),
          print("And the worker status list after handle job, before loop:~p", [UpdatedWorkers]),
          work_manager_loop(UpdatedWorkers);
        {error, not_found} ->
          print("Doc deleted?..."),
          work_manager_loop(Workers)
      end
  end.

init_docinfo(Db, Doc, DocId) ->
  JobStepList = get_field("job", Doc),
  #document{
    db = Db,
    doc = Doc,
    doc_id = DocId,
    current_step = get_field("step", Doc),
    job_step_list = JobStepList,
    job_length = length(JobStepList)
  }.
  

handle_job(Workers, DocInfo) ->
  case is_job_complete(DocInfo) of
    false -> %There are still steps that have not been executed.
      print("job is not complete, inspecting winner"),
      inspect_step_and_handle(Workers, DocInfo);
    true ->
      print("Job done"),
      release_worker(Workers, DocInfo)
  end.

inspect_step_and_handle(Workers, DocInfo) ->
  case is_step_executing(DocInfo) of
    true ->
      print("Job step is already running"),
      Workers;
    false ->
      print("job is not running, inspect winner and handle:"),
      inspect_winner_and_handle(Workers, DocInfo)
  end.

inspect_winner_and_handle(Workers, DocInfo) ->
  case has_winner(DocInfo) of
    true ->
      print("Job have a winner, handle has winner"),
      handle_has_winner(Workers, DocInfo);
    false ->
    %kolla om det är till alla,
      print("Job does not have a winner, inspect and claim"),
      inspect_claim_and_handle(Workers, DocInfo)
  end.

handle_has_winner(Workers, DocInfo) ->
  case is_winner(DocInfo) of
    true ->
      print("I am winner, handle is winner"),
      %nu ska det det finnas en worker reserverat för detta step, om det inte har krashat?
      %SKA Test att ge den en process. Farligt? XXX
      handle_is_winner(Workers, DocInfo);
    false ->
      %frigör eventuell bokad worker.
      print("I am not winner, release workers"),
      release_worker(Workers, DocInfo)
  end.

handle_is_winner(Workers, DocInfo) ->
  case have_reserved_worker(Workers, DocInfo) of
    true -> 
      print("I am winner, and have a reserved worker for this:
            doc,step,workerlist: ~p, ~p ~n ~p",[DocInfo#document.doc_id,
                                                DocInfo#document.current_step,
                                                Workers]),
      print("Giving job to reserved worker"),
      give_job_to_reserved_worker(Workers, DocInfo);
    false ->
      print("I am winner, but have no worker reserved. Removes claim and winner, resaves doc."),
      UpdatedDocInfo = remove_claim_and_winner(DocInfo),
      save_doc(UpdatedDocInfo),
      Workers
  end.

inspect_claim_and_handle(Workers, DocInfo) ->
  case is_claimed(DocInfo) of
    true ->
      print("Job is claimed, checkning if I am creator"),
      handle_is_claimed(Workers, DocInfo);
    false ->
      print("Step is not claimed, inspect target and handle..."),
      inspect_target_and_handle(Workers, DocInfo)
  end.

handle_is_claimed(Workers, DocInfo) ->
  case is_job_creator(DocInfo) of
    true ->
      print("I am the creator, setting winner, saves doc, returns workers"),
      UpdDocInfo = utils:set_step_winner(DocInfo),
      save_doc(UpdDocInfo),
      Workers;
    false ->
      print("I am not the creator, returns workers."),
      Workers
  end.

inspect_target_and_handle(Workers, DocInfo) ->
  case is_target_any(DocInfo) of
    true ->
      print("Any is target, handle any target"),
      handle_any_target(Workers, DocInfo);
    false ->
      print("there is a specific target, handle spcigic target"),
      handle_specific_target(Workers, DocInfo)
  end.

handle_any_target(Workers, DocInfo) ->
  case has_free_workers(Workers) of
    true ->
      print("there are free workers, setting claim"),
      UpdatedDocInfo = set_claim(DocInfo),
      UpdatedWorkers = book_worker(Workers, UpdatedDocInfo),
      print("And I have now booked a worker for job:~p ~n Workerlist:~p ",[UpdatedDocInfo#document.doc_id, UpdatedWorkers]),
      save_doc(UpdatedDocInfo),
      UpdatedWorkers;
    false ->
      %XXX Keeping the doc alive by saving it again. But should I increment some value, and then why?
      print("No free workers, resaves doc with 5 sec keep alive..."),
      create_keep_alive(DocInfo),
      Workers
  end.

handle_specific_target(Workers, DocInfo) ->
  case is_target_me(DocInfo) of
    true ->
      print("I am target, handle me target"),
      handle_me_target(Workers, DocInfo);
    false ->
      print("I am not target, doing nothing..."),
      Workers
  end.

handle_me_target(Workers, DocInfo) ->
  case has_free_workers(Workers) of
    true ->
      print("I am target (so gives work to worker) and has workers, see: ~p", [Workers]),
      give_job_to_worker(Workers, DocInfo);
    false ->
      print("I am target but have no workers, resaves doc with 5 sec keep alive"),
      %XXX Keeping the doc alive by saving it again, in 5 seconds.
      create_keep_alive(DocInfo),
      Workers
  end.
%%==== Worker Helpers =====
%%XXX ej testad?
give_job_to_worker(Workers, DocInfo) ->
  CurrentJobStep = get_current_job_step(DocInfo),
  JobStepDo = get_do(CurrentJobStep),
  RetryStrategy = get_retry_strategy(CurrentJobStep),
  {WorkerPid, free, _DocId} = get_free_worker(Workers), 
  UpdDocInfo1 = DocInfo#document{job_step_do = JobStepDo,
                                 retry_strategy = RetryStrategy},%XXX EGEN FUNKTION, SOM NEEEDAN
  UpdDocInfo2 = utils:set_executioner(UpdDocInfo1, WorkerPid),
  UpdDocInfo3 = utils:set_step_start_time(UpdDocInfo2),%%Move to worker?
  UpdDocInfo4 = utils:set_job_step_status(UpdDocInfo3, "Working"),
  print("The worker pid: ~p",[WorkerPid]),
  UpdDocInfo5 = save_doc(UpdDocInfo4),
  WorkerPid ! {work, self(), UpdDocInfo5},
  change_worker_status(WorkerPid, Workers, busy, UpdDocInfo5#document.doc_id).

give_job_to_reserved_worker(Workers, DocInfo) ->
  Doc_Id = DocInfo#document.doc_id,
  print("WHY DO I ALWAYS CRASH HERE? With DOCID = ~p", [Doc_Id]),
  case get_reserved_worker(Workers, DocInfo) of
    {WorkerPid, booked, Doc_Id} ->
      CurrentJobStep = get_current_job_step(DocInfo),
      JobStepDo = get_do(CurrentJobStep),
      RetryStrategy = get_retry_strategy(CurrentJobStep),
      UpdDocInfo1 = DocInfo#document{job_step_do = JobStepDo,
                                     retry_strategy = RetryStrategy},%%%XXX EGEN FUNKTION
      UpdDocInfo2 = utils:set_executioner(UpdDocInfo1, WorkerPid),
      UpdDocInfo3 = utils:set_step_start_time(UpdDocInfo2),%%Move to worker?
      UpdDocInfo4 = utils:set_job_step_status(UpdDocInfo3, "Working"),
      UpdDocInfo5 = save_doc(UpdDocInfo4),
      WorkerPid ! {work, self(), UpdDocInfo5},
      change_worker_status(WorkerPid, Workers, busy, UpdDocInfo5#document.doc_id);
    {_WorkerPid, _Status, Doc_Id} ->
      print("for some reason, Im already doing this job..."),
      Workers
  end.
  
change_worker_status(WorkerPid, Workers, NewStatus, DocId) ->
  lists:keyreplace(WorkerPid, 1, Workers, {WorkerPid, NewStatus, DocId}).

book_worker(Workers, DocInfo) ->
  {WorkerPid, free, _DocId} = get_free_worker(Workers),
  print("booking worker ~p", [WorkerPid]),
  change_worker_status(WorkerPid, Workers, booked, DocInfo#document.doc_id).

release_worker(Workers, DocInfo) ->
  Doc_Id = DocInfo#document.doc_id,
  case get_reserved_worker(Workers, DocInfo) of
    {WorkerPid, booked, Doc_Id} ->
      print("Release worker: ~p", [WorkerPid]),
      change_worker_status(WorkerPid, Workers, free, null);
    false ->
      print("Ah, I did not book this job"),
      Workers
  end.

remove_claim_and_winner(DocInfo) ->
  print("removing my claim"),
  UpdatedDocInfo = update_job_step_list(DocInfo, "claimed_by", null),
  print("...and removing me as winner"),
  update_job_step_list(UpdatedDocInfo, "winner", null).

create_keep_alive(DocInfo) ->
  print("I am create keep alive. I will now spawn a process to save the document ~p after 5 sec:", [DocInfo#document.doc_id]),
  Pid = spawn(keepdocalive, keep_doc_alive, [DocInfo]),
  print("Spawned, with Pid: ~p", [Pid]).
%%=========================  

is_job_complete(DocInfo) ->
  DocInfo#document.job_length < DocInfo#document.current_step + 1.

%Returns true if the job-step is claimed.
is_claimed(DocInfo) ->
  ClaimStatus = get_claim_status(DocInfo),
  null =/= ClaimStatus.

is_step_executing(DocInfo) ->
  StepStatus = get_step_status(DocInfo),
  null =/= StepStatus.

has_winner(DocInfo) ->
  WinnerStatus = get_winner_status(DocInfo),
  WinnerStatus =/= null.

is_winner(DocInfo) ->
  WinnerStatus = get_winner_status(DocInfo),
  {_,_,DbId,_} = DocInfo#document.db,
  WinnerStatus =:= list_to_binary(DbId).

is_target_any(DocInfo) ->
  Target = get_target(DocInfo),
  Target =:= "any".

is_target_me(DocInfo) ->
  Target = get_target(DocInfo),
  {_,_,DbId,_} = DocInfo#document.db,
  Target =:= DbId.

is_job_creator(DocInfo) ->
  Creator = get_field("creator", DocInfo#document.doc),
  {_,_,DbId,_} = DocInfo#document.db,
  Creator =:= list_to_binary(DbId).

has_free_workers(Workers) ->
  case get_free_worker(Workers) of
    false ->
      false;
    _Worker ->
      true
  end.

have_reserved_worker(Workers, DocInfo) ->
  case get_reserved_worker(Workers, DocInfo) of
    false ->
      false;
    _Worker ->
      true
  end.

save_doc(DocInfo) ->
  case couchbeam:save_doc(DocInfo#document.db, DocInfo#document.doc) of
    {ok, NewDoc} ->
      print("Saving doc id:~p",[DocInfo#document.doc_id]),
      DocInfo#document{ doc = NewDoc };
    {error, conflict} ->
      print("Save conflict on doc:~p",[DocInfo#document.doc_id]),
      DocInfo
  end.
%%===== Getters =====

get_id(Change) ->
  {Doc} = Change,
  {<<"id">>, ID} = lists:keyfind(<<"id">>, 1, Doc),
  binary_to_list(ID).

%? Merge with get_id? XXX SE ÖVER DETTA, LIKNANDE TAR OLIKA ARGUMENT!!!
get_do(CurrentJobStep) ->
  {Step} = CurrentJobStep,
  {<<"do">>, DO} = lists:keyfind(<<"do">>, 1, Step),
  binary_to_list(DO).

get_target(DocInfo) ->
  {CurrentJobStep} = get_current_job_step(DocInfo),
  {<<"target">>, Target} = lists:keyfind(<<"target">>, 1, CurrentJobStep),
  binary_to_list(Target).

get_retry_strategy(CurrentJobStep) ->
  {Step} = CurrentJobStep,
  {<<"retry_strategy">>, {RetryStrategy}} = lists:keyfind(<<"retry_strategy">>, 1, Step),
  RetryStrategy.

get_field(Field, Doc) ->
  couchbeam_doc:get_value(list_to_binary(Field), Doc).

get_free_worker(Workers) ->
  lists:keyfind(free, 2, Workers).

get_reserved_worker(Workers, DocInfo) ->%XXX är jag dum i huvudet? jag plockar ju ut workers, oavsett status.
  lists:keyfind(DocInfo#document.doc_id, 3, Workers).

get_current_job_step(DocInfo) -> %XXX Change order of args?
  lists:nth(DocInfo#document.current_step + 1, DocInfo#document.job_step_list). %Erlang list index starts on 1.

get_claim_status(DocInfo) ->
  {CurrentJobStep} = get_current_job_step(DocInfo),
  {<<"claimed_by">>, ClaimStatus} = lists:keyfind(<<"claimed_by">>, 1, CurrentJobStep),
  ClaimStatus.

get_winner_status(DocInfo) ->
  {CurrentJobStep} = get_current_job_step(DocInfo),
  {<<"winner">>, WinnerStatus} = lists:keyfind(<<"winner">>, 1, CurrentJobStep),
  WinnerStatus.

get_step_status(DocInfo) ->
  {CurrentJobStep} = get_current_job_step(DocInfo),
  {<<"step_status">>, StepStatus} = lists:keyfind(<<"step_status">>, 1, CurrentJobStep),
  StepStatus.

%%==== SETTERS ====
set_claim(DocInfo) ->
  {_,_,DbId,_} = DocInfo#document.db,
  update_job_step_list(DocInfo, "claimed_by", list_to_binary(DbId)).

increment_step(DocInfo) ->
  DocInfo#document{doc = set_key_on_doc(DocInfo,
                                        "step",
                                        DocInfo#document.current_step + 1)
                  }.
set_key_on_doc(DocInfo, Key, Value) ->
  couchbeam_doc:set_value(list_to_binary(Key),
                           Value,
                           DocInfo#document.doc).

%%==== To get job list, change value of key, and return an updated job list, or DocInfo?
update_job_step_list(DocInfo, Key, Value) ->
  {CurrJobStep} = get_current_job_step(DocInfo),
  UpdatedCurrJobStep = lists:keyreplace(list_to_binary(Key),
                                        1,
                                        CurrJobStep,
                                        {list_to_binary(Key),
                                        Value}
                                       ),
  Updated_Job_List = setnth(DocInfo#document.current_step + 1, %THis I hate, + 1, XXX
                            DocInfo#document.job_step_list, 
                            {UpdatedCurrJobStep}
                           ),
  DocInfo#document{job_step_list = Updated_Job_List,
                   doc = set_key_on_doc(DocInfo, "job", Updated_Job_List)}.
  

%% @spec setnth(Index, List, Element) -> list()
%% @doc Replaces element on index Index with Element
setnth(1, [_|Rest], New) -> [New|Rest];
setnth(I, [E|Rest], New) -> [E|setnth(I-1, Rest, New)].

%%==== just to have a nicer fuckning print. Hates io:format ====
print(String) ->
  print(String,[]).
print(String, Argument_List) ->
  io:format(String ++ "~n", Argument_List).