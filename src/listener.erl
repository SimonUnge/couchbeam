-module (listener).

-include ("couchbeam.hrl").

-record (document, {db, doc, doc_id, current_step, job_length, job_step_do}).

-export ([start/3, start/1, work_manager/1, worker/0]).

start(Host, Port, Name) ->
    application:start(sasl),
    application:start(ibrowse),
    application:start(crypto),
    application:start(couchbeam),
    Server = couchbeam:server_connection(Host, Port),
    {ok, Db} = couchbeam:open_or_create_db(Server, Name),
    {ok, ReqId} = couchbeam:changes_wait(Db, self(), [{heartbeat, "1000"}]),
    print("StartRef ~p", [ReqId]),
    WorkManagerPid = start_workmanager(),
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
start_workmanager() ->
    process_flag(trap_exit, true),
    WorkManagerPid = spawn_link(?MODULE, work_manager, [5]),
    register(workmanager, WorkManagerPid),
    WorkManagerPid.
    


%%Starts a continuous changes stream, and sends the change notification to a work manager.
get_changes(ReqId, Db, WorkManagerPid) ->
    receive
        {'EXIT', WorkManagerPid, Reson} ->
            print("~p died, and here is why: ~p",[WorkManagerPid, Reson]),
            NewPid= start_workmanager(),
            get_changes(ReqId, Db, NewPid);
        {ReqId, done} ->
            ok;
        {ReqId, {change, Change}} ->
            workmanager ! {changes, Change, Db},
            get_changes(ReqId, Db, WorkManagerPid);
        {ReqId, {error, E}}->
            print("error ? ~p", [E])
    end.

%%Work manager. Should be its own process, it is now?
work_manager(N) ->
    Workers = start_workers(N, []),
    print("Length workers ~p", [length(Workers)]),
    work_manager_loop(Workers).

work_manager_loop(Workers) ->
    receive
        {status, WorkerPid, Doc_Info, Status} ->
            print("Status from worker ~p: ~p",[WorkerPid, Status]),
            NewWorkers = lists:keyreplace(WorkerPid, 1, Workers, {WorkerPid, free}),
            UpdatedDoc = increment_step(Doc_Info#document.doc),
            couchbeam:save_doc(Doc_Info#document.db, UpdatedDoc),
            work_manager_loop(NewWorkers);
        {changes, Change, Db} ->
            DocId = get_id(Change),
            case couchbeam:open_doc(Db, DocId) of
            	{ok, Doc} ->
            		CurrentStepNumber = get_field("step", Doc),
            		print("The current step: ~p", [CurrentStepNumber]),
            		JobStepList = get_field("job", Doc),
                    DocInfo = #document{db = Db,
                                              doc = Doc,
                                              doc_id = DocId,
                                              current_step = CurrentStepNumber,
                                              job_length = length(JobStepList)
                                              },
                    case (DocInfo#document.job_length<DocInfo#document.current_step + 1) of %Bryt ut, gör snyggt.
                        false ->
                            CurrentJobStep = get_current_job_step(JobStepList, CurrentStepNumber),
                            JobStepDo = get_do(CurrentJobStep),
                            print("The do: ~p", [JobStepDo]),
                            UpdatedDocInfo = DocInfo#document{job_step_do = JobStepDo},
                            NewWorkers = case get_free_worker(Workers) of
                                            {WorkerPid, free} ->
                                                print("The worker pid: ~p",[WorkerPid]),
                                                WorkerPid ! {work, self(), UpdatedDocInfo},
                                                lists:keyreplace(WorkerPid, 1, Workers, {WorkerPid, occupied});
                                            false -> 
                                                print("No free workers, what to do?"),
                                                Workers
                                        end,
                            work_manager_loop(NewWorkers);
                        true -> 
                            print("Job done"),
                            work_manager_loop(Workers)
                    end;
            	{error, not_found} ->
            		print("Doc deleted?..."),
                    work_manager_loop(Workers)
            end
    end.

start_workers(N, Workers) when N>0 ->
    process_flag(trap_exit, true),
    WorkerPid = spawn_link(?MODULE, worker, []),
    start_workers(N - 1, [{WorkerPid, free} | Workers]);
start_workers(0, Workers) ->
    Workers.

%%Worker
worker() ->
    receive
        {work, From, Doc_Info} ->
            P = open_port({spawn, Doc_Info#document.job_step_do}, [exit_status]),
            Status = get_status(P),
            From ! {status, self(), Doc_Info, Status},
            worker()
    end.

get_status(P) ->
    receive
        {P, {exit_status, Status}} ->
            Status;
        {P, Any} ->
            io:format("This is what I got: ~p ~n", [Any]),
            get_status(P)
    end.
	
%%===== Getters =====

get_id(Change) ->
    {Doc} = Change,
    {<<"id">>, ID} = lists:keyfind(<<"id">>, 1, Doc),
    binary_to_list(ID).

%Make more generall? Merge with get_id?
get_do(CurrentJobStep) ->
    {Step} = CurrentJobStep,
    {<<"do">>, DO} = lists:keyfind(<<"do">>, 1, Step),
    binary_to_list(DO).

get_target(CurrentJobStep) ->
    {Step} = CurrentJobStep,
    {<<"target">>, Target} = lists:keyfind(<<"target">>, 1, Step),
    binary_to_list(Target).  

get_field(Field, Doc) ->
    couchbeam_doc:get_value(list_to_binary(Field), Doc).

get_free_worker(Workers) ->
    lists:keyfind(free, 2, Workers).

get_current_job_step(JobStepList, StepNum) ->
    lists:nth(StepNum + 1, JobStepList).

%%==== SETTERS ====

increment_step(Doc) ->
    CurrentStepNumber = get_field("step", Doc),
    couchbeam_doc:set_value(<<"step">>, CurrentStepNumber + 1, Doc).

%%== just to have a nicer fuckning print. Hates io:format
print(String) ->
    print(String,[]).
print(String, Argument_List) ->
    io:format(String ++ "~n", Argument_List).
