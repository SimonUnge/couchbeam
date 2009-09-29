#!/usr/bin/env escript
%% -*- erlang -*-
%%! -pa ./ebin

main(_) ->
    etap:plan(9),
    start_app(),
    case (catch test()) of
        ok ->
            stop_test(),
            etap:end_tests();
        Other ->
            stop_test(),
            etap:diag(io_lib:format("Test died abnormally: ~p", [Other])),
            etap:bail()
    end,
    ok.

start_app() ->
    couchbeam:start(),
    Pid = couchbeam_server:start_connection_link(),
    ok.
    
stop_test() ->
    catch couchbeam_server:delete_db(default, "couchbeam_testdb"),
    catch couchbeam_server:delete_db(default, "couchbeam_testdb2"),
    ok.
    
test() ->
    Db = couchbeam_server:create_db(default, "couchbeam_testdb"),
    etap:is(is_pid(Db), true, "db created ok"),
    F = fun() -> couchbeam_server:create_db(default, "couchbeam_testdb") end,
    etap:fun_is(F, precondition_failed, "conclict database ok"),
    Db2 = couchbeam_server:create_db(default, "couchbeam_testdb2"),
    etap:is(is_pid(Db2), true, "db2 created ok"),
    AllDbs = couchbeam_server:all_dbs(default),
    etap:ok(is_list(AllDbs), "all_dbs return a list"),
    etap:ok(lists:member(<<"couchbeam_testdb">>, AllDbs), "couchbeam_testdb exists ok "),
    etap:ok(couchbeam_db:is_db(default, "couchbeam_testdb"), "is_db exists ok "),
    etap:ok(lists:member(<<"couchbeam_testdb2">>, AllDbs), "couchbeam_testdb2 exists ok"),
    etap:is(couchbeam_server:delete_db(default, "couchbeam_testdb2"), ok, "delete couchbeam_testdb2 ok"),
    AllDbs1 = couchbeam_server:all_dbs(default),
    etap:not_ok(lists:member(<<"couchbeam_testdb2">>, AllDbs1), "couchbeam_testdb2 don't exists ok"),
    ok.