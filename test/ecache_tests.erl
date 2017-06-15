-module(ecache_tests).
-include_lib("eunit/include/eunit.hrl").

-export([tester/1, memoize_tester/1]).

-define(_E(A, B), ?_assertEqual(A, B)).

ecache_setup() ->
  % start cache server tc (test cache)
  % 6 MB cache
  % 5 minute TTL per entry (300 seconds)
  {ok, Pid} = ecache_server:start_link(tc, ecache_tests, tester, 6, 3000),
  Pid.

ecache_cleanup(Cache) ->
  exit(Cache, normal).

tester(Key) when is_binary(Key) orelse is_list(Key) ->
  erlang:md5(Key).

memoize_tester(Key) when is_binary(Key) orelse is_list(Key) ->
  erlang:crc32(Key).

count(Properties) ->
  proplists:get_value(datum_count, Properties).

ecache_test_() ->
  {setup,
    fun ecache_setup/0,
    fun ecache_cleanup/1,
    fun(_C) ->
      [
        ?_E(erlang:md5("bob"),  ecache:get(tc, "bob")),
        ?_E(erlang:md5("bob2"), ecache:get(tc, "bob2")),
        ?_E(ok,   ecache:dirty(tc, "bob2")),
        ?_E(ok,   ecache:dirty(tc, "bob2")),
        ?_E(["bob"],   ecache:rand_keys(tc, 400)),
        ?_E([erlang:md5("bob")],   ecache:rand(tc, 400)),
        ?_E(erlang:crc32("bob2"),
            ecache:memoize(tc, ?MODULE, memoize_tester, "bob2")),
        ?_E(ok, ecache:dirty_memoize(tc, ?MODULE, memoize_tester, "bob2")),
        ?_assertMatch(Size when Size >= 0 andalso Size < 1000, ecache:total_size(tc)),
        ?_E(1, count(ecache:stats(tc))),
        % now, sleep for 3.1 seconds and key "bob" will auto-expire
        ?_assertMatch(0, 
          fun() -> timer:sleep(3100), count(ecache:stats(tc)) end()),
        % Make a new entry for TTL update testing
        ?_E(erlang:md5("bob7"), ecache:get(tc, "bob7")),
        ?_E(1, count(ecache:stats(tc))),
        % Wait 2 seconds and read it to reset the TTL to 3 seconds
        ?_E(erlang:md5("bob7"), 
          fun() -> timer:sleep(2000), ecache:get(tc, "bob7") end()),
        % If the TTL doesn't reset, it would expire before the sleep returns:
        ?_E(1, fun() -> timer:sleep(2100), count(ecache:stats(tc)) end()),
        % Now wait another 1.1 sec for a total wait of 3.2s after the last reset
        ?_E(0, fun() -> timer:sleep(1100), count(ecache:stats(tc)) end()),
        ?_E(0, ecache:total_size(tc)),
        ?_E(ok, ecache:empty(tc)),
        ?_E(0, ecache:total_size(tc))
      ]
    end
  }.
  
