%%%
%% Grow Beard Consistent Hashing

%% SHA1 hash
%% Consistenet Hashing

-module(gb_chash).

-define(MAX_SHA, 2.923003274661806e48). %% math:pow(2,160 +1) -1


-compile(export_all).

-define(DB1, 'db1@skelter').
-define(DB2, 'db2@skelter').
-define(DB3, 'db3@skelter').
-define(DB4, 'db4@skelter').
-define(DB5, 'db5@skelter').
-define(DB6, 'db6@skelter').

get_chash_ring() ->
    lists:sort([{chash(?DB1), ?DB1},
		{chash(?DB2), ?DB2},
		{chash(?DB3), ?DB3},
		{chash(?DB4), ?DB4},
		{chash(?DB5), ?DB5},
		{chash(?DB6), ?DB6}]).

get_db_node(Key) ->
    HashedKey = chash(Key),
    Ring = get_chash_ring(),
    find_near_hash(Ring, HashedKey, hd(Ring)).

find_near_hash([{H, Node}|_], HK, _) when HK < H ->
    Node;
find_near_hash([_|T],HK,First) ->
    find_near_hash(T,HK,First);
find_near_hash([], _, {_,Node}) ->
    Node.

chash(Key) when is_binary(Key) ->
    %% hash_algorithms() =  md5 | ripemd160 | sha | sha224 | sha256 | sha384 | sha512 
    crypto:hash(sha, Key);
chash(Key) ->
    chash(term_to_binary(Key)).


%% test
test() ->
    random:seed(now()),
    List = [bddbms_chash:get_db_node(random:uniform(10000000)) || 
		_ <- lists:seq(1,100000)],
    count_occurrences(List, []).

count_occurrences([H|T], Aux) ->
    case lists:keyfind(H, 1, Aux) of 
	false ->
	    count_occurrences(T, [{H, 1} |Aux]);
	{H, C} ->
	    count_occurrences(T, lists:keyreplace(H, 1, Aux, {H,C+1}))
    end;
count_occurrences([], Aux) ->
    Aux.
