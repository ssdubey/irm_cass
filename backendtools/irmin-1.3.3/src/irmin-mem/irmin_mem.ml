(*
Assumption:
There is already a table with <choose a name> name in <choose a name> keyspace*)

open Lwt.Infix
open Lwt 
let src = Logs.Src.create "irmin.mem" ~doc:"Irmin in-memory store"
module Log = (val Logs.src_log src : Logs.LOG)

type cassSession
type cassStatement
type cassCluster
type uuid
type cassFuture  
type cassError
type cassUuid
type cassUuidGen
type stubtype
type cassTuple
type cassResult
type cassRow
type cassValue
type cassIterator 

external ml_cass_session_new : unit -> cassSession = "cass_session_new"
external ml_cass_cluster_new : unit -> cassCluster = "cass_cluster_new"
external ml_cass_cluster_set_contact_points : cassCluster -> string -> unit = 
                              "cass_cluster_set_contact_points"
external ml_cass_session_connect : cassSession -> cassCluster -> cassFuture =
                              "cass_session_connect"                             
external ml_cass_future_wait : cassFuture -> unit = "cass_future_wait"
external ml_cass_future_error_code : cassFuture -> cassError = "cass_future_error_code"
external ml_cass_cluster_free : cassCluster -> unit = "cass_cluster_free"
external ml_cass_session_free : cassSession -> unit = "cass_session_free"
external ml_cass_statement_new : string -> int -> cassStatement = "cass_statement_new"
external ml_cass_session_execute : cassSession -> cassStatement -> cassFuture = "cass_session_execute"
external ml_cass_future_free : cassFuture -> unit = "cass_future_free"
external ml_cass_statement_free : cassStatement -> unit = "cass_statement_free"
external ml_cass_uuid_gen_new : unit -> cassUuidGen = "cass_uuid_gen_new"
external ml_cass_uuid_gen_free : cassUuidGen -> unit = "cass_uuid_gen_free"
external ml_cass_statement_new : string -> int -> cassStatement = "cass_statement_new"
external ml_cass_statement_bind_string : cassStatement -> int -> string -> unit = 
                              "cass_statement_bind_string"
external ml_cass_future_get_result : cassFuture -> cassResult = "cass_future_get_result"
external ml_cass_result_first_row : cassResult -> cassRow = "cass_result_first_row"
external ml_cass_row_get_column : cassRow -> int -> cassValue = "cass_row_get_column"
external ml_cass_iterator_from_result : cassResult -> cassIterator = "cass_iterator_from_result"
external ml_cass_result_row_count : cassResult -> int = "cass_result_row_count"
external ml_cass_iterator_next : cassIterator -> bool = "cass_iterator_next"
external ml_cass_value_get_string : cassValue -> string -> int -> unit ="cass_value_get_string"

external cstub_get_string : cassValue -> string = "get_string"
external cstub_convert : int -> int = "convert"
external cstub_match_enum : cassError -> cassFuture -> bool = "match_enum"
external cstub_convert_to_bool : bool -> bool = "convert_to_bool"

let config () = Irmin.Private.Conf.empty

let create_cluster hosts = 
  
  let cluster = ml_cass_cluster_new () in 
    ml_cass_cluster_set_contact_points cluster hosts;
    cluster

let connect_session sess cluster = 
  let future = ml_cass_session_connect sess cluster in
  ml_cass_future_wait future ;
  let rc = ml_cass_future_error_code future in 
  let response = cstub_match_enum rc future in 
    response

let execute_query sess query =
  let v = cstub_convert 0 in 
  let statement = ml_cass_statement_new query v in
  let future = ml_cass_session_execute sess statement in
    ml_cass_future_wait future;
    
    let rc = ml_cass_future_error_code future in 
    let response = cstub_match_enum rc future in
      ml_cass_future_free future;
      ml_cass_statement_free statement;

    response

let cx_stmt session query keyStr valStr =
  let valCount = cstub_convert 2 in

    let statement = ml_cass_statement_new query valCount in 
      ml_cass_statement_bind_string statement (cstub_convert 0) keyStr;  (*key and value are converted into string*)
      ml_cass_statement_bind_string statement (cstub_convert 1) valStr;

    let future = ml_cass_session_execute session statement in
      ml_cass_future_wait future;

    let rc = ml_cass_future_error_code future in 
    let response = cstub_match_enum rc future in 

      ml_cass_future_free future;
      ml_cass_statement_free statement;


module RO (K: Irmin.Contents.Conv) (V: Irmin.Contents.Conv) = struct

  type key = K.t
  type value = V.t
  type t = cassSession 

  let item = ref ""

  let v _config = 
    let sess = ml_cass_session_new () in 
      let hosts = "127.0.0.1" in 
      let cluster = create_cluster hosts in   
      let response = connect_session sess cluster in 
      match response with 
      | false -> (print_string "\nSession connection failed\n";
                  ml_cass_cluster_free cluster;
                  ml_cass_session_free sess;
              
              Lwt.return sess)
      | true -> print_string "\nSession is connected\n";

              Lwt.return sess

  let find t key = 
    let keycs = Irmin.Type.encode_cstruct K.t key in 
    let keyStr = Cstruct.to_string keycs in 
    let keyStr = String.sub keyStr 8 ((String.length keyStr) - 8) in

    (*since we know that our table has only two columns and we need only 2nd one, hence the query*)
    (* let query = "select value from " ^ t.table ^ "where key = '" ^ keyStr ^ "'" in *)
    let query = "select value from employee.empmap where key = '" ^ keyStr ^ "'" in
    let statement = ml_cass_statement_new query (cstub_convert 0) in
    let future = ml_cass_session_execute t statement in 
      ml_cass_future_wait future;

    let rc = ml_cass_future_error_code future in 
    let response = cstub_match_enum rc future in 

    if response = true then (
      let result = ml_cass_future_get_result future in
      let row = ml_cass_result_first_row result in 
      let value = ml_cass_row_get_column row (cstub_convert 0) in 

      let valStr = cstub_get_string value in 

        ml_cass_future_free future;
        ml_cass_statement_free statement;

        let cstruct = Cstruct.of_string valStr in
                
        match (Irmin.Type.decode_cstruct V.t cstruct) with
        |Ok s -> Lwt.return_some s
        |_ -> Lwt.return_none 
      
    )else(
      
      ml_cass_future_free future;
      ml_cass_statement_free statement;

      Lwt.return_none
  )
    
  let mem t key = Lwt.return true
    
end

module AO (K: Irmin.Hash.S) (V: Irmin.Contents.Conv) = struct

  include RO(K)(V)

  let add t value =
    
    let key = K.digest V.t value in 

    let keycs = Irmin.Type.encode_cstruct K.t key in 
    let keyStr = Cstruct.to_string keycs in 
    let keyStr = String.sub keyStr 8 ((String.length keyStr) - 8) in
    print_string ("this is key in string " ^ keyStr ^ "\n");
    print_int (String.length keyStr);

    let valcs = Irmin.Type.encode_cstruct V.t value in 
    let valStr = Cstruct.to_string valcs in 
    let valStr = String.sub valStr 8 ((String.length valStr) - 8) in
 
    let query = "INSERT INTO employee.office (key, value) VALUES (?, ?)" in

      cx_stmt t query keyStr valStr;

    Lwt.return key

end

module RW (K: Irmin.Contents.Conv) (V: Irmin.Contents.Conv) = struct

  module RO = RO(K)(V)
  module W = Irmin.Private.Watch.Make(K)(V)
  module L = Irmin.Private.Lock.Make(K)

  type t = { t: cassSession; w: W.t; lock: L.t; table: string }
  type key = RO.key
  type value = RO.value
  type watch = W.watch

  let watches = W.v ()
  let lock = L.v ()
  (* let tab =  *)

  let v config =
    RO.v config >>= fun t ->
    Lwt.return { t; w = watches; lock ; table = "empmap"}

  let find t = RO.find t.t
  let mem t = RO.mem t.t
  let watch_key t = W.watch_key t.w
  let watch t = W.watch t.w
  let unwatch t = W.unwatch t.w

  let list t =
    Log.debug (fun f -> f "list");
    (* RO.KMap.fold (fun k _ acc -> k :: acc) t.t.RO.t *) []
    |> Lwt.return

  let set t key value =
    
    let keycs = Irmin.Type.encode_cstruct K.t key in 
    let keyStr = Cstruct.to_string keycs in 
    let keyStr = String.sub keyStr 8 ((String.length keyStr) - 8) in
    
    let valcs = Irmin.Type.encode_cstruct V.t value in 
    let valStr = Cstruct.to_string valcs in 
    let valStr = String.sub valStr 8 ((String.length valStr) - 8) in
      
    let query = "INSERT INTO employee.empmap (key, value) VALUES (?, ?)" in

      cx_stmt t.t query keyStr valStr;

    Lwt.return_unit 

  let remove t key =
    Log.debug (fun f -> f "remove");
    L.with_lock t.lock key (fun () ->
        (* t.t.RO.t <- RO.KMap.remove key t.t.RO.t ; *)
        Lwt.return_unit
      ) >>= fun () ->
    W.notify t.w key None

  let test_and_set t key ~test ~set =
    Log.debug (fun f -> f "test_and_set");

    (*since test has some extra length compared to what find returns*)
    let subtest = match test with 
            | Some key -> (
                    let cstruct = Irmin.Type.encode_cstruct V.t key in
                    let keyStr = Cstruct.to_string cstruct in 
                    let keyStr = String.sub keyStr 8 ((String.length keyStr) - 8) in 
                    
                    let keyCstruct = Cstruct.of_string keyStr in
                    let subkey = Irmin.Type.decode_cstruct V.t keyCstruct in
                    subkey
                  )
            | None -> ( (*Hoping that this will never run, so putting dummy string *)
                    let keyCstruct = Cstruct.of_string "keyStr" in
                    let subkey = Irmin.Type.decode_cstruct V.t keyCstruct in
                    subkey)
          in

    (match subtest with
        | Ok s -> Lwt.return_some s
        | _ -> Lwt.return_none )  >>= fun test ->

    find t key >>= fun v ->
          
      if Irmin.Type.(equal (option V.t)) test v then (
        print_string "\ninside true\n";
        Lwt.return true
      )else(
        print_string "\ninside false\n";
        Lwt.return false)

end

