(*
Assumption:
There is already a table with <choose a name> name in <choose a name> keyspace*)

open Lwt.Infix

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
external ml_cass_value_get_string : cassValue -> string -> int -> unit = "cass_value_get_string"
external ml_cass_iterator_get_row : cassIterator -> cassRow = "cass_iterator_get_row"
external ml_cass_row_get_column_by_name : cassRow -> string -> cassValue = "cass_row_get_column_by_name"
external ml_cass_iterator_from_tuple : cassValue -> cassIterator = "cass_iterator_from_tuple"
external ml_cass_iterator_get_value : cassIterator -> cassValue = "cass_iterator_get_value"
(* externa *)

external cstub_get_string : cassValue -> string = "get_string"
external cstub_convert : int -> int = "convert"
external cstub_match_enum : cassError -> cassFuture -> bool = "match_enum"
external cstub_convert_to_bool : bool -> bool = "convert_to_bool"
external cstub_convert_to_ml : int -> int = "convert_to_ml"

let config () = Irmin.Private.Conf.empty (*create own config and put table name into it*)

let get_error_code future statement = 
  let rc = ml_cass_future_error_code future in 
  let response = cstub_match_enum rc future in
    ml_cass_future_free future;
    ml_cass_statement_free statement;

  response


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
    
    get_error_code future statement


let tns_stmt session query keyStr testStr setStr =
  let valCount = cstub_convert 3 in

  let statement = ml_cass_statement_new query valCount in 
    ml_cass_statement_bind_string statement (cstub_convert 0) setStr;  (*key and value are converted into string*)
    ml_cass_statement_bind_string statement (cstub_convert 1) keyStr;
    ml_cass_statement_bind_string statement (cstub_convert 2) testStr;

  let future = ml_cass_session_execute session statement in
    ml_cass_future_wait future;

  get_error_code future statement


let del_stmt session query keyStr =
  let valCount = cstub_convert 1 in

  let statement = ml_cass_statement_new query valCount in 
    ml_cass_statement_bind_string statement (cstub_convert 0) keyStr;  (*key and value are converted into string*)
    
  let future = ml_cass_session_execute session statement in
    ml_cass_future_wait future;

  get_error_code future statement


let cx_stmt session query keyStr valStr =
  let valCount = cstub_convert 2 in

  let statement = ml_cass_statement_new query valCount in 
    ml_cass_statement_bind_string statement (cstub_convert 0) keyStr;  (*key and value are converted into string*)
    ml_cass_statement_bind_string statement (cstub_convert 1) valStr;

  let future = ml_cass_session_execute session statement in
    ml_cass_future_wait future;

  get_error_code future statement


module RO (K: Irmin.Contents.Conv) (V: Irmin.Contents.Conv) = struct

  type key = K.t
  type value = V.t
  type t = cassSession 

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

(* exception Wrong_key *)

  let find t key = 
    let keycs = Irmin.Type.encode_cstruct K.t key in 
    let keyStr = Cstruct.to_string keycs in 
    let keyStr = String.sub keyStr 8 ((String.length keyStr) - 8) in

    (*since we know that our table has only two columns and we need only 2nd one, hence the query*)
    let query = "select value from employee.empmap where key = '" ^ keyStr ^ "'" in
    let statement = ml_cass_statement_new query (cstub_convert 0) in
    let future = ml_cass_session_execute t statement in 
      ml_cass_future_wait future;

    let rc = ml_cass_future_error_code future in 
    let response = cstub_match_enum rc future in 

    if response then (
    
      let result = ml_cass_future_get_result future in
      let rowcount = ml_cass_result_row_count result in
      
      if  (cstub_convert_to_ml rowcount) > 0 then (

        let row = ml_cass_result_first_row result in 
        let value = ml_cass_row_get_column row (cstub_convert 0) in  (*seg fault comes here*)

        let valStr = cstub_get_string value in 
        let cstruct = Cstruct.of_string valStr in
                
        ml_cass_future_free future;
        ml_cass_statement_free statement;

        match (Irmin.Type.decode_cstruct V.t cstruct) with
          |Ok s -> Lwt.return_some s
          |_ -> Lwt.return_none        

      )else(
        
        ml_cass_future_free future;
        ml_cass_statement_free statement;      
        
        Lwt.return_none  
      
      );
      
      (* Lwt.return_none *)
    )else(
      
      ml_cass_future_free future;
      ml_cass_statement_free statement;

      Lwt.return_none
  ) 
    (* Lwt.return_none *)

  let mem t key = 
    find t key >>= fun v ->
      match v with 
      | Some s -> Lwt.return true
      | None -> Lwt.return false
    
end

module AO (K: Irmin.Hash.S) (V: Irmin.Contents.Conv) = struct

  include RO(K)(V)

  let add t value =
    
    let key = K.digest V.t value in 

    let keycs = Irmin.Type.encode_cstruct K.t key in 
    let keyStr = Cstruct.to_string keycs in 
    let keyStr = String.sub keyStr 8 ((String.length keyStr) - 8) in
    (* print_string ("this is key in string " ^ keyStr ^ "\n");
    print_int (String.length keyStr); *)

    let valcs = Irmin.Type.encode_cstruct V.t value in 
    let valStr = Cstruct.to_string valcs in 
    let valStr = String.sub valStr 8 ((String.length valStr) - 8) in
 
    let query = "INSERT INTO employee.office (key, value) VALUES (?, ?)" in

      ignore @@ cx_stmt t query keyStr valStr;

    Lwt.return key

end

let rec print_list = function 
[] -> ()
| e::l -> print_string e ; print_string " " ; print_list l

module RW (K: Irmin.Contents.Conv) (V: Irmin.Contents.Conv) = struct

  module RO = RO(K)(V)
  module W = Irmin.Private.Watch.Make(K)(V)
  module L = Irmin.Private.Lock.Make(K)

  type t = { t: cassSession; w: W.t; lock: L.t }
  type key = RO.key
  type value = RO.value
  type watch = W.watch

  let watches = W.v ()
  let lock = L.v ()

  let v config =
    RO.v config >>= fun t ->
    Lwt.return { t; w = watches; lock}

exception Not_allowed

  let find t = RO.find t.t
  let mem t = RO.mem t.t
  let watch_key t = raise Not_allowed (* W.watch_key t.w *)
  let watch t = raise Not_allowed (* W.watch t.w *)
  let unwatch t = raise Not_allowed (* W.unwatch t.w *)


  let list t =
    Log.debug (fun f -> f "list");
    
    let valCount = cstub_convert 0 in
    let query = "select key from employee.empmap" in
    let statement = ml_cass_statement_new query valCount in 
   (*--------check this out-----------*) 
    let future = ml_cass_session_execute t.t statement in
      ml_cass_future_wait future;
      
    let rc = ml_cass_future_error_code future in 
    let response = cstub_match_enum rc future in
(*------------check this out-----------*)
    let future = ml_cass_session_execute t.t statement in
      ml_cass_future_wait future;
      
    let rc = ml_cass_future_error_code future in 
    let response = cstub_match_enum rc future in
(*------------check this out-----------*)

    let keylist = ref [] in
      if response = true then
      (

        let result = ml_cass_future_get_result future in
        let rows = ml_cass_iterator_from_result result in
        
       while (cstub_convert_to_bool(ml_cass_iterator_next rows)) do 

          let row = ml_cass_iterator_get_row rows in
          let key_col = ml_cass_row_get_column_by_name row "key" in
          (* print_string ("\n" ^ cstub_get_string key_col); *)
            (* keylist = (cstub_get_string key_col) :: keylist; *)

            (*convert cassvalue to key*)
            let st = cstub_get_string key_col in
              print_string ("\n" ^ st);
              print_int (String.length st);
            let substr = String.sub st 0 (String.length st) in
            
            let cstruct = Cstruct.of_string substr in 
              print_int (Cstruct.len cstruct);
              (* Irmin.Type.decode_cstruct K.t cstruct; *)

            (* match (Irmin.Type.decode_cstruct V.t cstruct) with
              |Ok s -> keylist := s :: !keylist
              |_ -> [] ; *)
           
            (* let keylist = key_col :: !keylist in  *)
            (* print_int (List.length !keylist); *)
              ()
       
       done

      );
      (* print_string "\nprinting list\n"; *)
      (* print_list keylist; *)

      ml_cass_future_free future;
      ml_cass_statement_free statement;

      let kl = !keylist in 
      kl
(*the code is not returning the req. type but still compiling properly and running,
need to look into that*)
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

  let test_and_set t key ~test ~set:s =

    Log.debug (fun f -> f "test_and_set");

    let keycs = Irmin.Type.encode_cstruct K.t key in 
    let keyStr = Cstruct.to_string keycs in 
    let keyStr = String.sub keyStr 8 ((String.length keyStr) - 8) in

    let testStr = match test with 
      |Some x -> (
              let testcs = Irmin.Type.encode_cstruct V.t x in 
              let testStr = Cstruct.to_string testcs in 
              String.sub testStr 8 ((String.length testStr) - 8) )
      |None -> ""  in

    let setStr = match s with 
      |Some x -> (
              let testcs = Irmin.Type.encode_cstruct V.t x in 
              let testStr = Cstruct.to_string testcs in 
              String.sub testStr 8 ((String.length testStr) - 8) )
      |None -> ""  in


    let tns = match setStr with 
    | "" -> (
          let query = "DELETE from employee.empmap WHERE key = ?" in
          let response = del_stmt t.t query keyStr in

          if response then Lwt.return true else Lwt.return false

      )
    | _ -> (  (*IF makes the transaction light weight*)
          let query = "UPDATE employee.empmap SET value = ? WHERE key = ? IF value = ?" in
          let response = tns_stmt t.t query keyStr testStr setStr in

          if response then Lwt.return true else Lwt.return false

      ) in

    tns

end

