(*let the application first call the config to get the session
then call the add function to put some data----->*change*------>calling the config from
inside AO module so that it can be made available to add function.----------> creating empty
config in traditional way. In place of creating map as in irmin-mem, creating session 
and returning it as t.

the return type of the add function has to be of key type. So choosing some digest function
to generate the key.
not able to make key out of uuid. so, moving forward by digesting the value itself.

Assumption:
There is already a table with <choose a name> name in <choose a name> keyspace
generate a uuid which will act as primary key. take values from the user as a tuple.*)

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

external ml_cass_session_new : unit -> cassSession = "cass_session_new"
external ml_cass_cluster_new : unit -> cassCluster = "cass_cluster_new"
external ml_cass_cluster_set_contact_points : cassCluster -> string -> unit = 
                              "cass_cluster_set_contact_points"
external ml_cass_session_connect : cassSession -> cassCluster -> cassFuture =
                              "cass_session_connect"                             
external ml_cass_future_wait : cassFuture -> unit = "cass_future_wait"
external ml_cass_future_error_code : cassFuture -> cassError = "cass_future_error_code"
external cstub_match_enum : cassError -> cassFuture -> bool = "match_enum"
external ml_cass_cluster_free : cassCluster -> unit = "cass_cluster_free"
external ml_cass_session_free : cassSession -> unit = "cass_session_free"
external ml_cass_statement_new : string -> int -> cassStatement = "cass_statement_new"
external ml_cass_session_execute : cassSession -> cassStatement -> cassFuture = "cass_session_execute"
external ml_cass_future_free : cassFuture -> unit = "cass_future_free"
external ml_cass_statement_free : cassStatement -> unit = "cass_statement_free"
external ml_cass_uuid_gen_new : unit -> cassUuidGen = "cass_uuid_gen_new"
external ml_cass_uuid_gen_free : cassUuidGen -> unit = "cass_uuid_gen_free"

external cstub_convert : int -> int = "convert"

let config () = Irmin.Private.Conf.empty

let create_cluster hosts = 
  (* let cstub_create_cluster hosts *)

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

(*  *)

module RO (K: Irmin.Contents.Conv) (V: Irmin.Contents.Conv) = struct

  (* module KMap = Map.Make(struct
      type t = K.t
      let compare = Irmin.Type.compare K.t
    end) *)

  type key = K.t
  type value = V.t
  type t = cassSession (* { mutable t: value KMap.t } *)
  (* let map = { t = KMap.empty } *)
  (* let v _config = Lwt.return map *)

  let v _config = 
    let sess = ml_cass_session_new () in 
      let hosts = "127.0.0.1" in 
      let cluster = create_cluster hosts in   
      let response = connect_session sess cluster in 
      match response with 
      | false -> (print_string "\nSession connection failed\n";
                  ml_cass_cluster_free cluster;
                  ml_cass_session_free sess;
                (* (response, sess) *)
              Lwt.return sess)
      | true -> print_string "\nSession is connected\n";

      (* (response, sess) *)
      Lwt.return sess

  (* let find { t; _ } key = *)
  let find t key = Lwt.return_none
    (* Log.debug (fun f -> f "find %a" K.pp key); *)
    (* try Lwt.return (Some (true))
    with Not_found -> Lwt.return_none *)

  (* let mem { t; _ } key = *)
  let mem t key = Lwt.return true
    (* Log.debug (fun f -> f "mem %a" K.pp key); *)
    (* Lwt.return (KMap.mem key t) *)

end



module AO (K: Irmin.Hash.S) (V: Irmin.Contents.Conv) = struct

  include RO(K)(V)

  let add t value =
    let cstruct = Irmin.Type.encode_cstruct V.t value in 
    let str = Cstruct.to_string cstruct in 
    let str = String.sub str 8 ((String.length str) - 8) in
    let strList = String.split_on_char ' ' str in
    let strArr = Array.of_list strList in
    let empid, name, dept = Array.get strArr 0, Array.get strArr 1, Array.get strArr 2 in 
  

    (* let uuid_gen = ml_cass_uuid_gen_new () in *)
    (* let uuid_key = K.digest cassUuidGen uuid_gen in *)

    let key = K.digest V.t value in (*add key to the table in the query*)
                                    (*if we want to avoid calculating hash, we
                                    need to write our own hash function*)
        
    
    let insquery = "INSERT INTO employee.office (empid, Name, Department) VALUES 
                  ("
                   ^ empid 
                   ^ " , " 
                   ^ "\'" ^ name ^ "\'" 
                   ^ " , " 
                   ^ "\'" ^ dept ^ "\'"  
                   ^ ")" in

    let insqueryStatus = execute_query t insquery in   (*changed sess to t*)

    (* ml_cass_uuid_gen_free uuid_gen; *)

    Lwt.return key

end

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
    Lwt.return { t; w = watches; lock }

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
    let cstruct = Irmin.Type.encode_cstruct V.t value in 
    let str = Cstruct.to_string cstruct in 
    print_string str;
    let value = String.sub str 8 ((String.length str) - 8) in
    print_string value;
    
    let cstruct = Irmin.Type.encode_cstruct K.t key in 
    let str = Cstruct.to_string cstruct in 
    let key = String.sub str 8 ((String.length str) - 8) in
    


    let insquery = "INSERT INTO employee.empmap (key, value) VALUES 
                  ("
                   ^ "\'" ^ key ^ "\'" 
                   ^ " , " 
                   ^ "\'" ^ value ^ "\'"  
                   ^ ")" in
     let insqueryStatus = execute_query t.t insquery in 
     
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
    (* cassCondAdd set; *)
    
    (* L.with_lock t.lock key (fun () ->
        find t key >>= fun v ->
        if Irmin.Type.(equal (option V.t)) test v then (
          let () = match set with
            | None   -> t.t.RO.t <- RO.KMap.remove key t.t.RO.t
            | Some v -> t.t.RO.t <- RO.KMap.add key v t.t.RO.t
          in
          Lwt.return true
        ) else
          Lwt.return false
      ) >>= fun updated ->
    (if updated then W.notify t.w key set else Lwt.return_unit) >>= fun () ->
    Lwt.return updated *)
    Lwt.return true







  (* module RO = RO(K)(V)
  module W = Irmin.Private.Watch.Make(K)(V)

  type t = cassSession
  type key = RO.key
  type value = RO.value
  type watch = W.watch

  let watches = W.v ()

  let test_and_set t key ~test ~set = Lwt.return true
  let remove t key = Lwt.return_unit 
  let list t = Lwt.return []

  let unwatch t = ()
  let watch t = W.watch t
  let watch_key t = ()  
  let mem t = RO.mem t      
  let find t = RO.find t  

  let v config = 
    let sess = ml_cass_session_new () in 
      let hosts = "127.0.0.1" in 
      let cluster = create_cluster hosts in   
      let response = connect_session sess cluster in 
      match response with 
      | false -> (print_string "\nSession connection failed\n";
                  ml_cass_cluster_free cluster;
                  ml_cass_session_free sess;
                (* (response, sess) *)
              Lwt.return sess)
      | true -> print_string "\nSession is connected\n";

      (* (response, sess) *)
      Lwt.return sess

(* key will be branch name and value will be the key got from AO *)
  let set t key value = 
    (* let insquery = "INSERT INTO employee.empmap (key, value) VALUES 
                  ("
                   ^ "\'" ^ key ^ "\'" 
                   ^ " , " 
                   ^ "\'" ^ value ^ "\'"  
                   ^ ")" in
     let insqueryStatus = execute_query t insquery in *)
     
     Lwt.return_unit   
 *)
  
  
end

(* let config () = 
  let sess = ml_cass_session_new () in 
    let hosts = "127.0.0.1" in 
    let cluster = create_cluster hosts in   
    let response = connect_session sess cluster in 
    match response with 
    | false -> (print_string "\nSession connection failed\n";
                ml_cass_cluster_free cluster;
                ml_cass_session_free sess;
              (response, sess))
    | true -> print_string "\nSession is connected\n";

    (response, sess)
 *)


  (* (* ------------------------------ *)

   cassAdd value;
   (* ------------------------------ *)

    print_endline "\ncall from module\n";
    let key = K.digest V.t value in
    Log.debug (fun f -> f "add -> %a" K.pp key);
    t.t <- KMap.add key value t.t;
    Lwt.return key *)

