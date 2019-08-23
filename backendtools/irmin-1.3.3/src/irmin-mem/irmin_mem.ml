(*
 * Copyright (c) 2013-2017 Thomas Gazagnaire <thomas@gazagnaire.org>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *)

open Lwt.Infix
(* ------------------------------ *)
type cassSession
type cassStatement
type cassCluster
type uuid
type cassFuture  
type cassError = Failure | Success
type cassUuid
type cassUuidGen
type stubtype
type valuestr

external ml_cass_session_new : unit -> cassSession = "cass_session_new"
external ml_cass_cluster_new : unit -> cassCluster = "cass_cluster_new"
external ml_cass_cluster_set_contact_points : cassCluster -> string -> unit = "cass_cluster_set_contact_points"
external ml_cass_uuid_gen_new : unit -> uuid = "cass_uuid_gen_new"
external ml_cass_cluster_free : cassCluster -> unit = "cass_cluster_free"
external ml_cass_session_free : cassSession -> unit = "cass_session_free"
external ml_cass_uuid_gen_free : uuid -> unit = "cass_uuid_gen_free"

external cstub_cass_future_error_message: cassFuture -> unit = "cstub_cass_future_error_message"
external cstub_connect_session : cassSession -> cassCluster -> int = "cstub_connect_session"
external cstub_execute_query : cassSession -> string -> cassError = "cstub_execute_query"
external cstub_insert_into_tuple : cassSession -> uuid -> cassError = "cstub_insert_into_tuple"
external cstub_select_from_tuple : cassSession -> cassError = "cstub_select_from_tuple"
(* ------------------------------ *)


let src = Logs.Src.create "irmin.mem" ~doc:"Irmin in-memory store"
module Log = (val Logs.src_log src : Logs.LOG)

module RO (K: Irmin.Contents.Conv) (V: Irmin.Contents.Conv) = struct

  module KMap = Map.Make(struct
      type t = K.t
      let compare = Irmin.Type.compare K.t
    end)

  type key = K.t
  type value = V.t
  type t = { mutable t: value KMap.t }
  let map = { t = KMap.empty }
  let v _config = Lwt.return map

  let find { t; _ } key =
    Log.debug (fun f -> f "find %a" K.pp key);
    try Lwt.return (Some (KMap.find key t))
    with Not_found -> Lwt.return_none

  let mem { t; _ } key =
    Log.debug (fun f -> f "mem %a" K.pp key);
    Lwt.return (KMap.mem key t)

end

(* ------------------------------ *)


let create_cluster hosts = 
  (* let cstub_create_cluster hosts *)

  let cluster = ml_cass_cluster_new () in 
    ml_cass_cluster_set_contact_points cluster hosts;
    cluster

let execute_query session query = 
  cstub_execute_query session query
  

let print_error future =
  cstub_cass_future_error_message future


let connect_session session cluster = 
  cstub_connect_session session cluster



let cassAdd value =
  let sess = ml_cass_session_new () in 
    let hosts = "127.0.0.1" in 
    let cluster = create_cluster hosts in
    let uuid_gen = ml_cass_uuid_gen_new () in 
 
    let response = connect_session sess cluster in
      (* if response == 2 then begin
        ml_cass_cluster_free cluster;
        ml_cass_session_free sess
      end; *)
    
  execute_query sess "CREATE KEYSPACE examples WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3' }";
    execute_query sess "CREATE TABLE examples.tuples (id timeuuid, item frozen<tuple<text, bigint>>, PRIMARY KEY(id))";

    (* cstub_insert_into_tuple sess uuid_gen; *)
    cstub_insert_into_tuple sess uuid_gen;
    cstub_select_from_tuple sess;

    ml_cass_cluster_free cluster;
    ml_cass_session_free sess;

    ml_cass_uuid_gen_free uuid_gen

(* ------------------------------ *)



module AO (K: Irmin.Hash.S) (V: Irmin.Contents.Conv) = struct

  include RO(K)(V)

  let add t value =
  (* ------------------------------ *)

   cassAdd value;
   (* ------------------------------ *)

    print_endline "\ncall from module\n";
    let key = K.digest V.t value in
    Log.debug (fun f -> f "add -> %a" K.pp key);
    t.t <- KMap.add key value t.t;
    Lwt.return key

end

module Link (K: Irmin.Hash.S) = struct

  include RO(K)(K)

  let add t index key =
    Log.debug (fun f -> f "add link");
    t.t <- KMap.add index key t.t;
    Lwt.return_unit

end

module RW (K: Irmin.Contents.Conv) (V: Irmin.Contents.Conv) = struct

  module RO = RO(K)(V)
  module W = Irmin.Private.Watch.Make(K)(V)
  module L = Irmin.Private.Lock.Make(K)

  type t = { t: RO.t; w: W.t; lock: L.t }
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
    RO.KMap.fold (fun k _ acc -> k :: acc) t.t.RO.t []
    |> Lwt.return

  let set t key value =
    Log.debug (fun f -> f "update");
    L.with_lock t.lock key (fun () ->
        t.t.RO.t <- RO.KMap.add key value t.t.RO.t;
        Lwt.return_unit
      ) >>= fun () ->
    W.notify t.w key (Some value)

  let remove t key =
    Log.debug (fun f -> f "remove");
    L.with_lock t.lock key (fun () ->
        t.t.RO.t <- RO.KMap.remove key t.t.RO.t ;
        Lwt.return_unit
      ) >>= fun () ->
    W.notify t.w key None

  let test_and_set t key ~test ~set =
    Log.debug (fun f -> f "test_and_set");
    L.with_lock t.lock key (fun () ->
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
    Lwt.return updated

end

let config () = Irmin.Private.Conf.empty
  

module Make = Irmin.Make(AO)(RW)

module KV (C: Irmin.Contents.S) =
  Make
    (Irmin.Metadata.None)
    (C)
    (Irmin.Path.String_list)
    (Irmin.Branch.String)
    (Irmin.Hash.SHA1)
