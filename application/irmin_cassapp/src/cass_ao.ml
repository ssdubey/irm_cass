(*  #require "lwt.unix";;
 #require "irmin";;
 #require "irmin-mem";;

  *)
  open Lwt_main;;

(* this applicatin will store values in Irmin_mem and fetch them*)
module Store_module = Irmin_mem.AO (Irmin.Hash.SHA1) (Irmin.Contents.String);;

let config = Irmin_mem.config () in
let hashtable = Lwt_main.run @@ Store_module.v config in

let key = Lwt_main.run  (Store_module.add hashtable "value1") in

print_string "\n the value is stored in the mem\n";

let item = Lwt_main.run @@ Store_module.find hashtable key in 
  match item with
  | Some i -> print_string i
  | None -> print_string "its a scam\n";;








(* open Lwt_main

(* this applicatin will store values in Irmin_mem and fetch them*)
module Store_module = Irmin_mem.Append_only (Irmin.Contents.String) (Irmin.Contents.String);;

let config = Irmin_mem.config ();;
let hashtable = Lwt_main.run @@ Store_module.v config;;

Store_module.batch hashtable (fun hashtable -> Store_module.add hashtable "key" "value");;

print_string "\n the value is stored in the mem\n";;

let item = Lwt_main.run @@ Store_module.find hashtable "key" in 
  match item with
  | Some i -> print_string i
  | None -> print_string "its a scam\n"
 *)