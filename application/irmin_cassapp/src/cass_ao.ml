(*  #require "lwt.unix";;
 #require "irmin";;
 #require "irmin-mem";;

  *)
open Lwt_main;;
open Irmin;;
open Irmin_mem;;

(* this applicatin will store values in Irmin_mem and fetch them*)
module Store_module = Irmin_mem.AO (Irmin.Hash.SHA1) (Irmin.Contents.String);;
module RW_module = Irmin_mem.RW (Irmin.Contents.String) (Irmin.Contents.String);;

(* -------------------------------------------------------------------------- *)
let config = Irmin_mem.config () in
let hashtable = Lwt_main.run @@ Store_module.v config in

let key = Lwt_main.run  (Store_module.add hashtable "value2") in

let item = Lwt_main.run @@ Store_module.find hashtable key in 
  match item with
  | Some i -> print_string i
  | None -> print_string "its a scam\n";;
(* -------------------------------------------------------------------------- *)

let config = Irmin_mem.config () in
let aw_t = Lwt_main.run @@ RW_module.v config in
let _ = RW_module.set aw_t "ke" "value1" in 

let item = Lwt_main.run @@ RW_module.find aw_t "ke" in 
let _ = match item with
  | Some i -> print_string i
  | None -> print_string "its a scam\n" in

(* let a = Some "tns value" in 
let b = Some "value1" in *)
let tnsRet = Lwt_main.run @@ RW_module.test_and_set aw_t 
              "keyy" ~test:(Some "value1") ~set:(Some "tns value") in 
let _= match tnsRet with
  | true -> print_string "\nit is true\n"
  | false -> print_string "\nit is false\n" in

  let item = Lwt_main.run @@ RW_module.find aw_t "ke" in 
  match item with
  | Some i -> print_string i
  | None -> print_string "its a scam\n";







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