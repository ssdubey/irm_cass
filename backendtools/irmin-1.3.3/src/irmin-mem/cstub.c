#define CAML_NAME_SPACE
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <caml/memory.h>

#include "cassandra.h"


void print_error(CassFuture* future) {
  const char* message;
  size_t message_length;
  cass_future_error_message(future, &message, &message_length);
  fprintf(stderr, "Error: %.*s\n", (int)message_length, message);
}

CAMLprim value match_enum(value rc, value future){
	CAMLparam2(rc, future);
	if (rc != CASS_OK) {
		print_error((CassFuture*)future);	//typecast to avoid warning

    	CAMLreturn(Val_int(0));//false
  }else{
  	CAMLreturn (Val_int(1));//true
  }
 }
 
 CAMLprim value convert(value val){
 	CAMLparam1(val);
 	CAMLreturn(Int_val(val));
 }

