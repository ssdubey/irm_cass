#define CAML_NAME_SPACE
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <caml/memory.h>
#include <caml/alloc.h>

#include "cassandra.h"


void print_error(CassFuture* future) {
  const char* message;
  size_t message_length;
  cass_future_error_message(future, &message, &message_length);
  fprintf(stderr, "Error: %.*s\nthis was an error\n", (int)message_length, message);
}

CAMLprim value match_enum(value rc, value future){
	CAMLparam2(rc, future);
	if (rc != CASS_OK) {
		print_error((CassFuture*)future);	//typecast to avoid warning
    fprintf(stderr, "\ninside error of match enum\n");
    	CAMLreturn(Val_int(0));//false
  }else{
    // fprintf(stderr, "\nno error in match enum\n");
  	CAMLreturn (Val_int(1));//true
  }
 }
 
 CAMLprim value convert_to_ml(value val){
  CAMLparam1(val);
  CAMLreturn(Val_int(val));
 }

 CAMLprim value convert(value val){
 	CAMLparam1(val);
 	CAMLreturn(Int_val(val));
 }

 CAMLprim value convert_to_bool(value val){
  CAMLparam1(val);
  // printf ("\nvalue received is : %d\n", val);
  if (val == 1){
    CAMLreturn (Val_int(1));
  }else{
    CAMLreturn(Val_int(0));
  }
 }

CAMLprim value get_string(value val){
  
  CAMLparam1(val);
  CAMLlocal1(var_value);
  const char* text;
  size_t text_length;
  
  // char sub[10000];

  // printf("inside c function");
  cass_value_get_string((const CassValue*)val, &text, &text_length);
  // printf ("\nin c: %s", text);
  // cass_value_get_string((const CassValue*)val, &text, &text_length);
  // printf ("\nin c: %s", text);

  int c = 0;
  int length = (int)text_length;
  // printf("\nlength of the text received in C = %d\n", length);
  // char* sub = (char*) malloc (sizeof(char) * length);
  char sub [length];
  
  while (c < length) {
      sub[c] = text[c];
      c++;
  }
  // printf("\nhere\n");
  sub[c] = '\0';
  // printf("\nfinal index after copying whole string = %d\n", c);
  // printf("\nafter substring: %s\n", sub);
  var_value = caml_copy_string(sub);
  // free(sub);
  // var_value = caml_copy_string(text);
  // printf ("\nafter caml copy = %s", var_value);
  CAMLreturn(var_value);
 }

// CAMLprim value c_fun(value session, value query){
//   CAMLparam2(session, query);
//   CAMLlocal1(var_value);
//   CassError rc = CASS_OK;
//   CassFuture* future = NULL;
//   CassStatement* statement = cass_statement_new(query, 0);
//   char sub[100];
  
//   future = cass_session_execute(session, statement);
//   cass_future_wait(future);

//   rc = cass_future_error_code(future);
//   if (rc != CASS_OK) {
//     print_error(future);
//    }else {
//      const CassResult* result = NULL;
//      CassIterator* rows = NULL;

//      result = cass_future_get_result(future);
//      rows = cass_iterator_from_result(result);

//      while (cass_iterator_next(rows)) {
//        const CassRow* row = cass_iterator_get_row(rows);
//        const CassValue* id_value = cass_row_get_column_by_name(row, "key");
//        const CassValue* item_value = cass_row_get_column_by_name(row, "value");
//        CassIterator* item = cass_iterator_from_tuple(item_value);

//       //  while (cass_iterator_next(item)) {
//       //    const CassValue* value = cass_iterator_get_value(item);

//   //       if (!cass_value_is_null(value)) {
//            const char* text;
//              size_t text_length;
//              cass_value_get_string(item_value, &text, &text_length);
//              printf("\nprinting: \"%.*s\" \n", (int)text_length, text);
//              printf("\nprinting length: %d\n", (int)text_length);
//             //  char* a;
//             //  strncpy(&a, text, (int)text_length);
//             //  a[(int)text_length] = '\0';
//               printf("\nexperiment: %s\n", text);

//               int c = 0;
//               int length = (int)text_length;
              
//               while (c < length) {
//                   sub[c] = text[c];
//                   c++;
//               }
//               sub[c] = '\0';
//               printf("\nafter substring: %s\n", sub);
//               var_value = caml_copy_string(sub);
//   //       }
//       //  }
//      }
//        cass_result_free(result);
//    cass_iterator_free(rows);
//    }

//   cass_future_free(future);
//   cass_statement_free(statement);

//     CAMLreturn(var_value);
  
// }