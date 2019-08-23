#define CAML_NAME_SPACE
#include<stdio.h>
#include<caml/mlvalues.h>
#include<caml/alloc.h>
#include<caml/memory.h>
#include<caml/fail.h>
#include<caml/callback.h>
#include<caml/custom.h>
#include<caml/intext.h>
#include<caml/threads.h>

#include "cassandra.h"

/*CAMLprim value sum (value a, value b){
	//CAMLreturn (a+b);
	return (a+b);
}*/

int sum (int a, int b){
	//CAMLreturn (a+b);
	return (a*b);
}

CAMLprim value stub_sum(value a, value b)
{
	//printf("step1");
	CAMLparam2 (a, b);
	printf("from C code: %d %d", Int_val(a), Int_val(b));

	CAMLlocal1 (result);
	result = sum(Int_val(a), Int_val(b));
	//result = add(a,b);
	//result = a;
 	CAMLreturn(Val_int(result));
}

CAMLprim value cstub_cass_future_error_message(value future){
  const char* message;
  size_t message_length;
  cass_future_error_message(future, &message, &message_length);
  fprintf(stderr, "Error: %.*s\n", (int)message_length, message);
}

void print_error(CassFuture* future) {
  const char* message;
  size_t message_length;
  cass_future_error_message(future, &message, &message_length);
  fprintf(stderr, "Error: %.*s\n", (int)message_length, message);
}

CassError cstub_connect_session(CassSession* session, const CassCluster* cluster) {
	printf("\ncstub connect\n");
  CassError rc = CASS_OK;
  CassFuture* future = cass_session_connect(session, cluster);

  cass_future_wait(future);
  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    print_error(future);
    printf("\ncass not ok\n");
  }else{
  printf("\ncass ok\n");
  printf("\nrc=%d\n", rc);
}
  cass_future_free(future);

  return rc;
}

CassError cstub_execute_query(CassSession* session, const char* query) {
  CassError rc = CASS_OK;
  CassFuture* future = NULL;
  CassStatement* statement = cass_statement_new(query, 0);

  future = cass_session_execute(session, statement);
  cass_future_wait(future);

  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    print_error(future);
  }

  cass_future_free(future);
  cass_statement_free(statement);

  return rc;
}

CassCluster* cstub_create_cluster(const char* hosts) {
  CassCluster* cluster = cass_cluster_new();
  cass_cluster_set_contact_points(cluster, hosts);
  return cluster;
}

// CassError cstub_insert_into_tuple(CassSession* session, CassUuidGen* uuid_gen, char* value) {
CassError cstub_insert_into_tuple(CassSession* session, CassUuidGen* uuid_gen) {
  CassError rc = CASS_OK;
  CassStatement* statement = NULL;
  CassFuture* future = NULL;

  CassUuid id;
  char id_str[CASS_UUID_STRING_LENGTH];
  CassTuple* item = NULL;
printf("\ninside insert tuple function\n");
  const char* query = "INSERT INTO examples.tuples (id, item) VALUES (?, ?)";

  statement = cass_statement_new(query, 2);

  cass_uuid_gen_time(uuid_gen, &id);
  cass_uuid_string(id, id_str);

  item = cass_tuple_new(2);

  cass_tuple_set_string(item, 0, id_str);
  //cass_tuple_set_string(item, 0, Int_val(v);
  cass_tuple_set_int64(item, 1, id.time_and_version);
  /*cass_tuple_set_string(item, 1, value);*/

  cass_statement_bind_uuid(statement, 0, id);
  cass_statement_bind_tuple(statement, 1, item);

  future = cass_session_execute(session, statement);
  cass_future_wait(future);

  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    print_error(future);
  }

  cass_future_free(future);
  cass_statement_free(statement);
  cass_tuple_free(item);

  return rc;
}

CassError cstub_select_from_tuple(CassSession* session) {
  CassError rc = CASS_OK;
  CassStatement* statement = NULL;
  CassFuture* future = NULL;

  const char* query = "SELECT * FROM examples.tuples";

  statement = cass_statement_new(query, 0);

  future = cass_session_execute(session, statement);
  cass_future_wait(future);

  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    print_error(future);
  } else {
    const CassResult* result = NULL;
    CassIterator* rows = NULL;

    result = cass_future_get_result(future);
    rows = cass_iterator_from_result(result);

    while (cass_iterator_next(rows)) {
      CassUuid id;
      char id_str[CASS_UUID_STRING_LENGTH];
      const CassRow* row = cass_iterator_get_row(rows);
      const CassValue* id_value = cass_row_get_column_by_name(row, "id");
      const CassValue* item_value = cass_row_get_column_by_name(row, "item");
      CassIterator* item = cass_iterator_from_tuple(item_value);

      cass_value_get_uuid(id_value, &id);
      cass_uuid_string(id, id_str);

      printf("id %s ", id_str);

      while (cass_iterator_next(item)) {
        const CassValue* value = cass_iterator_get_value(item);

        if (!cass_value_is_null(value)) {
          if (cass_value_type(value) == CASS_VALUE_TYPE_VARCHAR) {
            const char* text;
            size_t text_length;
            cass_value_get_string(value, &text, &text_length);
            printf("\"%.*s\" ", (int)text_length, text);
          } else if (cass_value_type(value) == CASS_VALUE_TYPE_BIGINT) {
            cass_int64_t i;
            cass_value_get_int64(value, &i);
            printf("%lld ", (long long int)i);
          } else {
            printf("<invalid> ");
          }
        } else {
          printf("<null> ");
        }
      }

      printf("\n");
    }

    cass_result_free(result);
    cass_iterator_free(rows);
  }

  cass_future_free(future);
  cass_statement_free(statement);

  return rc;
}
