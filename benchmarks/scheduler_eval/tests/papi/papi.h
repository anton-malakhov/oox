// Test-only PAPI ABI subset. Never used by benchmark targets.
#pragma once
#ifndef OOX_PAPI_UNIT_TEST
#error "The fake PAPI header is for isolated unit tests only"
#endif
#define PAPI_OK 0
#define PAPI_NULL -1
#define PAPI_VER_CURRENT 0x07000000
extern "C" {
int PAPI_library_init(int);
int PAPI_thread_init(unsigned long (*)());
int PAPI_register_thread();
int PAPI_unregister_thread();
int PAPI_create_eventset(int *);
int PAPI_destroy_eventset(int *);
int PAPI_cleanup_eventset(int);
int PAPI_add_named_event(int, const char *);
int PAPI_start(int);
int PAPI_stop(int, long long *);
char *PAPI_strerror(int);
}
