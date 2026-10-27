/*
 * The C half of the cross-binding golden test, and the ABI's memory contract under
 * AddressSanitizer / LeakSanitizer / UBSan (or Valgrind). Driven by run.sh.
 *
 *   harness <request.json>   plan the request, print the response to stdout
 *
 * Every other check is internal and fails with a message and exit code 1. A leak is
 * reported by LeakSanitizer at exit (or by Valgrind), so every string this program
 * receives from planner_plan is released with planner_string_free — exactly once.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "planner.h"

#define CHECK(cond, what)                                                          \
    do {                                                                           \
        if (!(cond)) {                                                             \
            fprintf(stderr, "harness: FAILED: %s (%s:%d)\n", what, __FILE__,       \
                    __LINE__);                                                     \
            exit(1);                                                               \
        }                                                                          \
    } while (0)

static char *read_file(const char *path) {
    FILE *f = fopen(path, "rb");
    CHECK(f != NULL, "open the request file");
    CHECK(fseek(f, 0, SEEK_END) == 0, "seek the request file");
    long len = ftell(f);
    CHECK(len >= 0, "size the request file");
    rewind(f);
    char *buf = malloc((size_t)len + 1);
    CHECK(buf != NULL, "allocate the request buffer");
    CHECK(fread(buf, 1, (size_t)len, f) == (size_t)len, "read the request file");
    buf[len] = '\0';
    fclose(f);
    return buf;
}

int main(int argc, char **argv) {
    CHECK(argc == 2, "usage: harness <request.json>");

    /* The ABI and the contract it speaks. */
    CHECK(planner_abi_version() == 1, "planner_abi_version() == 1");
    const char *contract = planner_contract_version();
    CHECK(contract != NULL && strncmp(contract, "planner-embed/", 14) == 0,
          "planner_contract_version() names the wire contract");

    /* A NULL request is a JSON error, not a crash — and still ours to free. */
    char *err = planner_plan(NULL);
    CHECK(err != NULL, "planner_plan(NULL) returns an error response");
    CHECK(strstr(err, "\"ffi_error\"") != NULL, "a NULL request answers ffi_error");
    planner_string_free(err);

    /* Bytes that are not UTF-8 are refused the same way. */
    char *bad = planner_plan("{\"models\": \xff}");
    CHECK(bad != NULL && strstr(bad, "\"ffi_error\"") != NULL, "non-UTF-8 is ffi_error");
    planner_string_free(bad);

    /* Malformed JSON is the funnel's bad_request, through the same path. */
    char *malformed = planner_plan("{not json");
    CHECK(malformed != NULL && strstr(malformed, "\"bad_request\"") != NULL,
          "malformed JSON answers bad_request");
    planner_string_free(malformed);

    /* Freeing NULL is a no-op, however often. */
    planner_string_free(NULL);
    planner_string_free(NULL);

    /* Many alloc/free round trips: a per-call leak or a double free shows up here. */
    char *request = read_file(argv[1]);
    for (int i = 0; i < 200; i++) {
        char *resp = planner_plan(request);
        CHECK(resp != NULL, "planner_plan(request) returns a response");
        planner_string_free(resp);
    }

    /* The golden response, for run.sh to compare against the fixture. */
    char *resp = planner_plan(request);
    CHECK(resp != NULL, "planner_plan(request) returns a response");
    CHECK(strstr(resp, "\"error\"") == NULL, "the golden request plans without error");
    fputs(resp, stdout);
    fputc('\n', stdout);
    planner_string_free(resp);
    free(request);
    return 0;
}
