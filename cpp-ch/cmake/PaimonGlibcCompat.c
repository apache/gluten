/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/* Link-time wrappers for selected glibc calls in libpaimon.so. They bind math
 * calls to older symbol versions and emulate pthread_cond_clockwait on older
 * glibc releases; the getenv wrapper avoids a Bash startup symbol conflict. */
#define _GNU_SOURCE

#include <errno.h>
#include <math.h>
#include <pthread.h>
#include <stdlib.h>
#include <time.h>

/* Bash exports its own getenv, which is unsafe before Bash has initialized. */
char *__wrap_getenv(const char *name) { return secure_getenv(name); }

/* glibc 2.27/2.29 changed the default versions of these functions. */
#define WRAP_UNARY(name, type)                   \
    extern type old_##name(type);                \
    __asm__(".symver old_" #name "," #name "@GLIBC_2.2.5"); \
    type __wrap_##name(type value) { return old_##name(value); }

#define WRAP_BINARY(name, type)                  \
    extern type old_##name(type, type);          \
    __asm__(".symver old_" #name "," #name "@GLIBC_2.2.5"); \
    type __wrap_##name(type left, type right) { return old_##name(left, right); }

WRAP_UNARY(exp, double)
WRAP_UNARY(exp2, double)
WRAP_UNARY(expf, float)
WRAP_UNARY(log, double)
WRAP_UNARY(log2, double)
WRAP_UNARY(log2f, float)
WRAP_UNARY(logf, float)
WRAP_BINARY(pow, double)
WRAP_BINARY(powf, float)

/* pthread_cond_clockwait was introduced in glibc 2.30. */
int __wrap_pthread_cond_clockwait(
    pthread_cond_t *cond, pthread_mutex_t *mutex, clockid_t clock_id,
    const struct timespec *deadline)
{
    if (clock_id == CLOCK_REALTIME)
        return pthread_cond_timedwait(cond, mutex, deadline);
    if (clock_id != CLOCK_MONOTONIC || !deadline || deadline->tv_nsec < 0 ||
        deadline->tv_nsec >= 1000000000L)
        return EINVAL;

    struct timespec now_mono;
    struct timespec now_real;
    if (clock_gettime(CLOCK_MONOTONIC, &now_mono) != 0 ||
        clock_gettime(CLOCK_REALTIME, &now_real) != 0)
        return errno;

    time_t seconds = deadline->tv_sec - now_mono.tv_sec;
    long nanoseconds = deadline->tv_nsec - now_mono.tv_nsec;
    if (nanoseconds < 0) {
        --seconds;
        nanoseconds += 1000000000L;
    }
    if (seconds < 0)
        return ETIMEDOUT;

    struct timespec real_deadline = {now_real.tv_sec + seconds,
                                     now_real.tv_nsec + nanoseconds};
    if (real_deadline.tv_nsec >= 1000000000L) {
        ++real_deadline.tv_sec;
        real_deadline.tv_nsec -= 1000000000L;
    }
    return pthread_cond_timedwait(cond, mutex, &real_deadline);
}
