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

#include "jni/VeloxAllocationListener.h"

#include <glog/logging.h>
#include <glog/raw_logging.h>

#include "jni/JniCommon.h"
#include "jni/JniError.h"
#include "jni/VeloxJavaException.h"

namespace gluten {
namespace {

void restoreAfterRepayment(JNIEnv* env, jthrowable pending) {
  env->ExceptionClear(); // A failure to capture the secondary error must not replace the first.
  const auto status = env->Throw(pending);
  env->DeleteLocalRef(pending);
  if (status != JNI_OK && !env->ExceptionCheck()) {
    // Report the restore failure to Java without undoing a successful native repayment.
    struct RestoreFailure final : std::exception {
      const char* what() const noexcept override {
        return "Unable to restore the pending Java exception after memory repayment";
      }
    } failure;
    handleJniException(env, failure);
    if (!env->ExceptionCheck()) {
      throw failure;
    }
  }
}

// The local reference stays on this JNI entry's thread; saving it needs no global reference.
template <typename Repay>
void repayPreservingPending(JNIEnv* env, jthrowable pending, Repay repay) {
  if (!pending) {
    repay();
    return;
  }
  env->ExceptionClear();
  try {
    repay();
  } catch (...) {
    restoreAfterRepayment(env, pending);
    throw;
  }
  restoreAfterRepayment(env, pending);
}

class VeloxJniAllocationListener final : public SparkAllocationListener {
 public:
  VeloxJniAllocationListener(JavaVM* vm, jobject listener, const facebook::velox::VeloxException& fallback)
      : SparkAllocationListener(vm, listener), fallback_(fallback) {}

  void allocationChanged(int64_t diff) override {
    if (diff != 0) {
      wrapJavaException(
          [&] {
            auto* env = attachedEnv();
            if (env->ExceptionCheck()) {
              if (JniEntryGuard::isActive()) {
                if (diff > 0) {
                  // maybeReserve can swallow the native error. Keep the throwable pending for Java.
                  throw fallback_;
                }
                repayPreservingPending(env, env->ExceptionOccurred(), [&] { notifyJava(env, diff); });
                return;
              }
              checkException(env);
            }
            notifyJava(env, diff);
          },
          &fallback_);
    }
  }

 private:
  const facebook::velox::VeloxException& fallback_;
};

} // namespace

std::unique_ptr<AllocationListener> createVeloxJniAllocationListener(JavaVM* vm, jobject listener) {
  // Allocate before any reservation callback. Copies share immutable state; the cause is nested per throw.
  static const auto fallback = makeVeloxJavaException(nullptr, "Java memory reservation callback failed");
  return std::make_unique<VeloxJniAllocationListener>(vm, listener, fallback);
}

void rollbackVeloxReservation(AllocationListener& listener, int64_t reclaimedBytes, const std::exception& error) {
  if (reclaimedBytes == 0) {
    listener.allocationChanged(0);
    return;
  }
  RAW_VLOG(2, "Returning %lld reclaimed bytes after a failed reservation", static_cast<long long>(reclaimedBytes));
  // Failed carrier capture may leave a Java error pending for the guarded JNI return.
  auto* env = envForCurrentJniEntry();
  const auto pending = env ? env->ExceptionOccurred() : nullptr;
  if (!pending && !findJavaExceptionCause(error, unwrapVeloxException)) {
    // Includes capture failure on a worker: neither a carrier nor a pending throwable survived.
    listener.allocationChanged(-reclaimedBytes);
    return;
  }
  repayPreservingPending(env, pending, [&] {
    try {
      listener.allocationChanged(-reclaimedBytes);
    } catch (const std::exception& rollbackError) {
      RAW_LOG(WARNING, "Unreserve failed after a Java reservation error: %s", rollbackError.what());
    } catch (...) {
      RAW_LOG(WARNING, "Unreserve threw a non-standard exception after a Java reservation error");
    }
    if (!pending && env && env->ExceptionCheck()) {
      env->ExceptionClear(); // The primary is in the reservation's C++ carrier.
    }
  });
}

} // namespace gluten
