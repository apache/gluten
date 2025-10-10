#include "TaskStatusListener.h"

namespace gluten {

TaskStatusListener TaskStatusListener::instance_;

TaskStatusListener* TaskStatusListener::getInstance() {
  std::call_once(instance_.threadStartedFlag_, [&]() { instance_.startListener(); });
  return &instance_;
}

template <typename T>
static void check(JNIEnv* env, T obj, const char* where) {
  checkException(env);
  GLUTEN_CHECK(obj != nullptr, where);
}

void TaskStatusListener::addTask(int64_t taskAttemptId, std::weak_ptr<bytedance::bolt::exec::Task> task) {
  LOG(INFO) << __FUNCTION__ << " taskAttemptId=" << taskAttemptId;
  auto jvm = getJniCommonState()->getJavaVM();
  JNIEnv* env;
  attachCurrentThreadAsDaemonOrThrow(jvm, &env);
  jclass taskContextCls = env->FindClass("org/apache/spark/TaskContext");
  checkException(env);
  check(env, taskContextCls, "Cannot find org/apache/spark/TaskContext");

  jmethodID getMethod = env->GetStaticMethodID(taskContextCls, "get", "()Lorg/apache/spark/TaskContext;");
  check(env, getMethod, "Cannot find TaskContext.get()");

  jobject taskContextObj = env->CallStaticObjectMethod(taskContextCls, getMethod);
  check(env, taskContextObj, "TaskContext.get() returned null");

  jobject global = env->NewGlobalRef(taskContextObj);
  check(env, global, "NewGlobalRef failed");
  env->DeleteLocalRef(taskContextObj);
  env->DeleteLocalRef(taskContextCls);

  std::lock_guard<std::mutex> guard(lock_);
  GLUTEN_CHECK(
      tasks_.find(taskAttemptId) == tasks_.end(),
      "Task with attempt ID already exists: " + std::to_string(taskAttemptId));
  tasks_.emplace(taskAttemptId, TaskContext(global, std::move(task)));
}

void deleteTaskContextRef(JNIEnv* env, jobject globalRef) {
  if (globalRef != nullptr) {
    env->DeleteGlobalRef(globalRef);
  }
}

void TaskStatusListener::removeTask(int64_t taskAttemptId) {
  LOG(INFO) << __FUNCTION__ << " taskAttemptId=" << taskAttemptId;
  std::lock_guard<std::mutex> guard(lock_);
  auto it = tasks_.find(taskAttemptId);
  if (it != tasks_.end()) {
    auto jvm = getJniCommonState()->getJavaVM();
    JNIEnv* env;
    attachCurrentThreadAsDaemonOrThrow(jvm, &env);
    deleteTaskContextRef(env, it->second.jTaskContext);
    tasks_.erase(it);
  }
}

void TaskStatusListener::listen() {
  try {
    auto jniCommonState = getJniCommonState();
    GLUTEN_CHECK(jniCommonState != nullptr, "JniCommonState is null");
    auto jvm = getJniCommonState()->getJavaVM();
    GLUTEN_CHECK(jvm != nullptr, "JavaVM is null in TaskStatusListener");
    JNIEnv* env;
    attachCurrentThreadAsDaemonOrThrow(jvm, &env);
    // Cache classes and method IDs to minimize per-iteration overhead.
    jclass taskContextCls = env->FindClass("org/apache/spark/TaskContext");
    check(env, taskContextCls, "Cannot find org/apache/spark/TaskContext");

    jmethodID getKillReasonMid = env->GetMethodID(taskContextCls, "getKillReason", "()Lscala/Option;");
    check(env, getKillReasonMid, "Failed to get TaskContext.getKillReason method");

    jclass optionCls = env->FindClass("scala/Option");
    check(env, optionCls, "Failed to find scala.Option class");

    jmethodID isDefinedMid = env->GetMethodID(optionCls, "isDefined", "()Z");
    check(env, isDefinedMid, "Failed to get Option.isDefined method");

    jmethodID optionGetMid = env->GetMethodID(optionCls, "get", "()Ljava/lang/Object;");
    check(env, optionGetMid, "Failed to get Option.get method");

    jclass stringCls = env->FindClass("java/lang/String");
    check(env, stringCls, "Failed to find java.lang.String class");
    while (status_.load(std::memory_order_acquire) == ThreadStatus::RUNNING) {
      std::this_thread::sleep_for(std::chrono::seconds(1));

      auto futureIt = cancelFutures_.begin();
      while (futureIt != cancelFutures_.end()) {
        if (futureIt->second.isReady()) {
          LOG(INFO) << "Task with attempt ID " << futureIt->first << " has been cancelled.";
          futureIt = cancelFutures_.erase(futureIt); // Remove completed future
        } else {
          futureIt++;
        }
      }

      std::lock_guard<std::mutex> guard(lock_);
      auto it = tasks_.begin();
      while (it != tasks_.end()) {
        auto task = it->second.boltTask.lock();
        if (!task) {
          LOG(ERROR) << "Task with attempt ID " << it->first << " has expired.";
          deleteTaskContextRef(env, it->second.jTaskContext);
          it = tasks_.erase(it); // Remove expired task
        } else if (!task->isRunning()) {
          LOG(INFO) << "Task with attempt ID " << it->first << " is not running.";
          deleteTaskContextRef(env, it->second.jTaskContext);
          it = tasks_.erase(it); // Remove completed task
        } else {
          auto taskContext = it->second.jTaskContext;

          jobject opt = env->CallObjectMethod(taskContext, getKillReasonMid);
          check(env, opt, "Failed to call TaskContext.getKillReason");

          jboolean defined = env->CallBooleanMethod(opt, isDefinedMid);
          checkException(env);

          if (defined) {
            // task is interrupted, we need to cancel it
            jobject valObj = env->CallObjectMethod(opt, optionGetMid);
            check(env, valObj, "Failed to call Option.get");

            if (env->IsInstanceOf(valObj, stringCls)) {
              jstring jstr = static_cast<jstring>(valObj);
              LOG(INFO) << "task " << it->first << " killed with reason: " << jStringToCString(env, jstr);
            } else {
              GLUTEN_CHECK(false, "Expected kill reason to be a String");
            }

            cancelFutures_[it->first] = task->requestCancel();
            deleteTaskContextRef(env, it->second.jTaskContext);
            it = tasks_.erase(it); // Remove task after cancellation
            continue;
          } else {
            ++it;
          }
        }
      }
    }
    LOG(INFO) << "TaskStatusListener has stopped listening for task status changes.";
  } catch (const std::exception& e) {
    LOG(ERROR) << "Exception in TaskStatusListener: " << e.what();
  } catch (...) {
    LOG(ERROR) << "Unknown exception in TaskStatusListener";
  }
  LOG(INFO) << "finish TaskStatusListener";
}

TaskStatusListener::~TaskStatusListener() {
  status_.store(ThreadStatus::STOPPED, std::memory_order_release);
  if (listenerThread_.joinable()) {
    listenerThread_.join();
    LOG(INFO) << "TaskStatusListener thread has finished.";
  }
}

void TaskStatusListener::startListener() {
  if (listenerThread_.joinable()) {
    return;
  }
  LOG(INFO) << "Starting TaskStatusListener thread.";
  status_.store(ThreadStatus::RUNNING, std::memory_order_release);
  listenerThread_ = std::thread([this]() { this->listen(); });
}

} // namespace gluten