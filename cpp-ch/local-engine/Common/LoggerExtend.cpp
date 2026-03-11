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
#include "LoggerExtend.h"
#include <Loggers/ExtendedLogMessage.h>
#include <Loggers/OwnSplitChannel.h>

#include <Loggers/Loggers.h>
#include <Poco/AsyncChannel.h>
#include <Poco/AutoPtr.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/FormattingChannel.h>
#include <Poco/PatternFormatter.h>

using Poco::AsyncChannel;
using Poco::AutoPtr;
using Poco::ConsoleChannel;
using Poco::FormattingChannel;
using Poco::PatternFormatter;

namespace local_engine
{
void LoggerExtend::initConsoleLogger(const std::string & level)
{
    AutoPtr<ConsoleChannel> chan(new ConsoleChannel);

    AutoPtr<PatternFormatter> formatter(new PatternFormatter);
    formatter->setProperty("pattern", "%Y-%m-%d %H:%M:%S.%i <%p> %s: %t");
    formatter->setProperty("times", "local");

    AutoPtr<FormattingChannel> format_channel(new FormattingChannel(formatter, chan));
    AutoPtr<AsyncChannel> async_chann(new AsyncChannel(format_channel));

    Poco::Logger::root().setChannel(async_chann);
    Poco::Logger::root().setLevel(level);
}

void LoggerExtend::initFileLogger(Poco::Util::AbstractConfiguration & config, const std::string & cmd_name)
{
    static Loggers loggers;
    loggers.buildLoggers(config, Poco::Logger::root(), cmd_name);
}
}
