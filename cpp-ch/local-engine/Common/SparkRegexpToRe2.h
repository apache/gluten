/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); You may not use this file except in compliance with
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
#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <Common/Exception.h>

namespace DB::ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace local_engine::spark_regexp::re2
{
namespace detail
{
struct Class;
using ClassPtr = std::shared_ptr<Class>;

struct Part
{
    enum Kind
    {
        TEXT,
        NESTED
    } kind;
    std::string text;
    ClassPtr nested;
};

struct Class
{
    bool negated = false;
    std::vector<Part> parts;
};

// Keep top-level constructs distinct from their RE2 rendering. In particular,
// a future Group/FlagScope node can own a sequence of these nodes without
// changing how character classes, dots, and anchors are lowered.
enum class PatternNodeKind
{
    Literal,
    Escape,
    CharacterClass,
    Dot,
    EndAnchor,
    DotAllFlag,
    GroupOpen,
    GroupClose
};

struct PatternNode
{
    PatternNodeKind kind;
    std::string text;
    ClassPtr characterClass;
    // Reserved for recursive group/flag-scope parsing. Top-level groups are
    // currently represented by GroupOpen/GroupClose, not nested here.
    std::vector<PatternNode> children;
    // For DotAllFlag and flagged GroupOpen only; the RE2 flag is not emitted.
    std::optional<bool> dotAll;
};

class Translator
{
public:
    explicit Translator(const std::string & source) : source_(source) { }

    std::string run() { return lower(parsePattern()); }

private:
    const std::string & source_;
    size_t position_ = 0;

    // Phase 1: identify Java regexp constructs without lowering them.
    // The group boundaries and dot-all directives carry just enough structure
    // for lower() to track their scope; more complex flags need group AST nodes.
    std::vector<PatternNode> parsePattern()
    {
        std::vector<PatternNode> nodes;
        while (position_ < source_.size())
        {
            const char c = source_[position_];
            if (c == '\\')
            {
                nodes.push_back({PatternNodeKind::Escape, copyEscape(), nullptr, {}, std::nullopt});
                continue;
            }
            if (c == '[')
            {
                nodes.push_back({PatternNodeKind::CharacterClass, {}, parseClass(), {}, std::nullopt});
                continue;
            }
            if (c == '(')
            {
                // Java allows both scoped (?s:...) and unscoped (?s) DOTALL.
                // The unscoped form lasts until the end of the enclosing group.
                if (source_.compare(position_, 4, "(?s)") == 0 || source_.compare(position_, 5, "(?-s)") == 0)
                {
                    const bool enable = source_[position_ + 2] == 's';
                    nodes.push_back({PatternNodeKind::DotAllFlag, {}, nullptr, {}, enable});
                    position_ += enable ? 4 : 5;
                    continue;
                }
                if (source_.compare(position_, 4, "(?s:") == 0 || source_.compare(position_, 5, "(?-s:") == 0)
                {
                    const bool enable = source_[position_ + 2] == 's';
                    nodes.push_back({PatternNodeKind::GroupOpen, "(?:", nullptr, {}, enable});
                    position_ += enable ? 4 : 5;
                    continue;
                }
                validateGroupPrefix();
            }
            if ((c == '*' || c == '+' || c == '?') && position_ + 1 < source_.size() && source_[position_ + 1] == '+')
                fail("possessive quantifiers are unsupported");

            PatternNodeKind kind = PatternNodeKind::Literal;
            if (c == '$')
                kind = PatternNodeKind::EndAnchor;
            else if (c == '.')
                kind = PatternNodeKind::Dot;
            else if (c == '(')
                kind = PatternNodeKind::GroupOpen;
            else if (c == ')')
                kind = PatternNodeKind::GroupClose;
            nodes.push_back({kind, std::string(1, c), nullptr, {}, std::nullopt});
            ++position_;
        }
        return nodes;
    }

    void validateGroupPrefix() const
    {
        if (position_ + 2 >= source_.size() || source_[position_ + 1] != '?')
            return;
        if (source_[position_ + 2] == '=' || source_[position_ + 2] == '!' || source_[position_ + 2] == '<' || source_[position_ + 2] == '>')
            fail("lookaround, named captures and atomic groups are unsupported");
        if (source_[position_ + 2] == ':')
            return;
        const size_t end = source_.find_first_of(":)", position_ + 2);
        if (end == std::string::npos)
            fail("unterminated inline flag group");
        const auto flags = source_.substr(position_ + 2, end - position_ - 2);
        if (flags != "i" && flags != "-i")
            fail("inline flags other than case-insensitive matching require explicit translation");
    }

    // Phase 2: render the supported Java constructs as RE2. New translations
    // belong in this switch, not in the scanner or individual function parsers.
    std::string lower(const std::vector<PatternNode> & nodes)
    {
        std::string out;
        bool dotAll = false;
        std::vector<bool> groupDotAll;
        for (const auto & node : nodes)
        {
            switch (node.kind)
            {
                case PatternNodeKind::CharacterClass:
                    out += render(node.characterClass);
                    break;
                case PatternNodeKind::Dot:
                    // Java's default dot excludes Unicode line terminators; DOTALL includes them.
                    out += dotAll ? "(?s:.)" : "[^\\r\\n\\x{85}\\x{2028}\\x{2029}]";
                    break;
                case PatternNodeKind::DotAllFlag:
                    dotAll = *node.dotAll;
                    break;
                case PatternNodeKind::EndAnchor:
                    // RE2 cannot express the exact CRLF boundary without lookbehind:
                    // the LF alternative also matches between a final CR and LF.
                    out += "(?:$|\\r?\\n\\z|\\r\\z|\\x{85}\\z|\\x{2028}\\z|\\x{2029}\\z)";
                    break;
                case PatternNodeKind::GroupOpen:
                    groupDotAll.push_back(dotAll);
                    if (node.dotAll)
                        dotAll = *node.dotAll;
                    out += node.text;
                    break;
                case PatternNodeKind::GroupClose:
                    if (!groupDotAll.empty())
                    {
                        dotAll = groupDotAll.back();
                        groupDotAll.pop_back();
                    }
                    out += node.text;
                    break;
                case PatternNodeKind::Literal:
                case PatternNodeKind::Escape:
                    out += node.text;
                    break;
            }
        }
        return out;
    }

    [[noreturn]] void fail(const std::string & msg) const
    {
        throw DB::Exception(DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Cannot translate Spark regexp to RE2: {}", msg);
    }

    std::string copyEscape()
    {
        // source_[position_] == '\\'
        if (position_ + 1 < source_.size() && source_[position_ + 1] == 'Q')
        {
            size_t end = source_.find("\\E", position_ + 2);
            if (end == std::string::npos)
            {
                std::string r = source_.substr(position_);
                position_ = source_.size();
                return r;
            }
            std::string r = source_.substr(position_, end + 2 - position_);
            position_ = end + 2;
            return r;
        }
        if (position_ + 1 >= source_.size())
            fail("dangling backslash");
        const char escaped = source_[position_ + 1];
        if (escaped >= '1' && escaped <= '9')
            fail("backreferences are unsupported by RE2");
        if (escaped == 'R' || escaped == 'X' || escaped == 'C')
            fail("Java regex escape is unsupported by RE2");
        if (escaped == 'Z')
        {
            position_ += 2;
            return "(?:\\z|\\r?\\n\\z|\\r\\z|\\x{85}\\z|\\x{2028}\\z|\\x{2029}\\z)";
        }
        std::string r = source_.substr(position_, 2);
        position_ += 2;
        return r;
    }

    ClassPtr parseClass()
    {
        auto c = std::make_shared<Class>();
        ++position_; // '['
        if (position_ < source_.size() && source_[position_] == '^')
        {
            c->negated = true;
            ++position_;
        }

        bool first = true;
        std::string text;
        auto flush = [&] {
            if (!text.empty())
            {
                c->parts.push_back(Part{Part::TEXT, text, nullptr});
                text.clear();
            }
        };

        while (true)
        {
            if (position_ >= source_.size())
                fail("unterminated character class");
            char ch = source_[position_];
            if (ch == '\\')
            {
                if (position_ + 1 < source_.size() && source_[position_ + 1] == 'Q')
                {
                    size_t end = source_.find("\\E", position_ + 2);
                    if (end == std::string::npos)
                        fail("unterminated quoted literal in character class");
                    for (size_t q = position_ + 2; q < end; ++q)
                    {
                        const char literal = source_[q];
                        if (literal == ']' || literal == '[' || literal == '-' || literal == '\\' || literal == '^')
                            text += '\\';
                        text += literal;
                    }
                    position_ = end + 2;
                    first = false;
                    continue;
                }
                if (position_ + 1 >= source_.size())
                    fail("dangling backslash in class");
                text += source_[position_];
                text += source_[position_ + 1];
                position_ += 2;
                first = false;
                continue;
            }
            if (ch == '[')
            {
                flush();
                c->parts.push_back(Part{Part::NESTED, "", parseClass()});
                first = false;
                continue;
            }
            if (ch == '&' && position_ + 1 < source_.size() && source_[position_ + 1] == '&')
                fail("character-class intersection (&&) is not supported");
            if (ch == ']')
            {
                if (first)
                {
                    text += ']';
                    ++position_;
                    first = false;
                    continue;
                }
                ++position_;
                flush();
                return c;
            }
            text += ch;
            ++position_;
            first = false;
        }
    }

    static bool isEscapedAt(const std::string & s, size_t idx)
    {
        size_t n = 0;
        while (idx > n && s[idx - 1 - n] == '\\')
            ++n;
        return n % 2 == 1;
    }

    // Escape a '-' / ']' that is literal because of its position in the original class.
    static std::string escapeBoundaries(std::string t, bool frontDash, bool frontRightBracket, bool backDash)
    {
        if (t.empty())
            return t;
        const bool fd = frontDash && t.front() == '-';
        const bool fr = frontRightBracket && t.front() == ']';
        const bool bd = backDash && t.back() == '-' && !isEscapedAt(t, t.size() - 1);
        if (t.size() == 1)
            return (fd || fr || bd) ? "\\" + t : t;
        std::string r;
        if (fd || fr)
            r += '\\';
        r += t;
        if (bd)
            r.insert(r.size() - 1, "\\");
        return r;
    }

    // Concatenate the parts of a non-negated class into a single class body.
    std::string bodyOf(const ClassPtr & c)
    {
        std::string body;
        for (size_t k = 0; k < c->parts.size(); ++k)
        {
            const auto & p = c->parts[k];
            if (p.kind == Part::TEXT)
                body += escapeBoundaries(p.text, k > 0, false, k + 1 < c->parts.size());
            else
            {
                if (p.nested->negated)
                    fail("nested negated class inside a spliced class is not supported");
                body += escapeBoundaries(bodyOf(p.nested), !body.empty(), !body.empty(), true);
            }
        }
        return body;
    }

    std::string render(const ClassPtr & c)
    {
        if (c->negated)
            return "[^" + bodyOf(c) + "]";
        if (c->parts.size() == 1 && c->parts[0].kind == Part::TEXT)
            return "[" + c->parts[0].text + "]";

        std::string out = "(?:";
        bool firstPart = true;
        for (auto & p : c->parts)
        {
            if (!firstPart)
                out += "|";
            firstPart = false;
            if (p.kind == Part::TEXT)
                out += "[" + p.text + "]";
            else
                out += render(p.nested);
        }
        out += ")";
        return out;
    }
};

}

/// Translate the supported Spark/Java pattern subset to RE2, preserving capture indices.
/// Unsupported constructs are rejected instead of being silently misinterpreted.
inline std::string translatePattern(const std::string & pattern)
{
    return detail::Translator(pattern).run();
}
}
