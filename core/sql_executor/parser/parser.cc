#include "parser.h"

#include "yacc.tab.h"
#include "lex.yy.hpp"
#include <iostream>
#include <stdexcept>

std::shared_ptr<ast::TreeNode> ParseSql(const char* sql) {
    yyscan_t scanner = nullptr;
    if (yylex_init(&scanner) != 0) throw std::bad_alloc();
    // Flex destroys the active buffer with the scanner, including on exceptions.
    std::unique_ptr<void, decltype(&yylex_destroy)> owner(scanner, yylex_destroy);
    yy_scan_string(sql, scanner);

    SqlParseResult result;
    if (yyparse(scanner, result) != 0 || !result.tree) {
        if (!result.error.empty()) std::cerr << result.error;
        throw std::logic_error("Syntax error");
    }
    return std::move(result.tree);
}
