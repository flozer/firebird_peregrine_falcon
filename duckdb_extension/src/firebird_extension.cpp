#include "duckdb.hpp"
#include "duckdb/main/extension_util.hpp"
#include "firebird_extension.hpp"

// Forward declaration of the function we will implement in Step 3
// This links the entry point to your actual scanning logic
duckdb::TableFunction FirebirdScanFunction();

using namespace duckdb;

// This is the entry point DuckDB calls when you run LOAD
void FirebirdExtension::Load(DatabaseInstance &db) {
    // Register our custom table function with the database instance
    ExtensionUtil::RegisterFunction(db, FirebirdScanFunction());
}

std::string FirebirdExtension::Name() {
    return "firebird_peregrine_falcon";
}

} // namespace duckdb
