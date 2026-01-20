#pragma once

#include "duckdb.hpp"

namespace duckdb {

class FirebirdExtension : public Extension {
public:
	void Load(DatabaseInstance &db) override;
	std::string Name() override;
};

} // namespace duckdb
