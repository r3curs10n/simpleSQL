#ifndef H_CONDITION
#define H_CONDITION

#include <set>
using namespace std;

#include "record_type.h"

class condition {

public:

enum condition_type{
	OP_COLUMN, OP_CONST, AND, OR
};

	set<string> table_list;

	virtual condition_type type() = 0;

	virtual bool eval(record_type& rt, record& r) = 0;

	virtual void destroy();

};

#endif