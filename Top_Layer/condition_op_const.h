#ifndef H_CONDITION_OP_CONST
#define H_CONDITION_OP_CONST

#include "record_type.h"
#include "condition.h"
#include <string>

class condition_op_const : public condition {

public:
	string lhs_table_name;
	string lhs_column_name;
	string op;
	int offset, index;
	union {
		int vInt;
		float vFloat;
		char vString[500];
	} value;

	bool eval(record_type& rt, record& r);

	condition::condition_type type();

	condition_op_const();

};

#endif