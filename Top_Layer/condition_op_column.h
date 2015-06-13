#ifndef H_CONDITION_OP_COLUMN
#define H_CONDITION_OP_COLUMN

#include "record_type.h"
#include "condition.h"
#include <string>

class condition_op_column : public condition {

public:
	string lhs_table_name, rhs_table_name;
	string lhs_column_name, rhs_column_name;
	string op;
	
	int l_offset, l_index;
	int r_offset, r_index;
	
	bool eval(record_type& rt, record& r);

	condition::condition_type type();

	condition_op_column();

};

#endif