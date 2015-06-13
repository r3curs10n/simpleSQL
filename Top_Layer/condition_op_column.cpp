#include "condition_op_column.h"
#include <string.h>

#define AS_INT(x) (*(int*)(&(x)))
#define AS_FLOAT(x) (*(float*)(&(x)))
#define AS_STRING(x) ((const char*)(&(x)))

condition::condition_type condition_op_column::type(){
	return condition::OP_COLUMN;
}

condition_op_column::condition_op_column(){
	l_offset= r_offset=-1;
	l_index= r_index= -1;
}

bool condition_op_column::eval(record_type& rt, record& r){

	if (l_offset==-1){
		l_offset = offset_of_column(rt, lhs_table_name, lhs_column_name);
		if (l_offset==-1) throw string("Undefined column");
	}

	if (l_index==-1){
		l_index = index_of_column(rt, lhs_table_name, lhs_column_name);
		if (l_index==-1) throw string("Undefined column");
	}

	if (r_offset==-1){
		r_offset = offset_of_column(rt, rhs_table_name, rhs_column_name);
		if (r_offset==-1) throw string("Undefined column");
	}
	if (r_index==-1){
		r_index = index_of_column(rt, rhs_table_name, rhs_column_name);
		if (r_index==-1) throw string("Undefined column");
	}

	if ((rt[l_index].type == column_type::INT) && (rt[r_index].type == column_type::INT)){

		if (op=="<"){
			return AS_INT(r[l_offset]) < AS_INT(r[r_offset]);
		} else if (op=="="){
			return AS_INT(r[l_offset]) == AS_INT(r[r_offset]);
		} else if (op==">"){
			return AS_INT(r[l_offset]) > AS_INT(r[r_offset]);
		}

		throw string("unimplemented operators in condition_op_column");
	}

	else if ((rt[l_index].type == column_type::FLOAT) && (rt[r_index].type == column_type::FLOAT)){

		if (op=="<"){
			return AS_FLOAT(r[l_offset]) < AS_FLOAT(r[r_offset]);
		} else if (op=="="){
			return AS_FLOAT(r[l_offset]) == AS_FLOAT(r[r_offset]);
		} else if (op==">"){
			return AS_FLOAT(r[l_offset]) > AS_FLOAT(r[r_offset]);
		}

		throw string("unimplemented operators in condition_op_column");
	}
	
	else if ((rt[l_index].type == column_type::STRING) && (rt[r_index].type == column_type::STRING)){

		if (op=="<"){
			return (strncmp(AS_STRING(r[l_offset]), AS_STRING(r[r_offset]), 500) < 0);
		} else if (op=="="){
			return (strncmp(AS_STRING(r[l_offset]), AS_STRING(r[r_offset]), 500) == 0);
		} else if (op==">"){
			return (strncmp(AS_STRING(r[l_offset]), AS_STRING(r[r_offset]), 500) > 0);
		}

		throw string("unimplemented operators in condition_op_column");
	}
	
}