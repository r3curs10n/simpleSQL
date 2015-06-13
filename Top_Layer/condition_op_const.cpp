#include "condition_op_const.h"
#include <string.h>

#define AS_INT(x) (*(int*)(&(x)))
#define AS_FLOAT(x) (*(float*)(&(x)))
#define AS_STRING(x) ((char*)(&(x)))

condition::condition_type condition_op_const::type(){
	return condition::OP_CONST;
}

condition_op_const::condition_op_const(){
	offset=-1;
	index=-1;
}

bool condition_op_const::eval(record_type& rt, record& r){
	if (offset==-1){
		offset = offset_of_column(rt, lhs_table_name, lhs_column_name);
		if (offset==-1) throw string("Undefined column: ") + string(lhs_column_name);
	}
	if (index==-1){
		index = index_of_column(rt, lhs_table_name, lhs_column_name);
		if (index==-1) throw string("Undefined column");
	}

	if (rt[index].type == column_type::INT){

		if (op=="<"){
			return AS_INT(r[offset]) < value.vInt;
		} else if (op=="="){
			return AS_INT(r[offset]) == value.vInt;
		} else if (op==">"){
			return AS_INT(r[offset]) > value.vInt;
		}

		throw string("unimplemented operators in condition_op_const");
	}
	else if(rt[index].type == column_type::FLOAT){

		if (op=="<"){
			return AS_FLOAT(r[offset]) < value.vFloat;
		} else if (op=="="){
			return AS_FLOAT(r[offset]) == value.vFloat;
		} else if (op==">"){
			return AS_FLOAT(r[offset]) > value.vFloat;
		}
		throw string("unimplemented operators in condition_op_const");
	}
	else if(rt[index].type == column_type::STRING){

		if (op=="<"){
			return (strncmp(AS_STRING(r[offset]), value.vString, 500) < 0);
		} else if (op=="="){
			return (strncmp(AS_STRING(r[offset]), value.vString, 500) == 0);
		} else if (op==">"){
			return (strncmp(AS_STRING(r[offset]), value.vString, 500) > 0);
		}

		throw string("unimplemented operators in condition_op_const");
	}
	//throw string("unimplemented datatypes in condition_op_const");

}