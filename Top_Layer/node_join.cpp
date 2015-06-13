#include "node_join.h"
#include <cassert>
#include "record_type.h"
#include "node_table.h"
#include "../File_and_Access_Layer/file_and_access_layer.hpp"
#include <string.h>

#define AS_INT(x) (*(int*)(&(x)))
#define AS_FLOAT(x) (*(float*)(&(x)))
#define AS_STRING(x) ((char*)(&(x)))

extern file_and_access_layer faal;

node_join::node_join(){

	node_setup();
	lhs_cur_record_present = false;
	can_optimize = false;
}

void node_join::run(){
	node::run();

	//decide if optimization is possible
	if (check_possible_optimization()){
		can_optimize=true;
		cout<<"index found"<<endl;
		for (list<condition*>::iterator i = right->cond.conditions.begin(); i!= right->cond.conditions.end(); i++){
			cond.conditions.push_back(*i);
		}
		//it = faal.get_index(index_condition);
	}
	else
	can_optimize = false;

	getRecordType();


}

bool node_join::check_possible_optimization(){

	for(list<condition*>::iterator i= cond.conditions.begin(); i!=cond.conditions.end();i++){
		if((*i)->type()!= condition::OP_COLUMN) continue;
		condition_op_column* c = dynamic_cast <condition_op_column*> (*i);
		if(c->op != "=") continue;
		node_table* tright = dynamic_cast<node_table*>(right);
		if(c->lhs_table_name == tright->table_name){
			if(faal.is_indexed((tright->table_name),(c->lhs_column_name))){
				
				index_condition.lhs_column_name = c->lhs_column_name;
				index_condition.lhs_table_name = tright->table_name;
				index_condition.op = "="; 
				lhs_table= c->rhs_table_name;
				lhs_column=  c-> rhs_column_name;
				return true;
			}
		}

		else if(c->rhs_table_name == tright->table_name){
			if(faal.is_indexed((tright->table_name),(c->rhs_column_name))){
				
				index_condition.lhs_column_name = c->rhs_column_name;
				index_condition.lhs_table_name = tright->table_name;
				index_condition.op = "="; 
				lhs_table= c->lhs_table_name;
				lhs_column=  c-> lhs_column_name;
				return true;
			}
		}

	}


	for(list<condition*>::iterator i= cond.conditions.begin(); i!=cond.conditions.end();i++){
		if((*i)->type()!= condition::OP_COLUMN) continue;
		condition_op_column* c = dynamic_cast <condition_op_column*> (*i);
		if(c->op == "=") continue;
		node_table* tright = dynamic_cast<node_table*>(right);
		if(c->lhs_table_name == tright->table_name){
			if(faal.is_indexed((tright->table_name),(c->lhs_column_name))){
				
				index_condition.lhs_column_name = c->lhs_column_name;
				index_condition.lhs_table_name = tright->table_name;
				index_condition.op = c->op;
				lhs_table= c->rhs_table_name;
				lhs_column=  c-> rhs_column_name;
				return true;
			}
		}

		else if(c->rhs_table_name == tright->table_name){
			if(faal.is_indexed((tright->table_name),(c->rhs_column_name))){
				
				index_condition.lhs_column_name = c->rhs_column_name;
				index_condition.lhs_table_name = tright->table_name; 
				index_condition.op = (c->op == "<")?">":"<";
				lhs_table= c->lhs_table_name;
				lhs_column=  c-> lhs_column_name;
				return true;
			}
		}

	}

	return false;

}

void node_join::refresh(){
	lhs_cur_record_present = false;

	assert(left && right);

	run();

	left->refresh();
	right->refresh();
}

record_type node_join::getRecordType(){
	if (my_record_type_generated) return my_record_type;

	assert(left && right);

	record_type l, r;
	l = left->getRecordType();
	r = right->getRecordType();

	join_record_types(l, r, my_record_type);
	my_record_type_generated=true;

	proj.input_record_type = my_record_type;
	proj.setup();
	return my_record_type = proj.getOutputRecordType();
}

/*int node_join::optimizedGetNextRecord(record& r){

}

int node_join::trivialGetNextRecord(record& r){

}
*/


int node_join::getNextRecord(record& r){

	assert(left && right);

	if(can_optimize){
		return getNextRecord_optimised(r);
	}
	while (true){
	int record_id;

	record y;

	if (!lhs_cur_record_present){

		record_id = left->getNextRecord(lhs_cur_record);
		if (record_id < 0) return -1;
		lhs_cur_record_present=true;
	}

	if (right->getNextRecord(y) >= 0){
		join_records(lhs_cur_record, y, r);
	}
	else {
		while (true){
			record_id = left->getNextRecord(lhs_cur_record);
			right->refresh();
			if (record_id<0) return -1;
			if (right->getNextRecord(y) >=0){
				join_records(lhs_cur_record, y, r);
				break;
			}
		}
	}

	if (cond.eval(my_record_type, r)){
		proj.project(r);
		return 1;
	} else {
		//return getNextRecord(r);
	}
	}
	//TODO: eliminate tail recursion

}

int node_join::getNextRecord_optimised(record& r){


	int record_id, offset, index;
	record_type rt;
	record y;

	while (true){
	if (!lhs_cur_record_present){
		record_id = left->getNextRecord(lhs_cur_record);
		if (record_id < 0) return -1;
		lhs_cur_record_present=true;
	
		rt = left->getRecordType();
		offset = offset_of_column(rt, lhs_table, lhs_column);
		index = index_of_column(rt, lhs_table, lhs_column);
		//cout<<"-->"<<offset<<" "<<index<<endl;
		if (rt[index].type == column_type::INT){
			index_condition.value.vInt = AS_INT(lhs_cur_record[offset]);
		}
		else if (rt[index].type == column_type::FLOAT){
			index_condition.value.vFloat = AS_FLOAT(lhs_cur_record[offset]); 
		}
		else if (rt[index].type == column_type::STRING){
			strcpy(index_condition.value.vString, AS_STRING(lhs_cur_record[offset])); 
		}
		//index_condition.op = "=";
		//cout<<index_condition.lhs_table_name<<"."<<index_condition.lhs_column_name<<" "<<index_condition.op<<" "<<index_condition.value.vInt<<endl;
		it =faal.search_from_index(index_condition);

	}

	if (it->get_next(y) >= 0){
		join_records(lhs_cur_record, y, r);
	}
	else {
		while (true){
			//record_id = left->getNextRecord(lhs_cur_record);
			//if (record_id<0) return -1;

			record_id = left->getNextRecord(lhs_cur_record);
			if (record_id < 0) {lhs_cur_record_present=false; return -1;}
			//lhs_cur_record_present=true;
			rt = left->getRecordType();
			offset = offset_of_column(rt, lhs_table, lhs_column);
		index = index_of_column(rt, lhs_table, lhs_column);

			if (rt[index].type == column_type::INT){
				index_condition.value.vInt = AS_INT(lhs_cur_record[offset]); 
			}
			else if (rt[index].type == column_type::FLOAT){
				index_condition.value.vFloat = AS_FLOAT(lhs_cur_record[offset]); 
			}
			else if (rt[index].type == column_type::STRING){
				strcpy(index_condition.value.vString, AS_STRING(lhs_cur_record[offset])); 
			}
			//index_condition.op = "=";
			//cout<<index_condition.lhs_table_name<<"."<<index_condition.lhs_column_name<<" "<<index_condition.op<<" "<<index_condition.value.vInt<<endl;
			it =faal.search_from_index(index_condition);

			if (it->get_next(y) >=0){
				join_records(lhs_cur_record, y, r);
				break;
			}
		}
	}

	if (cond.eval(my_record_type, r)){
		proj.project(r);
		return 1;
	} else {
		//return getNextRecord_optimised(r);
	}

	}
}

node::node_type node_join::type(){
	return node::NODE_JOIN;
}