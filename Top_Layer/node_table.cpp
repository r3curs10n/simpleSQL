#include <iostream>
using namespace std;
#include "node_table.h"
#include "includes.h"

extern file_and_access_layer faal;

node_table::node_table(string tabname){
	table_name = tabname;
	table_open = false;
	my_record_type_generated = false;

	left = right = NULL;
	it = NULL;
	//cond = NULL;
	level = 0;
	/*
	shyamal.open(table_name);
	eor = shyamal.eor;
	*/

}

void node_table::refresh(){
	// shyamal.refresh()
	run();
	//throw string("unimplemented refresh in node_table");
}

node::node_type node_table::type(){
	return node::NODE_TABLE;
}

void node_table::run(){
	/*for (list<condition*>::iterator i = cond.conditions.begin(); i!=cond.conditions.end(); i++){
		if ((*i)->type() == condition::OP_CONST){
			condition_op_const* c = dynamic_cast<condition_op_const*>(*i);
			cout<<c->lhs_table_name<<"."<<c->lhs_column_name<<" "<<c->op<<" "<<c->value.vInt<<endl;
		}
	}
	*/

	//decide which index to use
	for (list<condition*>::iterator i = cond.conditions.begin(); i != cond.conditions.end(); i++){
		if ((*i)->type() != condition::OP_CONST) continue;
		condition_op_const* c = dynamic_cast<condition_op_const*>(*i);
		if (faal.is_indexed(c->lhs_table_name, c->lhs_column_name)){
			it = faal.search_from_index( *c );
			cout<<"Found Index on "<<c->lhs_column_name<<endl;
			getRecordType();
			return;
		}
		
	}

	//no suitable index found
	
	it = faal.scan_table(table_name.c_str());
	
	getRecordType();

}

int node_table::getNextRecord(record& r){

	
	int record_id;
	while ((record_id = it->get_next(r)) >= 0){
		if (cond.eval(my_record_type, r)) {proj.project(r); return record_id;}
	}

	return -1;
	

	//throw string("unimplemented getNextRecord in node_table");

}

record_type node_table::getRecordType(){

	if (my_record_type_generated) return my_record_type;

	my_record_type_generated = true;
	faal.get_schema(table_name.c_str(), my_record_type);
	proj.input_record_type = my_record_type;
	proj.setup();
	return my_record_type = proj.getOutputRecordType();

	//throw string("unimplemented getRecordType in node_table");
}