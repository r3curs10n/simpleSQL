#ifndef H_NODE_JOIN
#define H_NODE_JOIN

#include "node.h"
#include "condition_op_const.h"
#include "condition_op_column.h"
#include "../File_and_Access_Layer/file_and_access_layer.hpp"

class node_join : public node {

private:
	record lhs_cur_record;
	bool lhs_cur_record_present;

	bool can_optimize;

public:
	int getNextRecord(record& r);
	record_type getRecordType();

	table_scanner* it;

	
	void refresh();

	bool check_possible_optimization();
	condition_op_const index_condition;
	int getNextRecord_optimised(record& r);
	node::node_type type();
	string lhs_table;
	string lhs_column;

	void run();

	node_join();
};

#endif