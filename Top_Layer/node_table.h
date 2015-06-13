#ifndef H_NODE_TABLE
#define H_NODE_TABLE

#include "node.h"
#include "../File_and_Access_Layer/file_and_access_layer.hpp"
#include <string>
using namespace std;

class node_table : public node {

protected:
	
	bool table_open;
	table_scanner* it;

public:

	string table_name;

	record_type getRecordType();
	int getNextRecord(record& r);
	void refresh();

	void run();

	node::node_type type ();

	node_table(string tabname);

};

#endif