#ifndef H_NODE
#define H_NODE

#define SHREYAS

#include <set>
using namespace std;

#include "record_type.h"
#include "condition_list.h"
#include "projection.h"


class node {

public:
	enum node_type {
		NODE_JOIN, NODE_PROJECTION, NODE_CONDITIION, NODE_TABLE
	};

protected:
	record_type my_record_type;
	bool my_record_type_generated;

public:
	int level;
	node* left, *right;
	condition_list cond;
	projection proj;

	set<string> table_list;

	bool eor;	//end of records

	void unroll_conditions();
	void propogate_conditions();

	virtual node_type type() = 0;

	virtual record_type getRecordType() = 0;
	virtual int getNextRecord(record&) = 0;

	virtual void refresh() = 0;

	void node_setup();
	node();

	virtual void run();

	virtual void destroy();

};

#endif