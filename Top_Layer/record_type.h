#ifndef H_RECORD_TYPE
#define H_RECORD_TYPE

#include <string>
#include <vector>
using namespace std;

struct column_type {

enum data_type{
	INT, FLOAT, STRING
};
	string table_name, column_name;
	data_type type;
	int size;
};

typedef struct updateInfo{
	string column_name;
	column_type::data_type type;
	void* ptr_data;
}updateInfo;

typedef vector<column_type> record_type;
typedef vector<char> record;

inline int offset_of_column(record_type& rt, string& table_name, string& column_name){
	int offset = 0;
	for (vector<column_type>::iterator i = rt.begin(); i!=rt.end(); i++){
		if (table_name == i->table_name && column_name == i->column_name) return offset;
		offset += i->size;
	}
	return -1;
}

inline int index_of_column(record_type& rt, string& table_name, string& column_name){
	int index = 0;
	for (vector<column_type>::iterator i = rt.begin(); i!=rt.end(); i++){
		if (table_name == i->table_name && column_name == i->column_name) return index;
		index++;
	}
	return -1;
}

inline int record_size(record_type& rt){
	int size = 0;
	for (record_type::iterator i = rt.begin(); i != rt.end(); i++){
		size += i->size;
	}
	return size;
}

inline void join_record_types(record_type& l, record_type& r, record_type& dest){
	dest.clear();

	for (record_type::iterator i = l.begin(); i != l.end(); i++){
		dest.push_back(*i);
	}
	for (record_type::iterator i = r.begin(); i != r.end(); i++){
		dest.push_back(*i);
	}
}

inline void join_records(record& l, record& r, record& dest){
	dest.clear();

	for (record::iterator i = l.begin(); i != l.end(); i++){
		dest.push_back(*i);
	}
	for (record::iterator i = r.begin(); i != r.end(); i++){
		dest.push_back(*i);
	}
}

#endif