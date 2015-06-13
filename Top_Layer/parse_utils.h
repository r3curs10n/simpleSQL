#include <time.h>
#include <string>
#include <cstring>
#include "record_type.h"
#include "../File_and_Access_Layer/file_and_access_layer.hpp"

#define AS_INT(x) (*(int*)(&(x)))
#define AS_FLOAT(x) (*(float*)(&(x)))
#define AS_STRING(x) ((char*)(&(x)))

using namespace std;

file_and_access_layer faal;

pair<string, string> split_full_column_name(char* full_column_name){
	char* table_name = strtok(full_column_name, ".");
	char* column_name = strtok(NULL, ".");
	return make_pair(string(table_name), string(column_name));
}

void update_rows(string& table_name, vector<updateInfo>& update_v, condition* where_clause){
	node_table* n = new node_table(table_name);
	int record_id;
	record r;
	if(where_clause)
		n->cond.conditions.push_back(where_clause);
	n->unroll_conditions();
	
	n->run();

	while(true){
		record_id = n->getNextRecord(r);
	 	if(record_id<0) break;


 		faal.update_table(table_name.c_str(), record_id, update_v);

	}
	n->destroy();
}

void delete_rows(string& table_name,condition* where_clause){
	node_table* n = new node_table(table_name);
	int record_id;
	record r;
	if(where_clause)
		n->cond.conditions.push_back(where_clause);
	n->unroll_conditions();

	
	n->run();

	while(true){
		record_id = n->getNextRecord(r);
	 	if(record_id<0) break;
	 	//cout<<"delete called"<<endl;
	 	faal.delete_record(table_name.c_str(),record_id);
	}
	n->destroy();
}

inline void insert_row(string& table_name, vector<pair<column_type::data_type, void*> >& insert_vals){
	
	//cout <<"insert size:" <<insert_vals.size() <<endl;
	faal.insert_into_table(table_name.c_str(), insert_vals);
	
}

void create_database(string& db_name){
	faal.create_database(db_name.c_str());
}

void use_database(string& db_name){
	faal.use_database(db_name.c_str());
}

void create_table_func(string& table_name, record_type& schema){
	faal.create_table(table_name.c_str(), schema);
}
void drop_database(string& drop_dbase){

}
void drop_table_func(string& db_name){

}

void create_index_func(string& index_table,string& index_column){
	faal.create_index(index_table.c_str(),index_column.c_str());
}
void drop_index_func(string& index_table,string &index_column){
	faal.drop_index(index_table.c_str(),index_column.c_str());
}

void print_record(record_type& rt, record& r){
	int offset=0;
	for (record_type::iterator i = rt.begin(); i != rt.end(); i++){
		if (i->type == column_type::INT){
			cout<<AS_INT(r[offset])<<"\t\t";
		}
		else if (i->type == column_type::FLOAT){
			cout<<AS_FLOAT(r[offset])<<"\t\t";
		}
		else if (i->type == column_type::STRING){
			cout<<string(AS_STRING(r[offset]))<<"\t\t";
		}
		offset += i->size;
	}
	cout<<endl;
}

void select_rows(node* select_root, condition* where_clause, vector<pair<string, string> >& column_list){
	
	clock_t start, end;
    double cpu_time_used;
    start = clock();

	if (where_clause) select_root->cond.conditions.push_back(where_clause);
	select_root->unroll_conditions();

	select_root->propogate_conditions();
	cout<<"Records Returned:"<<endl;

	//select_root->proj.columns = column_list;
	//select_root->proj.enabled = true;
	select_root->run();

	record_type rt, rtp;
	rt = select_root->getRecordType();

	//projection p;
	//p.enabled = true;
	//p.columns = column_list;
	//p.input_record_type = rt;
	//p.setup();
	//rtp = p.output_record_type;
	record r;

	for (record_type::iterator i = rt.begin(); i!=rt.end(); i++){
		cout<<(*i).table_name<<"."<<(*i).column_name<<"\t\t";
	}
	cout<<endl;

	while (select_root->getNextRecord(r) >= 0){
		//p.project(r);
		print_record(rt, r);
	}

	select_root->destroy();

	end = clock();
	cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;
	cout<<"Time taken: "<<cpu_time_used<<endl;
}
