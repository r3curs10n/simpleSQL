%{

#include <cstdio>
#include <cstring>
#include <string>
#include <iostream>
#include <cstdlib>
#include "../File_and_Access_Layer/file_and_access_layer.hpp"
#include "includes.h"
#include "parse_utils.h"
#include "set_utils.h"

using namespace std;

vector<pair<string, string> > columns;	//table.column pairs
vector<string> tables;	//list of tables

vector<pair<column_type::data_type, void*> > insert_values;

condition* where_clause;
node* select_root;
record_type schema;

vector <updateInfo> update_v;
string update_table;
string delete_table;
string insert_table;
string create_dbase;
string vcreate_table;
string vdrop_table;
string db_dbase;
string use_dbase;
string index_table, index_column;
string drop_dbase;

extern int yylex();
extern int yyparse();

enum action {
	A_SELECT, A_INSERT, A_UPDATE, A_DELETE, A_CREATE_DB, A_CREATE_TABLE, A_USE_DB, A_DROP_DB, A_DROP_TABLE, A_CREATE_INDEX, A_DROP_INDEX, A_NOTHING
};

action query_type;

void yyerror(const char* s);

%}

%union {
	int vInt;
	float vFloat;
	char vString[100];
	node* vNode;
	condition* vCondition;
}

%token<vInt> CONST_INT
%token<vFloat> CONST_FLOAT
%token<vString> CONST_STRING COLUMN_NAME TABLE_NAME OPERATOR
%token COMMA SEMI SELECT FROM WHERE AND OR LP RP VALUES INSERT INTO UPDATE SET DELETE DROP USE CREATE DATABASE INT FLOAT STRING INDEX TABLE
%left AND OR

%type<vCondition> simple_condition complex_condition
%type<vNode> table_list

%start sql

%%

sql: select {return 0;}
	| update {return 0;}
	| delete {return 0;}
	| insert {return 0;}
	| create_table {return 0;}
	| create_db {return 0;}
	| use_db {return 0;}
	| drop_db {return 0;}
	| drop_table {return 0;}
	| create_index {return 0;}
	| drop_index {return 0;}
	;

select: SELECT column_list FROM table_list optional_where SEMI {
			select_root = $4;
			query_type = A_SELECT;
		}
;

column_list: column_list COMMA COLUMN_NAME {
				columns.push_back(split_full_column_name($3));
			}
			| COLUMN_NAME {
				columns.push_back(split_full_column_name($1));
			}
;

table_list: table_list COMMA TABLE_NAME {
				tables.push_back(string($3));
				node_join* nj = new node_join();
				//cout<<"joined"<<endl;
				node_table* nt = new node_table($3);
				nt->table_list.insert(string($3));
				nj->left = $1;
				nj->right = nt;
				merge_sets(nj->table_list, nt->table_list);
				$$ = nj;
				//cout<<">"<<$$->type()<<endl;
			}
			| TABLE_NAME {
				tables.push_back(string($1));
				node_table* nt = new node_table($1);
				nt->table_list.insert(string($1));
				$$ = nt;
			}
;

optional_where: 
			| WHERE complex_condition {
				where_clause = $2;
			}
;

complex_condition: complex_condition AND complex_condition {
						condition_and *c = new condition_and();
						c->left = $1;
						c->right = $3;
						merge_sets(c->table_list, $1->table_list);
						merge_sets(c->table_list, $3->table_list);
						$$ = c;
						//cout<<"kjrljrel"<<endl;
				}
				| complex_condition OR complex_condition {
					condition_or *c = new condition_or();
					c->left = $1;
					c->right = $3;
					merge_sets(c->table_list, $1->table_list);
					merge_sets(c->table_list, $3->table_list);
					$$ = c;
				}
				| LP simple_condition RP {
					$$ = $2;
				}
				| simple_condition {
					$$ = $1;
				}
;

simple_condition: COLUMN_NAME OPERATOR COLUMN_NAME {
			pair<string,string> l_col = split_full_column_name($1);
			condition_op_column* c = new condition_op_column();
			c->lhs_table_name = l_col.first;
			c->lhs_column_name = l_col.second;
			
			c->op = string($2);
			
			pair<string,string> r_col = split_full_column_name($3);
			c->rhs_table_name = r_col.first;
			c->rhs_column_name = r_col.second;

			c->table_list.insert(l_col.first);
			c->table_list.insert(r_col.first);

			$$=c;
		}
		| COLUMN_NAME OPERATOR CONST_INT {
			pair<string, string> col = split_full_column_name($1);
			condition_op_const* c = new condition_op_const();
			c->lhs_table_name = col.first;
			c->lhs_column_name = col.second;
			c->op = string($2);
			c->value.vInt = $3;
			c->table_list.insert(col.first);
			$$ = c;
		}
		| COLUMN_NAME OPERATOR CONST_FLOAT {
			pair<string, string> col = split_full_column_name($1);
			condition_op_const* c = new condition_op_const();
			c->lhs_table_name = col.first;
			c->lhs_column_name = col.second;
			c->op = string($2);
			c->value.vFloat = $3;
			c->table_list.insert(col.first);
			$$ = c;
		}
		| COLUMN_NAME OPERATOR CONST_STRING {
			pair<string, string> col = split_full_column_name($1);
			condition_op_const* c = new condition_op_const();
			c->lhs_table_name = col.first;
			c->lhs_column_name = col.second;
			c->op = string($2);
			strncpy(c->value.vString, $3, 100);
			c->table_list.insert(col.first);
			$$ = c;
		}
;

insert: INSERT INTO TABLE_NAME VALUES LP values RP SEMI {insert_table = string($3); query_type = A_INSERT;}
;

values: values COMMA value
		| value
;

value: CONST_INT {
		int* x = (int*)malloc(sizeof(int));
		*x = $1;
		//cout<<"ooo"<<endl;
		insert_values.push_back(make_pair(column_type::INT, (void*)x));
	}
	| CONST_FLOAT {
		float* x = (float*)malloc(sizeof(float));
		*x = $1;
		insert_values.push_back(make_pair(column_type::FLOAT, (void*)x));
	}
	| CONST_STRING {
		char* x = (char*)malloc(101);
		strncpy(x, $1, 100);
		insert_values.push_back(make_pair(column_type::STRING, (void*)x));
	}
;

update: UPDATE TABLE_NAME SET set_values optional_where SEMI
		{ query_type = A_UPDATE;
			update_table= string($2);
			//cout<<"---ddd-->"<<endl;		
		};

set_values:	COLUMN_NAME OPERATOR CONST_INT more_set_values{
			
			int* x= (int*) malloc(sizeof(int));
			*x = $3;
			//cout<<"----->"<<endl;
			updateInfo u;
			u.type= column_type::INT;
			u.ptr_data = (void*)x;
			u.column_name = split_full_column_name($1).second;

			update_v.push_back(u);
		} 
		|	COLUMN_NAME OPERATOR CONST_FLOAT more_set_values{
			
			float* x= (float*)malloc(sizeof(float));
			*x = $3;
			
			updateInfo u;
			u.type= column_type::FLOAT;
			u.ptr_data = (void*)x;
			u.column_name = split_full_column_name($1).second;

			update_v.push_back(u);

		}
		|	COLUMN_NAME OPERATOR CONST_STRING more_set_values{
			
			char* x= (char*)malloc(101);
			strncpy(x, $3, 100);
						
			updateInfo u;
			u.type= column_type::STRING;
			u.ptr_data = (void*)x;
			u.column_name = split_full_column_name($1).second;

			update_v.push_back(u);
		}
		;

more_set_values : 
				| COMMA set_values
				;


delete:	DELETE FROM TABLE_NAME optional_where SEMI{
			
			query_type = A_DELETE;
			delete_table = string($3);	
		};

create_db:	CREATE DATABASE TABLE_NAME SEMI{
			query_type = A_CREATE_DB;
			create_dbase = string($3);	
		};

use_db	: USE DATABASE TABLE_NAME SEMI{
			query_type = A_USE_DB;
			use_dbase = string($3);
		};

create_table:	CREATE TABLE TABLE_NAME LP fields RP SEMI{
			query_type = A_CREATE_TABLE;
			vcreate_table = string($3);
		};

fields: more_fields TABLE_NAME INT{
		column_type c;
		c.type = column_type::INT;
		c.size = sizeof(int);
		//c.table_name = create_table;  // bottom-up parse!
		c.column_name = string($2);

		schema.push_back(c);			
		}
		| more_fields TABLE_NAME FLOAT {
			column_type c;
			c.type = column_type::FLOAT;
			c.size = sizeof(FLOAT);
			//c.table_name = create_table;  // bottom-up parse!
			c.column_name = string($2);

			schema.push_back(c);			
		}
		| more_fields TABLE_NAME STRING {
			column_type c;
			c.type = column_type::STRING;
			c.size = 100;
			//c.table_name = create_table;  // bottom-up parse!
			c.column_name = string($2);

			schema.push_back(c);			
		};

more_fields: 
		   | fields COMMA
		   ;

drop_db: DROP DATABASE TABLE_NAME SEMI{
		 query_type = A_DROP_DB;
		 drop_dbase = string($3);
		};

drop_table: DROP TABLE TABLE_NAME SEMI{
			query_type = A_DROP_TABLE;
			vdrop_table = string($3);
			};

create_index: CREATE INDEX TABLE_NAME TABLE_NAME SEMI{
				query_type = A_CREATE_INDEX;
				index_table = string($3);
				index_column = string($4);					
		};

drop_index: DROP INDEX TABLE_NAME TABLE_NAME SEMI{
			query_type = A_DROP_INDEX;
			index_table= string($3);
			index_column = string($4);
		};		
%%

int main(){

	select_root = NULL;
	where_clause = NULL;
	
	cout<< "\nSQL mein aapka SWAAGAT hai _/\\_ \n";
	cout <<"__________________________________\n\n"; 
	
	do {
		where_clause = NULL;
		select_root = NULL;
		insert_values.clear();
		update_v.clear();
		schema.clear();
		columns.clear();
		query_type = A_NOTHING;

		//cout <<"> " ;
		yyparse();
		
		try {
			if (query_type == A_UPDATE){
				update_rows(update_table,update_v,where_clause);
			}
			else if (query_type == A_DELETE){
				delete_rows(delete_table,where_clause);
			}
			else if (query_type==A_SELECT){
				select_rows(select_root, where_clause, columns);
			}
			else if (query_type==A_INSERT){
				insert_row(insert_table, insert_values);
			}
			else if(query_type==A_CREATE_DB){
				create_database(create_dbase);
			}
			else if(query_type==A_USE_DB){
				use_database(use_dbase);
			}
			else if(query_type==A_CREATE_TABLE){
				create_table_func(vcreate_table,schema);
			}
			else if(query_type==A_DROP_DB){
				drop_database(drop_dbase);
			}
			else if(query_type==A_DROP_TABLE){
				drop_table_func(vdrop_table);
			}
			else if(query_type== A_CREATE_INDEX){
				create_index_func(index_table,index_column);
			}
			else if(query_type== A_DROP_INDEX){
				drop_index_func(index_table,index_column);
			}
		} catch (string s){
			cout<<"ERROR: "<<s<<endl;
		}


	} while (!feof(stdin));

	



	/*for (vector<pair<string,string> >::iterator i = columns.begin(); i!=columns.end(); i++){
		cout<<i->first<<" "<<i->second<<endl;
	}
	*/
	return 0;
}

void yyerror(const char* s){
	//nothing
	cout<< "ERROR: " << string(s)<<endl;
	while (yylex()!=SEMI);
}