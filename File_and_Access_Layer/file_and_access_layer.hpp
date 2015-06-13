#ifndef FILE_AND_ACCESS_LAYER_H
#define FILE_AND_ACCESS_LAYER_H

#include "includes.hpp"

#define MAX_TABLE_NAME 20
#define MAX_COL_NAME 20
#define CATALOG_BLOCK MAX_COL_NAME+MAX_TABLE_NAME+2*sizeof(blk_addr)+sizeof(blk_size)+2*sizeof(int)+4
#define OFFSET_IFINDEX MAX_TABLE_NAME+MAX_COL_NAME+sizeof(int)+sizeof(blk_size)

//this class provides access to the File and Access Layer
//through the functions provided by it
//using Space and Buffer Manager
class file_and_access_layer{
	char *current_database;	//database in use
	catalog_type catalog;	//catalog containing database info
	recordsize rsize;	//keeps track of record size for each table
	file *cfile;	//file in hard disk containing catalog

	bool read_catalog(); //read catalog from hard disk
	bool print_catalog();
	bool if_exists(const char *tname); //check for the existence of table in database
	file *open_table(const char *tname);	//opens the file for the table
	blk_size getoffset(string tname, string attrname); //get offset for the attribute in the record
	int getwhere(string tname, string attrname);	//get position of the attribute in the record
public:
	bool create_database(const char *s); //creates a new database
	bool use_database(const char *s);	//sets new database for use and brings database info
	bool create_table(const char *tname, record_type& schema); //creates a table in the database
	bool insert_into_table(const char *tname, entry_type& entry); //insert record into table
	bool delete_record(const char *tname, blk_addr rid); //delete a given record
	bool update_table(const char *tname, blk_addr baddr, update_vector& uv); //update a record
	table_scanner *scan_table(const char *tname); //scan the whole table
  	bool get_schema(const char *tname, schema_type& schema); //get table schema in schema_type format
	bool get_schema(const char *tname, record_type& record); //get table schema in record_type format
	int is_indexed(string& tname, string& colname); //check if attribute is indexed
	bool create_primary_index(const char *tname, const char *attrname); //create a primary key on an attribute
	bool create_index(const char *tname, const char *attrname); //create an index on an attribute
	table_scanner *search_from_index(condition_op_const &cnd); //search on given condition using index
	bool  drop_index(const char *tname, const char *attrname); //drop index

	file_and_access_layer();
	~file_and_access_layer();
};


#endif