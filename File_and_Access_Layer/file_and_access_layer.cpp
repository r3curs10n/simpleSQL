
#include "file_and_access_layer.hpp"

//creates a copy of c string
char *cstr(const char *s){
	char *s1 = new char[strlen(s)+1];
	strcpy(s1,s);
	return s1;
}

//utility function to parse a record entry according to schema
void parse_and_print(record &buf, schema_type schema){
	schema_type::iterator it;
	int sz=0;
	for(it = schema.begin(); it != schema.end(); it++){
		if(it->type == column_type::INT){
			cout << *((int *)&buf[sz]) << " ";
			sz += it->size;
		}
		else if(it->type == column_type::FLOAT){
			cout << *((float *)&buf[sz]) << " ";
			sz += it->size;
		}
		else{
			cout << string((char *)&buf[sz]) << " ";
			sz += it->size;
		}
	}
	cout << endl;
}

//constructor
file_and_access_layer::file_and_access_layer(){
	current_database = NULL;
	catalog = catalog_type();
	rsize = recordsize();
	cfile = NULL;
}

//create a new database, if one with the same name doesn't exist
bool file_and_access_layer::create_database(const char *s){
	if(!(mkdir(s, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH))){
		string filename(s);
		filename = filename + "/catalog"; //create aa catalog file for the database
		file *fp = new file(filename.c_str(), CATALOG_BLOCK, 0);
		if(fp->success){
			cout << "Database successfully created\n";
			delete fp;
			return true;
		}
		cerr << "System error\n";
		delete fp;
	}

	else{
		if(errno == EEXIST){
			cerr << "Database already exists\n";
		}
		else cerr << "Database couldn't be created\n";
	}
	return false;
}

//print catalog info (database info)
bool file_and_access_layer::print_catalog(){
	cout << "Catalog: \n";
	catalog_type::iterator it;
	for(it = catalog.begin(); it != catalog.end(); it++){
		cout << it->first << ":\n";
		schema_type::iterator it1;
		cout << "rID\tName\tType\tIndex\n";
		for(it1 = (it->second).begin(); it1 != (it->second).end(); it1++){
			cout << it1->rid << "\t" << it1->name << "\t";
			switch(it1->type){
				case column_type::INT:
					cout << "INT\t";
					break;
				case column_type::FLOAT:
					cout << "FLOAT\t";
					break;
				case column_type::STRING:
					cout << "STRING\t";
					break;
			}
			if(it1->ifindex)
				cout << "yes" << endl;
			else
				cout << "no" << endl;
		}
	}
}

//read catalog file and populate catalog residing in the main memory
bool file_and_access_layer::read_catalog(){
	string filename(current_database);
	filename = filename + "/catalog";
	cfile = new file(filename.c_str());

	if(cfile->success){
		blk_addr i = 0;
		char *buf;
		string tname;
		attribute_type attribute;
		while(buf=(char *)cfile->read_block(i)){
			tname = string(buf); //table name in the first position
			buf = buf + MAX_TABLE_NAME;
			attribute.name = string(buf); //attribue name in the first position
			buf = buf + MAX_COL_NAME;
			attribute.type = (column_type::data_type)*((int *)buf); //attribute data type in the 3rd pos
			buf = buf + sizeof(int);
			attribute.size = *((blk_size *)buf); //attribute size in the 4th pos
			buf = buf + sizeof(blk_size);
			attribute.ifindex = *((int *)buf); //attribute index in the 5th pos
			buf = buf + sizeof(int);
			attribute.rid = i;

			if(catalog.find(tname) == catalog.end()){
				rsize[tname] = 0;
				catalog[tname] = schema_type();
			}
			catalog[tname].push_back(attribute);
			rsize[tname] += attribute.size;
			cfile->unread_block(i);
			i++;
		}
		return true;
	}
	return false;
}

//sets new database for use and brings database info
bool file_and_access_layer::use_database(const char *s){
	struct stat sb;
	if(stat(s, &sb) == 0 && S_ISDIR(sb.st_mode)){
		current_database = cstr(s);
		catalog.clear();
		rsize.clear();
		if(cfile) delete cfile;
		cfile = NULL;
		if(read_catalog()){
			print_catalog();
			cout << "Database successfully changed\n";
			return true;
		}
		else cerr << "System error\n";	    
	}
	else{
		cerr << "Database doesn't exist\n";
	}
	return false;
}


//creates a table in the database
bool file_and_access_layer::create_table(const char *tname, record_type& schema){
	if(!current_database){
		cerr << "No database in use\n";
		return false;
	}

	if(catalog.find(string(tname)) != catalog.end()){
		cerr << "Table with table name: " << tname << " already exists\n";
		return false;
	}

	if(strlen(tname) >= MAX_TABLE_NAME){
		cerr << "Table namesize limit exceeded\n";
		return false;
	}

	record_type::iterator it;
	blk_size bsize = 0;
	for(it = schema.begin(); it != schema.end(); it++){
		if((it->column_name).size() >= MAX_COL_NAME){
			cerr << "Column namesize limit exceeded\n";
			return false;
		}
		bsize += it->size;
	}

	string filename(current_database), ifilename;
	filename = filename + "/";
	filename = filename + tname;
	ifilename = filename + "_secindex";
	file *fp = new file(filename.c_str(), bsize+4, 0);
	file *indexfp = new file(ifilename.c_str(), SEC_INDEX_BLOCK, 0);
	
	if((!fp->success)||(!indexfp->success)){
		delete fp;
		delete indexfp;
		cerr << "System error\n";
		return false;
	}

	blk_addr temp = indexfp->allocate_block();
	if(temp){
		delete fp;
		delete indexfp;
		cerr << "System Error\n";
		return false;
	}
	void *indexbuf = indexfp->read_block(0);
	*((blk_addr *)indexbuf) = -1;
	indexfp->write_back(0);
	indexfp->unread_block(0);

	delete fp;
	delete indexfp;

	schema_type new_schema;
	for(it = schema.begin(); it != schema.end(); it++){
		attribute_type attribute;
		attribute.type = it->type;
		attribute.size = (blk_size)it->size;
		attribute.name = it->column_name;
		attribute.ifindex = NOKEY;
		
		blk_addr baddr = cfile->allocate_block();
		if(baddr < 0){
			cerr << "System error\n";
			return false;
		}
		char *buf = (char *)cfile->read_block(baddr);
		if(!buf){
			cerr << "System error\n";
			cfile->deallocate_block(baddr);
			return false;
		}
		strcpy(buf, tname);
		buf = buf + MAX_TABLE_NAME;
		strcpy(buf, (attribute.name).c_str());
		buf = buf + MAX_COL_NAME;
		*((int *)buf) = attribute.type;
		buf = buf + sizeof(int);
		*((blk_size *)buf) = attribute.size;
		buf = buf + sizeof(blk_size);
		*((int *)buf) = attribute.ifindex;
		buf = buf + sizeof(int);
		*buf = '\0';

		cfile->write_back(baddr);
		cfile->unread_block(baddr);
		attribute.rid = baddr;
		new_schema.push_back(attribute);
	}
	catalog[string(tname)] = new_schema;
	rsize[string(tname)] = bsize;
	//print_catalog();
	return true;
}

//check for the existence of table in database
bool file_and_access_layer::if_exists(const char *tname){
	if(!current_database){
		cerr << "No database in use\n";
		return false;
	}

	if(catalog.find(string(tname)) == catalog.end()){
		cerr << "Table with table name: " << tname << " doesn't exist\n";
		return false;
	}
	return true;
}

//opens the file for the table
file *file_and_access_layer::open_table(const char *tname){
	string filename(current_database);
	filename = filename + "/";
	filename = filename + tname;
	file *fp = new file(filename.c_str());
	
	if(!fp->success){
		delete fp;
		cerr << "System error\n";
		return NULL;
	}
	return fp;
}

//insert record into table
bool file_and_access_layer::insert_into_table(const char *tname, entry_type& entry){
	if(!if_exists(tname)) return false;

	schema_type tschema = catalog[string(tname)];
	if(entry.size() != tschema.size()){
		cerr << "Invalid entry\n";
		return false;
	}


	schema_type::iterator sit;
	entry_type::iterator eit;
	for(sit = tschema.begin(), eit = entry.begin(); sit != tschema.end(); sit++, eit++){
		if(sit->type == column_type::STRING){
			if((eit->first == column_type::STRING)&&((strlen((char *)eit->second)) < sit->size)) continue;
		}
		else{ 
			if(eit->first != column_type::STRING) continue;
		}
		cerr << "Invalid entry\n";
		return false;
	}

	file *fp;
	if(!(fp = open_table(tname))) return false;
	
	blk_addr baddr = fp->allocate_block();
	if(baddr < 0){
		cerr << "System error\n";
		delete fp;
		return false;
	}
	char *buf = (char *)fp->read_block(baddr);
	if(!buf){
		cerr << "System error\n";
		fp->deallocate_block(baddr);
		delete fp;
		return false;
	}
	
	file *ifp;
	string ifname(tname);
	ifname = ifname + "_secindex";
	if(!(ifp = open_table(ifname.c_str()))){
		//cerr << "System error\n";
		fp->unread_block(baddr);
		fp->deallocate_block(baddr);
		delete fp;
		return false;
	}

	bptree<blk_addr> secinx(ifp, SEC_INDEX_ORDER);
	if(!secinx.insert(baddr, baddr)){
		cout << "System error\n";
		fp->unread_block(baddr);
		fp->deallocate_block(baddr);
		delete fp;
		delete ifp;
		return false;
	}

	char *tempbuf = buf;
	for(sit = tschema.begin(), eit = entry.begin(); sit != tschema.end(); sit++, eit++){
		switch(eit->first){
			case column_type::INT:
				*((int *)tempbuf) = *((int *)eit->second);
				//cout << *((int *)buf) << " " << *((int *)eit->second) << endl;
				tempbuf = tempbuf + sit->size;
				break;
			case column_type::FLOAT:
				*((float *)tempbuf) = *((float *)eit->second);
				//cout << *((float *)buf) << " " << *((float *)eit->second) << endl;
				tempbuf = tempbuf + sit->size;
				break;
			case column_type::STRING:
				strcpy(tempbuf, (char *)eit->second);
				tempbuf = tempbuf + sit->size;
		}
	}

	*tempbuf = '\0';
	fp->write_back(baddr);

	mystr x;
	string pathname(current_database);
	pathname = pathname + "/";
	pathname = pathname + tname;

	for(sit = tschema.begin(), eit = entry.begin(); sit != tschema.end(); sit++, eit++){
		if(sit->ifindex > 0){
			if(sit->type == column_type::INT){
				indexhelper<int> iihelp(pathname, sit->name, tschema, rsize[string(tname)]);
				iihelp.insert_into_index(*((int *)eit->second), baddr);
			}
			else if(sit->type == column_type::FLOAT){
				indexhelper<float> fihelp(pathname, sit->name, tschema, rsize[string(tname)]);
				fihelp.insert_into_index(*((float *)eit->second), baddr);
			}
			else if(sit->type == column_type::STRING){
				indexhelper<mystr> sihelp(pathname, sit->name, tschema, rsize[string(tname)]);
				strcpy(x.str, (char *)eit->second);
				sihelp.insert_into_index(x, baddr);
			}
		}
		buf = buf + sit->size;
	}

	fp->unread_block(baddr);
	delete fp;
	delete ifp;

	return true;
}

//scan the whole table
//and return a table iterator
table_scanner *file_and_access_layer::scan_table(const char *tname){
	if(!if_exists(tname)) return NULL;

	file *fp, *sifp;
	if(!(fp = open_table(tname))) return NULL;

	string ifname(tname);
	ifname = ifname + "_secindex";
	if(!(sifp = open_table(ifname.c_str()))){
		delete fp;
		return NULL;
	}
	
	table_iterator<blk_addr> *tit = new table_iterator<blk_addr>(fp, sifp, rsize[string(tname)]);
	return (table_scanner*)tit;
}

//delete a given record
bool file_and_access_layer::delete_record(const char *tname, blk_addr rid){
	if(!if_exists(tname)) return false;
	schema_type tschema = catalog[string(tname)];
	schema_type::iterator sit;

	file *fp;
	file *ifp;
	string ifname(tname);
	ifname = ifname + "_secindex";

	fp = open_table(tname);
	if(!fp) return false;

	char *buf = (char *)fp->read_block(rid);
	if(!buf){
		cerr << "System error\n";
		delete fp;
		return false;
	}

	ifp = open_table(ifname.c_str());
	if(!ifp){
		fp->unread_block(rid);
		delete fp;
		return false;
	}

	bptree<blk_addr> secinx(ifp, SEC_INDEX_ORDER);
	secinx.remove(rid);

	mystr x;
	string pathname(current_database);
	pathname = pathname + "/";
	pathname = pathname + tname;

	for(sit = tschema.begin(); sit != tschema.end(); sit++){
		if(sit->ifindex > 0){
			if(sit->type == column_type::INT){
				indexhelper<int> iihelp(pathname, sit->name, tschema, rsize[string(tname)]);
				iihelp.remove_from_index(*((int *)buf), rid);
			}
			else if(sit->type == column_type::FLOAT){
				indexhelper<float> fihelp(pathname, sit->name, tschema, rsize[string(tname)]);
				fihelp.remove_from_index(*((float *)buf), rid);
			}
			else if(sit->type == column_type::STRING){
				indexhelper<mystr> sihelp(pathname, sit->name, tschema, rsize[string(tname)]);
				strcpy(x.str, (char *)buf);
				sihelp.remove_from_index(x, rid);
			}
		}
		buf = buf + sit->size;
	}


	fp->unread_block(rid);
	fp->deallocate_block(rid);
	delete fp;
	delete ifp;
}

//get table schema in schema_type format
bool file_and_access_layer::get_schema(const char *tname, schema_type &schema){
	if(!if_exists(tname)) return false;

	schema = catalog[string(tname)];
	return true;
}

//get table schema in record_type format
bool file_and_access_layer::get_schema(const char *tname, record_type &record){
	if(!if_exists(tname)) return false;

	schema_type schema;
	schema_type::iterator it;
	schema = catalog[string(tname)];
	for(it = schema.begin(); it != schema.end(); it++){
		column_type col;
		col.table_name = string(tname);
		col.column_name = it->name;
		col.type = it->type;
		col.size = it->size;
		record.push_back(col);
	}
	return true;
}

//assuming table exists
//get offset for the attribute in the record
blk_size file_and_access_layer::getoffset(string tname, string attrname){
	schema_type schema = catalog[tname];
	schema_type::iterator it;

	blk_size bsize = 0;
	for(it = schema.begin(); it != schema.end(); it++){
		if(attrname == it->name)
			return bsize;
		bsize += it->size;
	}
	return -1;
}

//assuming table exists
//get position of the attribute in the record
int file_and_access_layer::getwhere(string tname, string attrname){
	schema_type schema = catalog[tname];
	schema_type::iterator it;

	int where = 0;
	for(it = schema.begin(); it != schema.end(); it++){
		if(attrname == it->name)
			return where;
		where++;
	}
	return -1;
}

//assuming table may/mayn't exist
//check if attribute is indexed
int file_and_access_layer::is_indexed(string& tname, string& colname){
	if(catalog.find(tname) == catalog.end()) return false;

	int where = getwhere(tname, colname);
	if(where < 0) return 0;
	else return (catalog[tname])[where].ifindex;
}

//update a record in the table
bool file_and_access_layer::update_table(const char *tname, blk_addr baddr, update_vector &uv){
	if(!if_exists(tname)) return false;

	update_vector::iterator it;
	int where;
	schema_type schema = catalog[string(tname)];
	for(it = uv.begin(); it != uv.end(); it++){
		if((where = getwhere(string(tname), it->column_name)) < 0){
			cerr << "Invalid attribue\n";
			return false;
		}
		else{
			if(it->type == column_type::STRING){
				if((schema[where].type == column_type::STRING)&&
					((strlen((char *)it->ptr_data)) < schema[where].size))
					continue;
			}
			else{ 
				if(schema[where].type != column_type::STRING) continue;
			}
			cerr << "Invalid entry\n";
			return false;
		}
	}

	file *fp;
	if(!(fp = open_table(tname))) return false;
	
	char *buf = (char *)fp->read_block(baddr);
	if(!buf){
		cerr << "System error\n";
		return false;
	}
	
	char *buffer;
	mystr x;
	string pathname(current_database);
	pathname = pathname + "/";
	pathname = pathname + tname;

	for(it = uv.begin(); it != uv.end(); it++){
		where = getwhere(string(tname), it->column_name);
		attribute_type attribute = schema[where];
		if(it->type == column_type::INT){
			buffer = buf + getoffset(string(tname), it->column_name);
			if(attribute.ifindex > 0){
				indexhelper<int> iihelp(pathname, it->column_name, schema, rsize[string(tname)]);
				iihelp.remove_from_index(*((int *)buffer), baddr);
				iihelp.insert_into_index(*((int *)it->ptr_data), baddr);
			}
			*((int *)buffer) = *((int *)it->ptr_data);
			//cout << *((int *)buf) << " " << *((int *)eit->second) << endl;
		}
		if(it->type == column_type::FLOAT){
			buffer = buf + getoffset(string(tname), it->column_name);
			if(attribute.ifindex > 0){
				indexhelper<float> fihelp(pathname, it->column_name, schema, rsize[string(tname)]);
				fihelp.remove_from_index(*((float *)buffer), baddr);
				fihelp.insert_into_index(*((float *)it->ptr_data), baddr);
			}
			*((float *)buffer) = *((float *)it->ptr_data);
			//cout << *((float *)buf) << " " << *((float *)eit->second) << endl;
		}
		if(it->type == column_type::STRING){
			buffer = buf + getoffset(string(tname), it->column_name);
			if(attribute.ifindex > 0){
				indexhelper<mystr> sihelp(pathname, it->column_name, schema, rsize[string(tname)]);
				strcpy(x.str, buffer);
				sihelp.remove_from_index(x, baddr);
				strcpy(x.str, (char *)it->ptr_data);
				sihelp.insert_into_index(x, baddr);
			}
			strcpy(buffer, (char *)it->ptr_data);
		}
	}

	fp->write_back(baddr);
	fp->unread_block(baddr);
	delete fp;
	return true;
}

//create a primary key on an attribute
bool file_and_access_layer::create_primary_index(const char *tname, const char *attrname){
	if(!if_exists(tname)) return false;

	string tblname(tname);
	string attr(attrname);

	int where = getwhere(tblname, attr);
	if(where < 0){
		cerr << "Invalid attribute\n";
		return false;
	}

	schema_type schema = catalog[tblname];
	if(schema[where].ifindex == PRIKEY){
		cerr << "Already a primary key\n";
		return false;
	}

	string pathname(current_database);
	pathname = pathname + "/";
	pathname = pathname + tblname;

	if(schema[where].type == column_type::INT){
		indexhelper<int> iihelp(pathname, attr, schema, rsize[tblname]);
		if(!iihelp.init()){
 			return false;
		}
		if(!iihelp.create_primary_index()){
			return false;
		}
	}
	else if(schema[where].type == column_type::FLOAT){
		indexhelper<float> fihelp(pathname, attr, schema, rsize[tblname]);
		if(!fihelp.init())
			return false;
		if(!fihelp.create_primary_index())
			return false;
	}
	else if(schema[where].type == column_type::STRING){
		indexhelper<mystr> sihelp(pathname, attr, schema, rsize[tblname]);
		if(!sihelp.init())
			return false;
		if(!sihelp.create_primary_index())
			return false;
	}

	(catalog[tblname])[where].ifindex = PRIKEY;
	char *buf = (char *)cfile->read_block(schema[where].rid);
	buf = buf + OFFSET_IFINDEX;
	*((int *)buf) = PRIKEY;
	cfile->write_back(schema[where].rid);
	cfile->unread_block(schema[where].rid);
	return true;
}

//create an index on an attribute
bool file_and_access_layer::create_index(const char *tname, const char *attrname){
	if(!if_exists(tname)) return false;

	string tblname(tname);
	string attr(attrname);

	int where = getwhere(tblname, attr);
	if(where < 0){
		cerr << "Invalid attribute\n";
		return false;
	}

	schema_type schema = catalog[tblname];
	if(schema[where].ifindex == PRIKEY){
		cerr << "Already a primary key\n";
		return false;
	}
	if(schema[where].ifindex == SECKEY){
		cerr << "Already a key\n";
		return false;
	}

	string pathname(current_database);
	pathname = pathname + "/";
	pathname = pathname + tblname;

	if(schema[where].type == column_type::INT){
		indexhelper<int> iihelp(pathname, attr, schema, rsize[tblname]);
		if(!iihelp.init())
			return false;
		if(!iihelp.create_index())
			return false;
	}
	else if(schema[where].type == column_type::FLOAT){
		indexhelper<float> fihelp(pathname, attr, schema, rsize[tblname]);
		if(!fihelp.init())
			return false;
		if(!fihelp.create_index())
			return false;
	}
	else if(schema[where].type == column_type::STRING){
		indexhelper<mystr> sihelp(pathname, attr, schema, rsize[tblname]);
		if(!sihelp.init())
			return false;
		if(!sihelp.create_index())
			return false;
	}

	(catalog[tblname])[where].ifindex = SECKEY;
	char *buf = (char *)cfile->read_block(schema[where].rid);
	buf = buf + OFFSET_IFINDEX;
	*((int *)buf) = SECKEY;
	cfile->write_back(schema[where].rid);
	cfile->unread_block(schema[where].rid);
	return true;
}

//search on given condition using index
//and return a table scanner
table_scanner *file_and_access_layer::search_from_index(condition_op_const &cnd){
	
	string tblname(cnd.lhs_table_name);
	string attr(cnd.lhs_column_name);

	if(!if_exists(tblname.c_str())) return NULL;

	int where = getwhere(tblname, attr);
	if(where < 0){
		cerr << "Invalid attribute\n";
		return NULL;
	}

	schema_type schema = catalog[tblname];
	if(schema[where].ifindex == NOKEY){
		cerr << "No indexing on: " << attr << endl;
		return NULL;
	}

	file *fp;
	if(!(fp = open_table(tblname.c_str()))) return NULL;
	
	string ifname;
	ifname = tblname + "_index_";
	ifname = ifname + attr;
	file *indexfp;
	
	if(!(indexfp = open_table(ifname.c_str()))){
		delete fp;
		return NULL;
	}

	if(schema[where].type == column_type::INT){
		iindex start, end;
		start = make_pair(INT_MIN, -1);
		end = make_pair(INT_MAX, MAX_RECORDS);
		if(cnd.op == "<") end = make_pair(cnd.value.vInt, MAX_RECORDS);
		else if(cnd.op == ">") start = make_pair(cnd.value.vInt, -1);
		else{
			start = make_pair(cnd.value.vInt, -1);
			end = make_pair(cnd.value.vInt, MAX_RECORDS);
		}

		table_iterator<iindex > *tit = new table_iterator<iindex > 
					(fp, indexfp, rsize[tblname], start, end);
		return (table_scanner *) tit;

	}
	else if(schema[where].type == column_type::FLOAT){
		findex start, end;
		start = make_pair(numeric_limits<float>::min(), -1);
		end = make_pair(numeric_limits<float>::max(), MAX_RECORDS);
		if(cnd.op == "<") end = make_pair(cnd.value.vFloat, MAX_RECORDS);
		else if(cnd.op == ">") start = make_pair(cnd.value.vFloat, -1);
		else{
			start = make_pair(cnd.value.vFloat, -1);
			end = make_pair(cnd.value.vFloat, MAX_RECORDS);
		}

		table_iterator<findex > *tit = new table_iterator<findex > 
					(fp, indexfp, rsize[tblname], start, end);
		return (table_scanner *) tit;
	}
	else if(schema[where].type == column_type::STRING){
		sindex start, end;
		mystr x;
		x.str[0] = '0'-1;
		x.str[1] = '\0';
		start = make_pair(x, -1);
		x.str[0] = 'z'+1;
		end = make_pair(x, MAX_RECORDS);
		strcpy(x.str, cnd.value.vString);
		if(cnd.op == "<") end = make_pair(x, MAX_RECORDS);
		else if(cnd.op == ">") start = make_pair(x, -1);
		else{
			start = make_pair(x, -1);
			end = make_pair(x, MAX_RECORDS);
		}

		table_iterator<sindex > *tit = new table_iterator<sindex > 
					(fp, indexfp, rsize[tblname], start, end);
		return (table_scanner *) tit;
	}

	return NULL;
}

bool file_and_access_layer::drop_index(const char *tname, const char *attrname){
	if(!if_exists(tname)) return false;

	string tblname(tname);
	string attr(attrname);

	int where = getwhere(tblname, attr);
	if(where < 0){
		cerr << "Invalid attribute\n";
		return false;
	}

	schema_type schema = catalog[tblname];
	
	if(schema[where].ifindex == NOKEY){
		cerr << "No key on the attribute\n";
		return false;
	}

	string ifname(current_database);
	ifname = ifname + "/";
	ifname = ifname + tblname;
	ifname = ifname + "_index_";
	ifname = ifname + attr;
	
	if(remove(ifname.c_str()) != 0){
		cerr << "System error\n";
		return false;
	}

	(catalog[tblname])[where].ifindex = NOKEY;
	char *buf = (char *)cfile->read_block(schema[where].rid);
	buf = buf + OFFSET_IFINDEX;
	*((int *)buf) = NOKEY;
	cfile->write_back(schema[where].rid);
	cfile->unread_block(schema[where].rid);
	return true;
}

file_and_access_layer::~file_and_access_layer(){
	if(cfile) delete cfile;
}
