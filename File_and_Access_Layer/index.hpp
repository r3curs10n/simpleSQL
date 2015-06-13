#ifndef INDEX_H
#define INDEX_H

#include "../Space_and_Buffer_Manager/file.h"
#include "bptree.hpp"
#include "table_iterator.hpp"
#include "typedefs.hpp"

//helper class for creation and managements of indices on tables
template <class keytype> class indexhelper{
	file *tfile;
	file *secinxfile;
	schema_type tblschema;
	blk_size blksize;
	string tblname;
	string attrname;
	int where;
	blk_size offset;
	bool ifinit;

	blk_size getoffset();
	blk_size getpibsize();
	blk_size getibsize();
	int getwhere();
	int is_indexed();
	bool ifdups();

public:
	indexhelper(string tname, string attr, schema_type sc, blk_size bs);
	bool init();
	bool create_primary_index();
	bool create_index();
	bool insert_into_index(keytype key, blk_addr bad);
	bool remove_from_index(keytype key, blk_addr bad);
	~indexhelper();
	
};

template <class keytype> indexhelper<keytype>::indexhelper(string tname, string attr, 
											schema_type sc, blk_size bs){
	tblname = tname;
	attrname = attr;
	tblschema = sc;
	blksize = bs;
	tfile = NULL;
	secinxfile = NULL;
	ifinit = false;
	where = getwhere();
	offset = getoffset();
}

template <class keytype> bool indexhelper<keytype>::init(){
	tfile = new file(tblname.c_str());
	
	if(!tfile->success){
		delete tfile;
		tfile = NULL;
		cerr << "System error\n";
		return false;
	}
	ifinit = true;
	return true;
}

template <class keytype> blk_size indexhelper<keytype>::getoffset(){
	schema_type::iterator it;
	blk_size offset = 0;

	for(it = tblschema.begin(); it != tblschema.end(); it++){
		if(attrname == it->name)
			return offset;
		offset += it->size;
	}
	return -1;
}

template <class keytype> int indexhelper<keytype>::getwhere(){
	schema_type::iterator it;
	int where = 0;

	for(it = tblschema.begin(); it != tblschema.end(); it++){
		if(attrname == it->name)
			return where;
		where++;
	}
	return -1;
}

template <class keytype> int indexhelper<keytype>::is_indexed(){
	return tblschema[where].ifindex;
}


//Assuming tblfile is not NULL and attr is a valid attribute
template <class keytype> bool indexhelper<keytype>::ifdups(){
	set<keytype> datavals;
	vector<char> buffer;

	table_iterator<blk_addr> *tit = new table_iterator<blk_addr>(tfile, secinxfile, blksize);
	blk_addr temp;
	while((temp = tit->get_next(buffer)) >= 0){
		keytype val = *((keytype *)&buffer[offset]);
		if(datavals.find(val) != datavals.end()) return true;
		datavals.insert(val);
	}

	return false;
}

template <class keytype> blk_size indexhelper<keytype>::getpibsize(){
	switch(tblschema[where].type){
		case column_type::INT:
			return INT_INDEX_BLOCK;
		case column_type::FLOAT:
			return FLOAT_INDEX_BLOCK;
		case column_type::STRING:
			return STRING_INDEX_BLOCK;
	}
}

template <class keytype> blk_size indexhelper<keytype>::getibsize(){
	switch(tblschema[where].type){
		case column_type::INT:
			return INT_INDEX_BLOCK + (SEC_INDEX_ORDER-1)*sizeof(blk_addr);
		case column_type::FLOAT:
			return FLOAT_INDEX_BLOCK + (SEC_INDEX_ORDER-1)*sizeof(blk_addr);
		case column_type::STRING:
			return STRING_INDEX_BLOCK + (SEC_INDEX_ORDER-1)*sizeof(blk_addr);
	}
}

template <class keytype> bool indexhelper<keytype>::create_primary_index(){
	if(!ifinit) return false;

	string sifname(tblname);
	sifname = sifname + "_secindex";

	secinxfile = new file(sifname.c_str());
	
	if(!secinxfile->success){
		delete secinxfile;
		secinxfile = NULL;
		cerr << "System error\n";
		return false;
	}

	if(ifdups()){
		cerr << "There are duplicate entries in the table for: " << attrname << endl;
		return false;
	}

	string ifname;
	ifname = tblname + "_pindex_";
	ifname = ifname + attrname;
	file *indexfp = new file(ifname.c_str(), getpibsize(), 0);
	
	if(!indexfp->success){
		delete secinxfile;
		secinxfile = NULL;
		delete indexfp;
		cerr << "System error\n";
		return false;
	}

	blk_addr temp = indexfp->allocate_block();
	if(temp){
		delete secinxfile;
		secinxfile = NULL;
		delete indexfp;
		cerr << "System error\n";
		return false;
	}
	void *indexbuf = indexfp->read_block(0);
	*((blk_addr *)indexbuf) = -1;
	indexfp->write_back(0);
	indexfp->unread_block(0);

	bptree<keytype> bp(indexfp, SEC_INDEX_ORDER);
	table_iterator<blk_addr> *tit = new table_iterator<blk_addr>(tfile, secinxfile, blksize);
	vector<char> buffer;
	while((temp = tit->get_next(buffer)) >= 0){
		keytype val = *((keytype *)&buffer[offset]);
		if(!bp.insert(val, temp)){
			delete secinxfile;
			secinxfile = NULL;
			delete indexfp;
			cerr << "System error\n";
			return false;			
		}
	}

	delete secinxfile;
	secinxfile = NULL;
	delete indexfp;

	return true;
}


template <class keytype> bool indexhelper<keytype>::create_index(){
	if(!ifinit) return false;

	string sifname(tblname);
	sifname = sifname + "_secindex";

	secinxfile = new file(sifname.c_str());
	
	if(!secinxfile->success){
		delete secinxfile;
		secinxfile = NULL;
		cerr << "System error\n";
		return false;
	}

	string ifname;
	ifname = tblname + "_index_";
	ifname = ifname + attrname;
	file *indexfp = new file(ifname.c_str(), getibsize(), 0);
	
	if(!indexfp->success){
		delete secinxfile;
		secinxfile = NULL;
		delete indexfp;
		cerr << "System error\n";
		return false;
	}

	blk_addr temp = indexfp->allocate_block();
	if(temp){
		delete secinxfile;
		secinxfile = NULL;
		delete indexfp;
		cerr << "System error\n";
		return false;
	}
	void *indexbuf = indexfp->read_block(0);
	*((blk_addr *)indexbuf) = -1;
	indexfp->write_back(0);
	indexfp->unread_block(0);

	bptree<pair<keytype,blk_addr> > bp(indexfp, SEC_INDEX_ORDER);
	table_iterator<blk_addr> *tit = new table_iterator<blk_addr>(tfile, secinxfile, blksize);
	vector<char> buffer;
	while((temp = tit->get_next(buffer)) >= 0){
		keytype val = *((keytype *)&buffer[offset]);
		if(!bp.insert(make_pair(val, temp), temp)){
			delete secinxfile;
			secinxfile = NULL;
			delete indexfp;
			cerr << "System error\n";
			return false;			
		}
	}

	delete secinxfile;
	secinxfile = NULL;
	delete indexfp;

	return true;
}

template <class keytype> bool indexhelper<keytype>::insert_into_index(keytype key, blk_addr bad){
	string ifname;
	ifname = tblname + "_index_";
	ifname = ifname + attrname;
	file *indexfp = new file(ifname.c_str());
	
	if(!indexfp->success){
		delete indexfp;
		cerr << "System error\n";
		return false;
	}

	bptree<pair<keytype,blk_addr> > bp(indexfp, SEC_INDEX_ORDER);
	if(!bp.insert(make_pair(key, bad), bad)){
		delete indexfp;
		cerr << "System error\n";
		return false;
	}
	delete indexfp;
	return true;
}

template <class keytype> bool indexhelper<keytype>::remove_from_index(keytype key, blk_addr bad){
	string ifname;
	ifname = tblname + "_index_";
	ifname = ifname + attrname;
	file *indexfp = new file(ifname.c_str());
	
	if(!indexfp->success){
		delete indexfp;
		cerr << "System error\n";
		return false;
	}

	bptree<pair<keytype,blk_addr> > bp(indexfp, SEC_INDEX_ORDER);
	bp.remove(make_pair(key,bad));
	delete indexfp;
	return true;
}

//template <class keytype> 

template <class keytype> indexhelper<keytype>::~indexhelper(){
	if(tfile) delete tfile;
	if(secinxfile) delete secinxfile;
}


#endif