#ifndef TABLE_SCANNER_H
#define TABLE_SCANNER_H

//base class for table_iterator
class table_scanner{
public:
	virtual blk_addr get_next(record&)=0;

};

#endif