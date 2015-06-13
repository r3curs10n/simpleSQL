#ifndef H_CONDITION_AND
#define H_CONDITION_AND

#include "condition.h"

class condition_and : public condition {

public:
	condition::condition_type type();
	condition_and();

	condition* left, *right;

	bool eval(record_type& rt, record& r);

	void destroy();

};

#endif