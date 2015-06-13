#ifndef H_CONDITION_OR
#define H_CONDITION_OR

#include "condition.h"

class condition_or : public condition {

public:
	condition::condition_type type();
	condition_or();

	condition* left, *right;

	bool eval(record_type& rt, record& r);

	void destroy();

};

#endif