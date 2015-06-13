#include "condition_list.h"

bool condition_list::eval(record_type& rt, record& r){

	for (list<condition*>::iterator i = conditions.begin(); i != conditions.end(); i++){
		if ((*i)->eval(rt, r) == false) return false;
	}

	return true;

}

bool condition_list::condition_comp(condition* l, condition* r){
	return l->type() < r->type();
}

void condition_list::sort_conditions(){
	//sort(conditions.begin(), conditions.end(), condition_comp);
	conditions.sort(condition_list::condition_comp);
}

void condition_list::destroy(){
	for (list<condition*>::iterator i = conditions.begin(); i != conditions.end(); i++){
		(*i)->destroy();
		delete *i;		
	}
}