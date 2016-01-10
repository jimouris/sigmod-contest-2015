#include <stdio.h>
#include <string.h>
#include <inttypes.h>
#include "constants.h"


int main(void)
{
	int arr[] = {1,3,5,7,11,15,15,15,17,18,18,18,19,20,22,22,22,22,22,22,22,22,22,22,27,27,27};
	uint64_t size = sizeof(arr) / sizeof(int);
	fprintf(stderr, "Size: %zu \n",size );
	
	uint64_t from = 4;
	uint64_t to = 15;
	uint64_t first_appearance_start, first_appearance_end;

	uint64_t first = 0;
	uint64_t last =  size - 1;
	uint64_t middle = (first+last)/2;
	Boolean_t not_found = False;

	while (first <= last && not_found == False) {
		if (arr[middle] <  from){
			first = middle + 1;    
		}
		else if (arr[middle]  ==  from) {
			first_appearance_start = middle;
			break;
		}
		else{
			if(middle == 0){
				not_found = True;
				break;
			}
			last = middle - 1;
		}
		middle = (first + last)/2;
	}
	if (first > last || not_found == True){	//Not found
		first_appearance_start = (last <= first) ? last : first;
		while(first_appearance_start <  size && arr[first_appearance_start]  <  from){
			first_appearance_start++;
		}
	}
	if(first_appearance_start >=  size){
		printf("Not exists\n");
		return 0;
	}
	while(first_appearance_start > 0 && arr[first_appearance_start-1]  == arr[first_appearance_start] ){
		first_appearance_start--;
	}

	first_appearance_start = 2;
	uint64_t i;

	first = 0;
	last = size - 1;
	middle = (first+last)/2;
	first_appearance_end = 0;
	not_found = False;

	while (first <= last && not_found == False) {
		if (arr[middle] < to){
			first = middle + 1;    
		}
		else if (arr[middle] == to) {
			first_appearance_end = middle;
			break;
		}
		else{
			if(middle == 0){
				not_found = True;
				break;
			}
			last = middle - 1;
		}
		middle = (first + last)/2;
	}
	if (first > last || not_found == True){	//Not found
		fprintf(stderr, "not_found = %d\n",not_found);
		fprintf(stderr, "first = %zu\n",first );
		fprintf(stderr, "last = %zu\n",last );
		fprintf(stderr, "middle = %zu\n",middle );
		fprintf(stderr, "first_appearance_end(before) = %zu\n",first_appearance_end );
		first_appearance_end = (last > first) ? last : first;
		if(first_appearance_end >= size)
			first_appearance_end = size-1;
		fprintf(stderr, "first_appearance_end(after) = %zu\n",first_appearance_end );
		while(first_appearance_end >= first_appearance_start && arr[first_appearance_end] > to){
			first_appearance_end--;
		}
	}
	if(first_appearance_end >= size){
		printf("Not exists\n");
		return 0;
	}
	// while(first_appearance_end > 0 && arr[first_appearance_end-1] == arr[first_appearance_end]){
	// 	first_appearance_end--;
	// }

	fprintf(stderr, "first_appearance_end= %zu\n",first_appearance_end );
	last = first_appearance_end;
	while(last+1 < size && arr[last+1] <= to ) {
		last++;
	}
	fprintf(stderr, "last= %zu\n",last );
	i = last - first_appearance_start + 1;
	printf("records: %zu\n", i);


	char str[50];
	sprintf(str,"%zu", i);
	printf("str: %s\n", str);
	printf("len: %lu\n", strlen(str));


	return 0;
}
