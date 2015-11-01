#include <iostream>

using namespace std;

class ExtendibleHashing {
	
	private:	
		class Index {
			private:
				typedef struct Bucket {
					int data;
				} Bucket;

				size_t localDepth;
				size_t numBuckets;
				Bucket *buckets;
			public:
				Index();
				~Index();
		};

		Index *index;

	public:
		ExtendibleHashing() {

		}

		~ExtendibleHashing();
	

};

int main(void) {

}
