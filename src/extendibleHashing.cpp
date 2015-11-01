#include <iostream>

using namespace std;

class ExtendibleHashing {
	
	private:

		class Index {
			private:
				typedef struct Bucket {
					int data;
					size_t localDepth;
				} Bucket;

				size_t globalDepth;
				size_t numBuckets;
				Bucket *buckets;
			public:
				Index() { 
					this->globalDepth = 1;
					this->numBuckets = 2;
					this->buckets = new Bucket[2];
				}

				~Index();
		};

		Index *index;

	public:
		ExtendibleHashing() {
			this->index = new Index;
		}

		~ExtendibleHashing();
	
		
};

int main(void) {

}
