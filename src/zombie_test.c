#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <errno.h>
#include <string.h>
#include "constants.h"

typedef struct Column Column_t;
typedef struct Zombie_node Zombie_node;
typedef struct ZombieList ZombieList;

typedef struct Zombie_node {
   void* subBucket;
   int num;
   struct Zombie_node *next;
   struct Zombie_node *prev;
} Zombie_node;

typedef struct ZombieList {
  	Zombie_node* first_zombie;
  	Zombie_node* last_zombie;
	uint64_t num_of_zombies;
} ZombieList;


ZombieList* zombieList_create(void) {
	ZombieList* zombie_list = malloc(sizeof(ZombieList));
	ALLOCATION_ERROR(zombie_list);
	zombie_list->first_zombie = NULL;
	zombie_list->last_zombie = NULL;
	zombie_list->num_of_zombies = 0;
	return zombie_list;
}

Zombie_node * zombieList_insert_end(ZombieList* zombie_list,void* subBucket,int num) {
	Zombie_node *new_zombie = malloc(sizeof(Zombie_node));
	ALLOCATION_ERROR(new_zombie);
	new_zombie->subBucket = subBucket;
	new_zombie->num = num;
	new_zombie->next = NULL;	
	if (zombie_list->num_of_zombies == 0){
		zombie_list->first_zombie = new_zombie;
		zombie_list->last_zombie = new_zombie;
		new_zombie->prev = NULL;		
	} else {
		new_zombie->prev = zombie_list->last_zombie;
		zombie_list->last_zombie->next = new_zombie;
		zombie_list->last_zombie = new_zombie;
	}
	zombie_list->num_of_zombies++;
	return new_zombie;
}

void zombieList_remove(ZombieList *zombie_list, Zombie_node * zombie) {
	if (zombie->next != NULL) { /*there is next zombie*/
		if (zombie->prev != NULL) { /*somewhere in the middle of the list*/
			zombie->next->prev = zombie->prev;
			zombie->prev->next = zombie->next;
		} else { /*you are the first node*/
			zombie->next->prev = NULL;
			zombie_list->first_zombie = zombie->next;
		}
	} else {
		if (zombie->prev == NULL) { /*only one zombie on the zombie list*/
			zombie_list->first_zombie = NULL;
			zombie_list->last_zombie = NULL;
		} else { /*you are the last node*/
			zombie->prev->next = NULL;
			zombie_list->last_zombie = zombie->prev;
		}
	}
	zombie_list->num_of_zombies--;
	free(zombie);
}


void zombieList_destroy(ZombieList *zombie_list){
	while(zombie_list->num_of_zombies > 0)
		zombieList_remove(zombie_list, zombie_list->first_zombie);
	free(zombie_list);
}

int main(void) {
	ZombieList* zombie_list =  zombieList_create();
	Zombie_node* del =zombieList_insert_end(zombie_list,NULL,1);
	Zombie_node* del2 = zombieList_insert_end(zombie_list,NULL,2);
	Zombie_node* del3 =zombieList_insert_end(zombie_list,NULL,3);
	Zombie_node* del4 =zombieList_insert_end(zombie_list,NULL,4);
	Zombie_node* del5 =zombieList_insert_end(zombie_list,NULL,5);
	Zombie_node* del6 =zombieList_insert_end(zombie_list,NULL,46);
	Zombie_node* del7 =zombieList_insert_end(zombie_list,NULL,45);
	Zombie_node* del8 =zombieList_insert_end(zombie_list,NULL,44);
	Zombie_node* del9 =zombieList_insert_end(zombie_list,NULL,42);
	Zombie_node* del10 =zombieList_insert_end(zombie_list,NULL,24);
	Zombie_node* del11 =zombieList_insert_end(zombie_list,NULL,41);
	zombieList_remove(zombie_list,del);
	zombieList_remove(zombie_list,del11);
	zombieList_remove(zombie_list,del2);
	zombieList_remove(zombie_list,del10);
	zombieList_remove(zombie_list,del9);
	// zombieList_remove(zombie_list,del8);
	zombieList_remove(zombie_list,del3);
	zombieList_remove(zombie_list,del5);
	// zombieList_remove(zombie_list,del6);
	zombieList_remove(zombie_list,del7);
	zombieList_remove(zombie_list,del4);
	Zombie_node *temp = zombie_list -> first_zombie;
	while(temp != NULL) {
		printf("(%d)\n",temp->num);
		temp = temp->next;
	}
	zombieList_destroy(zombie_list);
	return 0;
}