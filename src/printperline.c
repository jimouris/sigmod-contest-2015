#include <stdio.h>

int main(void) {
    char c;
    while ((c = getchar()) != EOF) {
        putchar(c);
        putchar('\n');
    }
    return 0;
}
