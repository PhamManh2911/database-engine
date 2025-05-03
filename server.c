#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

char *get_input()
{
    size_t baseCapacity = 10;
    char *inputBuffer = malloc(baseCapacity * sizeof(int));

    if (!inputBuffer)
    {
        perror("Malloc failed!");
        exit(EXIT_FAILURE);
    }
    // c for capture user input each type user type.
    // how about when user delete
    int c;
    // current size of user input
    int currentSize = 0;

    while ((c = getchar()) != '\n' && c != EOF)
    {
        // leave at least one character for ending char (\n) checking.
        if (c + 1 >= baseCapacity)
        {
            baseCapacity *= 2;
            char *newBuffer = realloc(inputBuffer, baseCapacity * sizeof(int));

            if (!newBuffer)
            {
                perror("Realloc failed!");
                free(inputBuffer);
                exit(EXIT_FAILURE);
            }
            inputBuffer = newBuffer;
        }
        *(inputBuffer + currentSize) = (char)c;
        currentSize++;
    }
    *(inputBuffer + currentSize) = '\0';

    return inputBuffer;
}

void clearInputBuffer(char *buffer)
{
    free(buffer);
    buffer = NULL;
}

int main(int argc, char const *argv[])
{
    printf("Server's listening!\n");
    while (true)
    {
        // read data from input and load to input buffer
        printf("db > ");

        char *userInput = get_input();

        // processing input data
        if (strcmp(userInput, ".exit") == 0)
        {
            clearInputBuffer(userInput);
            exit(EXIT_SUCCESS);
        }
        // some processes are done

        // define output buffer for storing result (chars[])
        printf("Your query is: %s\n", userInput);

        // free memory address after using it
        clearInputBuffer(userInput);
    }

    exit(EXIT_SUCCESS);
}
