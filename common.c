/**
 * Common functions useful for various purposes implementation.
 * By Stan Hatko
 *
 * License: GNU GPL
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "common.h"

// Ensure environment variable of OK length.
void validate_config_len(const char *name, const char *var, int max_len)
{
    if (var == NULL)
    {
        fprintf(stderr, "Variable %s cannot be NULL!", name);
        exit(1);
    }

    int nv = strlen(var);
    if (nv > max_len)
    {
        fprintf(stderr, "Variable %s too long at %d characters, maximum is %d!", name, nv, max_len);
        exit(1);
    }
}

// Print config variable.
void print_config_var(const char *name, const char *var)
{
    if (var == NULL)
        fprintf(stderr, "Configuration variable %s is NULL pointer.", name);
    else
        fprintf(stderr, "Configuration variable %s: %s", name, var);
}
