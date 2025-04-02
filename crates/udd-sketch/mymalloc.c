#define _GNU_SOURCE
#include <dlfcn.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>

char tmpbuff[1024];
unsigned long tmppos = 0;
unsigned long tmpallocs = 0;

void *memset(void*,int,size_t);
void *memmove(void *to, const void *from, size_t size);

/*=========================================================
 * interception points
 */

static void * (*myfn_calloc)(size_t nmemb, size_t size);
static void * (*myfn_malloc)(size_t size);
static void   (*myfn_free)(void *ptr);
static void * (*myfn_realloc)(void *ptr, size_t size);
static void * (*myfn_memalign)(size_t blocksize, size_t bytes);

static void init()
{
    myfn_malloc     = dlsym(RTLD_NEXT, "malloc");
    myfn_free       = dlsym(RTLD_NEXT, "free");
    myfn_calloc     = dlsym(RTLD_NEXT, "calloc");
    myfn_realloc    = dlsym(RTLD_NEXT, "realloc");
    myfn_memalign   = dlsym(RTLD_NEXT, "memalign");

    if (!myfn_malloc || !myfn_free || !myfn_calloc || !myfn_realloc || !myfn_memalign)
    {
        fprintf(stderr, "Error in `dlsym`: %s\n", dlerror());
        exit(1);
    }
}

#define _GNU_SOURCE
#include <dlfcn.h>
#include <stdio.h>
#include <stdlib.h>
#include <malloc/malloc.h>

#define DYLD_INTERPOSE(_replacment,_replacee) \
__attribute__((used)) static struct{ const void* replacment; const void* replacee; } _interpose_##_replacee \
__attribute__ ((section ("__DATA,__interpose"))) = { (const void*)(unsigned long)&_replacment, (const void*)(unsigned long)&_replacee };

void* pMalloc(size_t size) //would be nice if I didn't have to rename my function..
{
   void *ptr = malloc(size);

   if (ptr != NULL) {
      size = malloc_size(ptr);
      printf("LD_PRELOAD:malloc %zu \n", size);
   }
   return ptr;
}
DYLD_INTERPOSE(pMalloc, malloc);

void *pRealloc(void *ptr, size_t size)
{
   size_t old_size;

   old_size = malloc_size(ptr);
   ptr = realloc(ptr, size);
   
   if (ptr != NULL) {
      size = malloc_size(ptr);
      printf("LD_PRELOAD:realloc %zu %zu \n", size, old_size;
   }
   return ptr;
}
DYLD_INTERPOSE(pRealloc, realloc);

void *pCalloc(size_t nmemb, size_t size)
{
   void *ptr = calloc(nmemb, size);
   if (ptr != NULL) {
      size = malloc_size(ptr);
      printf("LD_PRELOAD:calloc %zu \n", size);
   }
   return ptr;
}
DYLD_INTERPOSE(pCalloc, calloc);
