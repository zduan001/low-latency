#include <bits/stdc++.h>
using namespace std;

class MemPool {
private:
    struct Header {
        int sz;
        bool is_free;
        Header* prev = nullptr;
        Header* nxt = nullptr;
        multimap<int, Header*>::iterator it;
    };
    int total_size;
    Header* start;
    multimap<int, Header*> mp;
    int SZ = sizeof(Header);
public:
    MemPool(int sz) : total_size(sz) {
        start = (Header*)malloc(total_size);
        start->sz = total_size - SZ;
        start->is_free = true;
        start->it = mp.emplace(start->sz, start);
    }

    ~MemPool() {
        free(start);
    }

    void* allocate(int sz) {
        if (sz == 0) return nullptr;

        auto it = mp.lower_bound(sz);
        if (it == mp.end()) return nullptr;

        Header* cur = it->second;
        cur->is_free = false;
        mp.erase(cur->it);

        if (cur->sz == sz) {
            return (void*)((char*)cur + SZ);
        } 

        Header* nxt_add = (Header*)((char*)cur + SZ + sz);
        nxt_add->sz = cur->sz - SZ -sz;
        nxt_add->is_free = true;

        nxt_add->nxt = cur->nxt;
        if (nxt_add->nxt) nxt_add->nxt->prev = nxt_add;
        nxt_add->it = mp.emplace(nxt_add->sz, nxt_add);

        cur->sz = sz;
        cur->nxt = nxt_add;
        nxt_add->prev = cur;

        return (void*)((char*)cur + SZ);
    }

    void deallocate(void* ptr) {
        if (!ptr) return;

        Header* cur = (Header*)((char*)ptr - SZ);
        if (cur->is_free) return;
        cur->is_free = true;

        Header* nxt = cur->nxt;
        if (nxt && nxt->is_free) {
            mp.erase(nxt->it);
            cur->sz += cur->nxt->sz + SZ;
            cur->nxt = nxt->nxt;
            if (cur->nxt) cur->nxt->prev = cur;
        }

        Header* prev = cur->prev;
        if (prev && prev->is_free) {
            mp.erase(prev->it);
            prev->nxt = cur->nxt;
            if (cur->nxt) cur->nxt->prev = prev;
            prev->sz += cur->sz + SZ;

            cur = prev;
        }
        cur->it = mp.emplace(cur->sz, cur);
    }

    void print_out() {
   
        Header* cur = start;
        int cnt = 0;
        while(cur) {
        std::cout << "BLOCK: " << cnt++ << ": "
                    << (cur->is_free ? " [FREE] " : " [USED] ")
                    << cur->sz << "\n";
        cur = cur->nxt; 
        }
    }

};

int main() {
  // Initialize pool with 1KB of memory
  MemPool pool(1024);

  pool.print_out();

  // Allocate some integers
  int* p1 = (int*)pool.allocate(sizeof(int));
  *p1 = 42;
  std::cout << "Allocated int: " << *p1 << "\n";
  pool.print_out();

  // Allocate an array
  char* p2 = (char*)pool.allocate(100);
  std::cout << "Allocated 100 bytes\n";

  pool.print_out();

  // Free the first int
  pool.deallocate(p1);
  std::cout << "Freed int\n";
  pool.print_out();
  
  // Allocate something larger to trigger a merge/split
  void* p3 = pool.allocate(sizeof(double));
  std::cout << "Allocated double\n";

  pool.print_out();
  

  std::cout << "freed 100:\n";
  pool.deallocate(p2);
  pool.print_out();

  std::cout << "freed double:\n";
  pool.deallocate(p3);

  std::cout << "All freed:\n";
  pool.print_out();

  return 0;
}