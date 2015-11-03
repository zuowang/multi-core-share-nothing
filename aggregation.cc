#include <sys/types.h>
#include <sys/stat.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <assert.h>
#include <stdio.h>
#include <sched.h>
#include <time.h>
#include <tr1/unordered_map>
#include <vector>
#include "rand.h"

using namespace std;
using namespace std::tr1;

#define LIKELY(expr) __builtin_expect(!!(expr), 1)
#define UNLIKELY(expr) __builtin_expect(!!(expr), 0)

uint64_t thread_time(void) {
  struct timespec t;
  assert(clock_gettime(CLOCK_THREAD_CPUTIME_ID, &t) == 0);
  return t.tv_sec * 1000 * 1000 * 1000 + t.tv_nsec;
}

int hardware_threads(void)
{
  char name[64];
  struct stat st;
  int threads = -1;
  do {
    sprintf(name, "/sys/devices/system/cpu/cpu%d", ++threads);
  } while (stat(name, &st) == 0);
  return threads;
}

void bind_thread(int thread_id)
{
  int threads = hardware_threads();
  assert(thread_id >= 0 && thread_id < threads);
  size_t size = CPU_ALLOC_SIZE(threads);
  cpu_set_t *cpu_set = CPU_ALLOC(threads);
  assert(cpu_set != NULL);
  CPU_ZERO_S(size, cpu_set);
  CPU_SET_S(thread_id, size, cpu_set);
  assert(pthread_setaffinity_np(pthread_self(), size, cpu_set) == 0);
  CPU_FREE(cpu_set);
}

void *mamalloc(size_t size)
{
  void *ptr = NULL;
  return posix_memalign(&ptr, 64, size) ? NULL : ptr;
}

inline void *align(const void *p)
{
  size_t i = 63 & (size_t) p;
  return (void*) (i ? (int*)p + 64 - i : (int*)p);
}

uint8_t exact_log_2(size_t number)
{
  size_t base = 1;
  uint8_t exponent = 0;
  while (base < number) {
    base += base;
    exponent++;
  }
  return base == number ? exponent : -1;
}

struct Info_blu{
  pthread_t id;
  int cpu;
  int seed;
  size_t size;
  uint64_t time;
  size_t partitions;
  pthread_barrier_t *barrier;
} ;

struct Info {
  pthread_t id;
  int cpu;
  int seed;
  size_t size;
  uint64_t time;
  size_t partitions;
  uint32_t* keys;
  uint32_t* vals;
};
  
struct Node {
  uint32_t key;
  uint32_t val;
  uint32_t hits;
  uint32_t part;
  Node() : key(0), val(0), hits(0), part(0) {}
};

struct OBNode {
  uint32_t key;
  uint32_t val;
  OBNode(int k, int v) : key(k), val(v) {}
};

struct MNode {
  uint32_t key;
  uint32_t val;
  MNode* next;
};

void single_thread_aggregation(int size)
{
  int i;
        size_t p;
  uint32_t *keys = reinterpret_cast<uint32_t*>(mamalloc(size * sizeof(uint32_t)));
  uint32_t *vals = reinterpret_cast<uint32_t*>(mamalloc(size * sizeof(uint32_t)));
  int seed = rand();
  rand32_t *gen = rand32_init(seed);
  for (i = 0 ; i != size ; ++i) {
    keys[i] = rand32_next(gen);
    vals[i] = ~keys[i];
  }
  uint64_t t = thread_time();
  uint32_t factor = rand32_next(gen) | 1;
  int num_buckets = 1024;
  const float MAX_BUCKET_OCCUPANCY_FRACTION = 0.75f;
  int num_buckets_till_resize = MAX_BUCKET_OCCUPANCY_FRACTION * num_buckets;

  vector<MNode*> buckets(num_buckets);
  vector<MNode*> mem_pool;
  const int PAGE_SIZE = 8 * 1024 * 1024;
   MNode* next_node = reinterpret_cast<MNode*>(mamalloc(PAGE_SIZE));
  mem_pool.push_back(next_node);
  int node_remaining_current_page = PAGE_SIZE / sizeof(MNode);
        int num_filled_buckets = 0;
  for (i = 0; i < size; ++i)
  {
    if (UNLIKELY(num_filled_buckets > num_buckets_till_resize)) {
      //ResizeBuckets(num_buckets * 2);
    }
    uint32_t hash = keys[i] * factor;
    int bucket_idx = hash & (num_buckets - 1);
    if (node_remaining_current_page == 0) 
    {
      next_node = reinterpret_cast<MNode*>(mamalloc(PAGE_SIZE));
      mem_pool.push_back(next_node);
      node_remaining_current_page = PAGE_SIZE / sizeof(MNode);      
    }
    MNode* bucket = buckets[bucket_idx];
    
    while (bucket != NULL)
    {
      if (bucket->key == keys[i]) break;
      bucket = bucket->next;
    }
    if (bucket == NULL)
    {
      ++num_filled_buckets;
      next_node->key = keys[i];
      next_node->val = vals[i];
      next_node->next = bucket;
      bucket = next_node;
      --node_remaining_current_page;
      ++next_node;      
    }
    else
    {
      bucket->key = keys[i];
      bucket->val += vals[i];
    }    
  }
  t = thread_time() - t;
  double billion = 1000 * 1000 * 1000;
  fprintf(stderr, "%-30s %6.4f secs %6.2f btps\n",
      "Single thread", t / billion, size * 1.0 / t);
  free(gen);
  free(keys);
  free(vals);
  for (p = 0; p < mem_pool.size(); ++p) free(mem_pool[p]);
}

const int num_threads = hardware_threads();
vector<vector<Node>* > global_buckets(num_threads, NULL);
typedef unordered_map<int, vector<OBNode> > OVERFLOW_BUFFER;
vector<OVERFLOW_BUFFER* > global_obs(num_threads, NULL);
vector<vector<int>* > global_partcnt(num_threads, NULL);

void *multi_thread_blu(void *arg)
{
  Info_blu *d = (Info_blu*) arg;
  assert(pthread_equal(pthread_self(), d->id));
  bind_thread(d->cpu);
  size_t i, o, b = 0, size = d->size;
  uint32_t *keys = reinterpret_cast<uint32_t*>(mamalloc(size * sizeof(uint32_t)));
  uint32_t *vals = reinterpret_cast<uint32_t*>(mamalloc(size * sizeof(uint32_t)));
  rand32_t *gen = rand32_init(d->seed);
  for (i = 0 ; i != size ; ++i) {
    keys[i] = rand32_next(gen);
    vals[i] = ~keys[i];
  }

  uint64_t t = thread_time();
  size_t num_buckets = 32768;
  vector<Node> buckets;
  buckets.resize(num_buckets, Node());
  int node_remaining = num_buckets;
  
  OVERFLOW_BUFFER ob;
  uint32_t factor = rand32_next(gen) | 1;
  for (i = 0 ; i < size; ++i) {
    uint32_t hash = keys[i] * factor;    
    size_t bucket_idx = hash & (num_buckets - 1);
    if (node_remaining == 0) 
    {
      for (b = 0; b < num_buckets; ++b)
      {
        if (buckets[b].hits < 10)
        {
          uint32_t part = buckets[b].part;
          OVERFLOW_BUFFER::iterator it = ob.find(part);
          if (it == ob.end())
          {
            it = ob.insert(make_pair(part, vector<OBNode>())).first;
          }
          it->second.push_back(OBNode(buckets[b].key, buckets[b].val));
          buckets[b].key = -1;
          buckets[b].val = 0;
          ++node_remaining;
        }
      }
      if (node_remaining == 0) 
      {
        for(; i < size; ++i)
        {
          uint32_t hash = keys[i] * factor;
          uint32_t part = hash % d->partitions;
          OVERFLOW_BUFFER::iterator it = ob.find(part);
          if (it == ob.end())
          {
            it = ob.insert(make_pair(part, vector<OBNode>())).first;
          }
          it->second.push_back(OBNode(keys[i], vals[i]));
        }
      }
    }
    size_t cur_idx = bucket_idx;
    while (cur_idx < num_buckets && buckets[cur_idx].hits != 0)
    {
      if (buckets[cur_idx].key == keys[i]) break;
      ++cur_idx;
    }
    if (cur_idx == num_buckets) 
    {
      cur_idx = 0;
      while (cur_idx < bucket_idx && buckets[cur_idx].hits != 0)
      {
        if (buckets[cur_idx].key == keys[i]) break;
        ++cur_idx;
      }
    }
    
    if (buckets[cur_idx].hits == 0)
    {
      buckets[cur_idx].key = keys[i];
      buckets[cur_idx].val = vals[i];
      uint32_t part = hash % d->partitions;
      buckets[cur_idx].part = part;
      ++buckets[cur_idx].hits;
      --node_remaining;
    }
    else
    {
      buckets[cur_idx].val += vals[i];
      ++buckets[cur_idx].hits;
    }
  }
  vector<int> partcnt(d->partitions, 0);
  for (b = 0; b < num_buckets; ++b)
  {
    uint32_t part = buckets[b].part;
    ++partcnt[part];
  }
  OVERFLOW_BUFFER::const_iterator it = ob.begin();
  for (; it != ob.end(); ++it)
  {
    partcnt[it->first] += it->second.size();    
  }
  global_buckets[d->cpu] = &buckets;
  global_obs[d->cpu] = &ob;
  global_partcnt[d->cpu] = &partcnt;
  pthread_barrier_wait(d->barrier);
  
  int part_keys = 0;
  for (int n = 0; n < num_threads; ++n)
  {
    part_keys += (*global_partcnt[n])[d->cpu];
  }

  const float MAX_BUCKET_OCCUPANCY_FRACTION = 0.75f;
  uint8_t bits = exact_log_2(part_keys / MAX_BUCKET_OCCUPANCY_FRACTION);
  assert(bits > 0);
  num_buckets = 0x1L << bits;
  int num_buckets_till_resize = num_buckets * MAX_BUCKET_OCCUPANCY_FRACTION;
  
  vector<MNode*> merge_buckets;
  merge_buckets.resize(num_buckets);
  vector<MNode*> mem_pool;
  const int PAGE_SIZE = 8 * 1024 * 1024;
   MNode* next_node = reinterpret_cast<MNode*>(mamalloc(PAGE_SIZE));
  mem_pool.push_back(next_node);
  int node_remaining_current_page = PAGE_SIZE / sizeof(MNode);
  int num_filled_buckets = 0;
  for (i = 0; i < global_buckets.size(); ++i)
  {
    vector<Node>& buckets = *global_buckets[i];
    for (b = 0; b < buckets.size(); ++b)
    {
      int part = buckets[b].part;
      if (part == d->cpu)
      {
        if (UNLIKELY(num_filled_buckets > num_buckets_till_resize)) {
          //ResizeBuckets(num_buckets_ * 2);
        }
        uint32_t hash = buckets[b].key * factor;
        int bucket_idx = hash & (num_buckets - 1);
        if (node_remaining_current_page == 0) 
        {
          next_node = reinterpret_cast<MNode*>(mamalloc(PAGE_SIZE));
          mem_pool.push_back(next_node);
          node_remaining_current_page = PAGE_SIZE / sizeof(Node);      
        }
        MNode* bucket = merge_buckets[bucket_idx];
        while (bucket != NULL)
        {
          if (bucket->key == buckets[b].key) break;
          bucket = bucket->next;
        }
        if (bucket == NULL)
        {
          ++num_filled_buckets;
          next_node->key = buckets[b].key;
          next_node->val = buckets[b].val;
          next_node->next = bucket;
          bucket = next_node;
          --node_remaining_current_page;
          ++next_node;      
        }
        else
        {
          bucket->key = buckets[b].key;
          bucket->val += buckets[b].val;
        }            
      }
    }    
  }
  for (i= 0; i < global_obs.size(); ++i)
  {
    OVERFLOW_BUFFER::const_iterator it = global_obs[i]->find(d->cpu);
    if (it != global_obs[i]->end())
    {
      const vector<OBNode>& ob = it->second;
      for (o = 0; o < ob.size(); ++o)
      {
        if (UNLIKELY(num_filled_buckets > num_buckets_till_resize)) {
          //ResizeBuckets(num_buckets_ * 2);
        }
        uint32_t hash = ob[i].key * factor;
        int bucket_idx = hash & (num_buckets - 1);
        if (node_remaining_current_page == 0) 
        {
          next_node = reinterpret_cast<MNode*>(mamalloc(PAGE_SIZE));
          mem_pool.push_back(next_node);
          node_remaining_current_page = PAGE_SIZE / sizeof(Node);      
        }
        MNode* bucket = merge_buckets[bucket_idx];
        while (bucket != NULL)
        {
          if (bucket->key == ob[i].key) break;
          bucket = bucket->next;
        }
        if (bucket == NULL)
        {
          ++num_filled_buckets;
          next_node->key = ob[i].key;
          next_node->val = ob[i].val;
          next_node->next = bucket;
          bucket = next_node;
          --node_remaining_current_page;
          ++next_node;      
        }
        else
        {
          bucket->key = ob[i].key;
          bucket->val += ob[i].val;
        }  
      }
    }
  }
  
  d->time = thread_time() - t;
  free(gen);
  free(keys);
  free(vals);
  pthread_exit(NULL);
}

void *multi_thread_logical_core_share_nothing(void *arg)
{
  Info *d = (Info*) arg;
  assert(pthread_equal(pthread_self(), d->id));
  bind_thread(d->cpu);
  uint64_t t = thread_time();
  size_t i, b = 0, size = d->size, partitions = d->partitions;
  uint32_t* keys = d->keys;
  uint32_t* vals = d->vals;
  size_t num_buckets = 32768;
  vector<Node> buckets;
  buckets.resize(num_buckets, Node());
  vector<OBNode> ob;
  int node_remaining = num_buckets;
  
        rand32_t *gen = rand32_init(d->seed);
  uint32_t factor = rand32_next(gen) | 1;
  for (i = 0 ; i < size; ++i) {
    uint32_t hash = keys[i] * factor;  
    int part = hash % partitions;
    if (part != d->cpu) continue;
    size_t bucket_idx = hash & (num_buckets - 1);
    if (node_remaining == 0) 
    {
      for (b = 0; b < num_buckets; ++b)
      {
        if (buckets[b].hits < 10)
        {
          ob.push_back(OBNode(buckets[b].key, buckets[b].val));
          buckets[b].key = -1;
          buckets[b].val = 0;
          ++node_remaining;
        }
      }
      if (node_remaining == 0) 
      {
        for(; i < size; ++i)
        {
          uint32_t hash = keys[i] * factor;
          int part = hash % partitions;
          if (part == d->cpu)
          {
            ob.push_back(OBNode(keys[i], vals[i]));
          }
        }
      }
    }
    size_t cur_idx = bucket_idx;
    while (cur_idx < num_buckets && buckets[cur_idx].hits != 0)
    {
      if (buckets[cur_idx].key == keys[i]) break;
      ++cur_idx;
    }
    if (cur_idx == num_buckets) 
    {
      cur_idx = 0;
      while (cur_idx < bucket_idx && buckets[cur_idx].hits != 0)
      {
        if (buckets[cur_idx].key == keys[i]) break;
        ++cur_idx;
      }
    }
    
    if (buckets[cur_idx].hits == 0)
    {
      buckets[cur_idx].key = keys[i];
      buckets[cur_idx].val = vals[i];
      ++buckets[cur_idx].hits;
      --node_remaining;
    }
    else
    {
      buckets[cur_idx].val += vals[i];
      ++buckets[cur_idx].hits;
    }
  }
    
  int part_keys = 0;
  for (b = 0; b < num_buckets; ++b)
  {
    if (buckets[b].hits != 0)
    {
      ++part_keys;
    }
  }
  part_keys += ob.size();

  const float MAX_BUCKET_OCCUPANCY_FRACTION = 0.75f;
  uint8_t bits = exact_log_2(part_keys / MAX_BUCKET_OCCUPANCY_FRACTION);
  assert(bits > 0);
  num_buckets = 0x1L << bits;
  int num_buckets_till_resize = num_buckets * MAX_BUCKET_OCCUPANCY_FRACTION;
  
  vector<MNode*> merge_buckets;
  merge_buckets.resize(num_buckets);
  vector<MNode*> mem_pool;
  const int PAGE_SIZE = 8 * 1024 * 1024;
   MNode* next_node = reinterpret_cast<MNode*>(mamalloc(PAGE_SIZE));
  mem_pool.push_back(next_node);
  int node_remaining_current_page = PAGE_SIZE / sizeof(MNode);
  int num_filled_buckets = 0;
  for (b = 0; b < num_buckets; ++b)
  {
    if (buckets[b].hits != 0)
    {
      if (UNLIKELY(num_filled_buckets > num_buckets_till_resize)) {
        //ResizeBuckets(num_buckets_ * 2);
      }
      uint32_t hash = buckets[b].key * factor;
      int bucket_idx = hash & (num_buckets - 1);
      if (node_remaining_current_page == 0) 
      {
        next_node = reinterpret_cast<MNode*>(mamalloc(PAGE_SIZE));
        mem_pool.push_back(next_node);
        node_remaining_current_page = PAGE_SIZE / sizeof(Node);      
      }
      MNode* bucket = merge_buckets[bucket_idx];
      while (bucket != NULL)
      {
        if (bucket->key == buckets[b].key) break;
        bucket = bucket->next;
      }
      if (bucket == NULL)
      {
        ++num_filled_buckets;
        next_node->key = buckets[b].key;
        next_node->val = buckets[b].val;
        next_node->next = bucket;
        bucket = next_node;
        --node_remaining_current_page;
        ++next_node;      
      }
      else
      {
        bucket->key = buckets[b].key;
        bucket->val += buckets[b].val;
      }          
    }
  }
  for (i = 0; i < ob.size(); ++i)
  {
    if (UNLIKELY(num_filled_buckets > num_buckets_till_resize)) {
      //ResizeBuckets(num_buckets_ * 2);
    }
    uint32_t hash = ob[i].key * factor;
    int bucket_idx = hash & (num_buckets - 1);
    if (node_remaining_current_page == 0) 
    {
      next_node = reinterpret_cast<MNode*>(mamalloc(PAGE_SIZE));
      mem_pool.push_back(next_node);
      node_remaining_current_page = PAGE_SIZE / sizeof(Node);      
    }
    MNode* bucket = merge_buckets[bucket_idx];
    while (bucket != NULL)
    {
      if (bucket->key == ob[i].key) break;
      bucket = bucket->next;
    }
    if (bucket == NULL)
    {
      ++num_filled_buckets;
      next_node->key = ob[i].key;
      next_node->val = ob[i].val;
      next_node->next = bucket;
      bucket = next_node;
      --node_remaining_current_page;
      ++next_node;      
    }
    else
    {
      bucket->key = ob[i].key;
      bucket->val += ob[i].val;
    }    
  }

  d->time = thread_time() - t;
  free(gen);
  free(keys);
  free(vals);
  pthread_exit(NULL);
}

int main(int argc, char **argv)
{
  srand(time(NULL));
  int t, threads = hardware_threads();
  int gigs = 6;
  size_t i, size = gigs * 128 * 1024 * 1024;
  fprintf(stderr, "Tuples: %ld\n", size);
  fprintf(stderr, "Threads: %d\n", threads);
  //char *names[] = {"Single thread",
  //                 "Multi threads blu",
  //                 "Multi threads logical core share-nothing",
  //                 "Multi threads physical core share-nothing"};
  //int m, methods = sizeof(names) / sizeof(char*);
  
  single_thread_aggregation(size);
  
  pthread_barrier_t barrier;
  pthread_barrier_init(&barrier, NULL, threads);
  Info_blu info[threads];
  for (t = 0 ; t != threads ; ++t) {
    info[t].cpu = t;
    info[t].seed = rand();
    info[t].partitions = threads;
    info[t].time = 0;
    info[t].size = (size / threads) & ~15;
    info[t].barrier = &barrier;
    pthread_create(&info[t].id, NULL, multi_thread_blu, (void*) &info[t]);
  }
  pthread_barrier_destroy(&barrier);
  
  uint64_t nanos = 0;
  for (t = 0 ; t != threads ; ++t)
    nanos += info[t].time;
  nanos /= threads;
  double billion = 1000 * 1000 * 1000;
  fprintf(stderr, "%-30s %6.4f secs %6.2f btps\n",
      "Multi threads blu", nanos / billion, size * 1.0 / nanos);
  
  uint32_t *keys = reinterpret_cast<uint32_t*>(mamalloc(size * sizeof(uint32_t)));
  uint32_t *vals = reinterpret_cast<uint32_t*>(mamalloc(size * sizeof(uint32_t)));
  int seed = rand();
  rand32_t *gen = rand32_init(seed);
  for (i = 0 ; i != size ; ++i) {
    keys[i] = rand32_next(gen);
    vals[i] = ~keys[i];
  }

  Info thread_info[threads];
  for (t = 0 ; t != threads ; ++t) {
    thread_info[t].cpu = t;
    thread_info[t].seed = rand();
    thread_info[t].size = size;
    thread_info[t].partitions = num_threads;
    thread_info[t].keys = keys;
    thread_info[t].vals = vals;
    pthread_create(&thread_info[t].id, NULL, multi_thread_logical_core_share_nothing, (void*) &thread_info[t]);
  }
  nanos = 0;
  for (t = 0 ; t != threads ; ++t)
    nanos += thread_info[t].time;
  nanos /= threads;
  fprintf(stderr, "%-30s %6.4f secs %6.2f btps\n",
      "Multi threads logical core share-nothing", nanos / billion, size * 1.0 / nanos);
  free(gen);
        return 0;
}
