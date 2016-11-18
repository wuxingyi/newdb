#ifndef __thread_h__
#define __thread_h__
#include <pthread.h>
#include <string>

using namespace std;
class Thread
{
public:
  Thread();
  virtual ~Thread();
  
  int Start();
  int Join();
  int Detach();
  pthread_t Self();
  virtual void* Run() = 0;
  virtual string getThreadName() = 0;
    
private:
  pthread_t m_tid;
  bool m_running;
  bool m_detached;
};

#endif
