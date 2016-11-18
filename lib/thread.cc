#include "thread.h"

static void* runThread(void* arg)
{
    return ((Thread*)arg)->Run();
}

Thread::Thread() : m_tid(-1), m_running(false), m_detached(false){}

Thread::~Thread()
{
    if (m_running == true && m_detached == false) {
        pthread_detach(m_tid);
    }
    if (m_running == true) {
        pthread_cancel(m_tid);
    }
}

int Thread::Start()
{
    int result = pthread_create(&m_tid, NULL, runThread, this);
    if (result == 0) {
        m_running = true;
    }
    return result;
}

int Thread::Join()
{
    int result = -1;
    if (m_running == true) {
        result = pthread_join(m_tid, NULL);
        if (result == 0) {
            m_detached = false;
        }
    }
    return result;
}

int Thread::Detach()
{
    int result = -1;
    if (m_running == true && m_detached == false) {
        result = pthread_detach(m_tid);
        if (result == 0) {
            m_detached = true;
        }
    }
    return result;
}

pthread_t Thread::Self() {
    return m_tid;
}
