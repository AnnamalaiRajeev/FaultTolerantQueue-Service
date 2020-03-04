from multiprocessing import  Process
import threading


def run_thread(fn):
    def run(*k, **kw):
        t = threading.Thread(target=fn, args=k, kwargs=kw)
        t.start()
        return t
    return run


def run_child_process(f):

    def run_process(*k, **kw):
        # relatively faster time of execution compared to Multi-threading
        p = Process(target=f, args=k, kwargs=kw)
        p.start()
        return p
    return run_process