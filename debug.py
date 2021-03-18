import signal, faulthandler


def dump_traceback(sig, frame):
    faulthandler.dump_traceback()


signal.signal(signal.SIGUSR1, dump_traceback)  # Register handler
