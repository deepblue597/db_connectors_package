
def log_calls(method):
    """Decorator to log method calls."""
    def wrapper(self, *args, **kwargs):
        print(f"Calling method: {method.__name__} with args: {args} kwargs: {kwargs}")
        result = method(self, *args, **kwargs)
        print(f"Method {method.__name__} completed")
        return result
    return wrapper

def timer(method):
    """Decorator to time method execution."""
    import time
    def wrapper(self, *args, **kwargs):
        start_time = time.time()
        result = method(self, *args, **kwargs)
        end_time = time.time()
        print(f"Method {method.__name__} executed in {end_time - start_time:.4f} seconds")
        return result
    return wrapper

