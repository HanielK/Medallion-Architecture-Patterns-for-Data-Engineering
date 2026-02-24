# from abc import ABC, abstractmethod
# from pyspark.sql.utils import AnalysisException
# from datetime import datetime,timedelta
# import requests
# import time

# class try_call(ABC):
#     def __init__(self, *args, **kwargs):
#         pass
           
#     @abstractmethod
#     def try_call(self,func):
#         pass

# class try_call_api(try_call):
#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#     def try_call(self, func):
#         def wrapper(*args, **kwargs):
#             try:
#                 return_value = func(*args, **kwargs)
#                 return "success",return_value
#             except requests.exceptions.Timeout:
#                 print("|API HANDLER|[INFO] timeout")
#                 return "timeout",None
#             except Exception as e:
#                 print(f"|API HANDLER|[ERROR] {e}")
#                 return "fatal",None 
#         return wrapper 

# class api_handler(ABC):
#     def __init__(self, **kwargs):
#         self.response       = None
    
#     class api_connecter():
#         def __init__(self, **kwargs):
#             super().__init__(**kwargs)
    
#         def time_out_handler(func):
#             def wrapper(*args, **kwargs):
#                 lc              = 0
#                 is_timeout      = False
#                 response        = None
#                 delay           = 10
#                 while True:
#                     print(f"|API HANDLER|[ACTION] timeout handler [attempt: {lc}]")
#                     response    = func(*args, **kwargs)
#                     if response[0] == "success":
#                         response_package = response[1]
#                     if response[0] == "fatal":
#                         raise(Exception(f"|API HANDLER| fatal error {response}")) 

#                     if response[0] == "timeout":
#                         print("|API HANDLER|[INFO] timed out")
#                         time.sleep(delay)
#                         lc = lc + 1 
#                         is_timeout = False
#                     if lc > 10:
#                         raise(Exception("|API HANDLER| all attempts failed"))
#                     elif response_package.status_code == 200:
#                         print(f"|API HANDLER|[INFO] sucessful call code: {response_package.status_code}")
#                         break
#                     elif response_package.status_code in [204, 400, 401, 402, 403, 404, 405, 406, 407, 411, 500]:
#                         print("|API HANDLER|[INFO] bad response_package")
#                         raise(Exception(f"|API HANDLER| fatal code: {response_package.status_code}"))
#                         break
#                     elif lc > 10:
#                         print("|API HANDLER|[INFO] bad response_package")
#                         raise(Exception("|API HANDLER| all attempts failed"))
#                     else:
#                         print(
#                             f"|API HANDLER|[INFO] sleep {delay} seconds "
#                             f"due to non-fatal code: {response_package.status_code}"
#                             )
#                         time.sleep(delay)
#                         lc = lc + 1
#                 return response_package
#             return wrapper 
      
#         @time_out_handler
#         @try_call_api().try_call
#         def get_requests_response(self, api_url, **kwargs):
#             print(f"""|API HANDLER|[FUNCTION] get_requests_response Vars: {kwargs}""")
#             return requests.get(api_url, **kwargs)

#         def make_api_call(self, api_url:str = "", **kwargs):
#             if "timeout" not in kwargs:
#                 kwargs["timeout"] = 30
#             print(f"""|API HANDLER|[INFO] {kwargs}""")
#             self.response = self.get_requests_response(api_url,**kwargs)
        
#         def get_response(self):
#             return self.response
        
# api_handler.py

from datetime import datetime, timedelta
import time
import requests


class api_handler:
    """Simple API helper with retry + timeout logic."""

    def __init__(self):
        self._response = None

    def make_api_call(
        self,
        api_url: str,
        max_attempts: int = 5,
        backoff_seconds: int = 5,
        timeout: int = 30,
        **kwargs,
    ):
        """Call an API with basic retry logic."""
        attempt = 0
        last_error = None

        while attempt < max_attempts:
            attempt += 1
            try:
                print(f"|API HANDLER|[INFO] Attempt {attempt} calling {api_url}")
                resp = requests.get(api_url, timeout=timeout, **kwargs)

                if resp.status_code == 200:
                    print(f"|API HANDLER|[INFO] Successful call: {resp.status_code}")
                    self._response = resp
                    return

                # Treat these as fatal
                if resp.status_code in {
                    204, 400, 401, 402, 403, 404, 405, 406, 407, 411, 500
                }:
                    raise RuntimeError(
                        f"|API HANDLER| Fatal status code: {resp.status_code}"
                    )

                # Non-fatal -> wait and retry
                print(
                    f"|API HANDLER|[INFO] Sleeping {backoff_seconds}s due to "
                    f"non-fatal code {resp.status_code}"
                )
                time.sleep(backoff_seconds)

            except requests.exceptions.RequestException as exc:
                last_error = exc
                print(f"|API HANDLER|[WARN] Exception: {exc}. Retrying...")
                time.sleep(backoff_seconds)

        raise RuntimeError(
            f"|API HANDLER| All attempts failed. Last error: {last_error}"
        )

    def get_response(self):
        if self._response is None:
            raise RuntimeError(
                "|API HANDLER|[ERROR] No response set. Call make_api_call() first."
            )
        return self._response
