import os
import ray


ray.init(
    address="ray://cifrar.cju.ac.kr:25577",
    runtime_env={
        "py_modules": [os.path.abspath("../PCFGCracking/pcfg_lib")],
        "excludes": [".gitignore","*.db","**/.git/**","**/.gitignore",],
        "pip": [],
    },
    log_to_driver=False
)

# 예시 remote
@ray.remote
def test_fn(x):
    from pcfg_lib import test
    return test.plus(x, x)

print(ray.get(test_fn.remote(5)))
