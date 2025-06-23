# test_ray_logging.py
import ray, time, json

ray.init("ray://cifrar.cju.ac.kr:25577",log_to_driver=True)

@ray.remote
def emit_event(i):
    # JSON 문자열을 stdout 으로 바로 찍습니다.
    print(json.dumps({
        "event": "square_done",
        "input": i,
        "output": i + i
    }), flush=True)
    time.sleep(0.1)
    return i + i

if __name__ == "__main__":
    # 5개만 테스트
    results = ray.get([emit_event.remote(i) for i in range(5,100)])
    print("Done:", results)
