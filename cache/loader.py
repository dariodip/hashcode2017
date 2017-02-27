from queue import PriorityQueue
from readytogo.cache import cache_objects as co


def load(file):

    with open(file, 'r') as f:
        # read first line
        v, e, r, c, cache_size = [int(_) for _ in f.readline().split(' ')]
        # read videos size
        line = f.readline().split(' ')
        video_list = {i: int(j) for i, j in zip(range(len(line)), line)}
        minimum_video_size = min([int(_) for _ in line])
        if not len(video_list) == v:
            raise Exception("Something wrong in parsing")

        caches = {i: co.Cache(cache_size) for i in range(c)}  # dict {cache_id : cache_object}
        endpoints = {}  # dict {endpoint_id: endpoint_object}
        video_requests = PriorityQueue()  # Priority Queue (-video_request_times, (video_object))

        for i in range(e):  # read each endpoint info
            latency_to_datacenter, cache_count = [int(_) for _ in f.readline().split(' ')]
            cache_pq = PriorityQueue()  # PriorityQueue (cache_latency, cache_object)
            for j in range(cache_count):
                cache_id, cache_latency = [int(_) for _ in f.readline().split(' ')]
                cache_pq.put((cache_latency, caches[cache_id]))
            endpoint = co.EndPoint(cache_pq, latency_to_datacenter)
            endpoints[i] = endpoint

        for i in range(r):  # read each request
            video_id, end_point_id, request_times = [int(_) for _ in f.readline().split(' ')]
            if video_list[video_id] > cache_size:
                continue
            video = co.Video(video_list[video_id], video_id, end_point_id)
            video_requests.put((-request_times, video))

        if not len(endpoints) == e or not len(caches) == c:
            raise Exception("Something wrong in parsing")

        return caches, endpoints, video_requests, video_list, \
            {'v': v, 'e': e, 'r': r, 'c': c, 'cache_size': cache_size}, minimum_video_size
