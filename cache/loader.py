from queue import PriorityQueue, Queue
from cache import cache_objects as co


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

        caches = {i: co.Cache(cache_size, i) for i in range(c)}  # dict {cache_id : cache_object}
        endpoints = {}  # dict {endpoint_id: endpoint_object}
        requests = {i: co.Request() for i in range(v)}

        for i in range(e):  # read each endpoint info
            latency_to_datacenter, cache_count = [int(_) for _ in f.readline().split(' ')]
            cache_pq = PriorityQueue()  # PriorityQueue (cache_latency, cache_object)
            cache_dict = dict()
            for j in range(cache_count):
                cache_id, cache_latency = [int(_) for _ in f.readline().split(' ')]
                cache_pq.put((cache_latency, caches[cache_id]))
                cache_dict[cache_id] = cache_latency
                caches[cache_id].add_ep(i, cache_latency)
            endpoint = co.EndPoint(cache_pq, latency_to_datacenter, cache_dict)
            endpoints[i] = endpoint

        for i in range(r):  # read each request
            video_id, end_point_id, request_times = [int(_) for _ in f.readline().split(' ')]
            if video_list[video_id] > cache_size:
                continue
            video = co.Video(video_list[video_id], video_id, end_point_id)
            req = requests[video_id]
            req.required_video = video
            req.add_ep(endpoints[end_point_id], request_times)

        video_reqs = Queue(maxsize=len(requests))
        for req in requests:
            request = requests[req]
            if request.required_video is None:
                continue
            if request.required_video.size > cache_size:
                continue
            video_reqs.put((-request.request_times, request))

        if not len(endpoints) == e or not len(caches) == c:
            raise Exception("Something wrong in parsing")

        return caches, endpoints, video_reqs, video_list, \
            {'v': v, 'e': e, 'r': r, 'c': c, 'cache_size': cache_size}, minimum_video_size
